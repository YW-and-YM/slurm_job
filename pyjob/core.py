"""Base classes for managing jobs on the cluster."""

import datetime
import json
import shutil
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, Literal, Union

import cloudpickle
import sh

END_OF_JOB = "JOBEND"


PYTHON_SCRIPT = """
import sys
import cloudpickle
from pathlib import Path

func = cloudpickle.loads(Path("{pickle_path}").read_bytes())
ret_path = Path("{ret_path}")

try:
    ret = func()
    ret_path.write_bytes(cloudpickle.dumps(ret))
except Exception as e:
    ret_path.write_bytes(cloudpickle.dumps(e))
    raise e
"""


@dataclass
class Template:
    """Class to manage a template."""

    python_script: str = PYTHON_SCRIPT
    interpreter: str = "/bin/bash"
    python_exec: str = "python"
    before: str = ""
    after: str = "echo JOBEND"

    def __str__(self) -> str:
        return (
            f"#!{self.interpreter}\n"
            f"{self.before}\n"
            f"{self.python_exec} << EOF\n"
            f"{self.python_script}\n"
            "EOF\n"
            f"{self.after}\n"
        )

    def format(self, **kwargs) -> str:
        """Format the template."""
        return str(self).format(**kwargs)


@dataclass
class FunctionCall:
    """Class to manage a function call."""

    func: Callable
    args: tuple
    kwargs: dict

    def __call__(self):
        return self.func(*self.args, **self.kwargs)

    def dump(self, path: Path):
        """Dump the function call to a file."""
        func = partial(self.func, *self.args, **self.kwargs)
        path.write_bytes(cloudpickle.dumps(func))


@dataclass
class JobAssets:
    """Class to manage job assets."""

    root: Path
    job_script: Path = field(init=False)
    function_call_dump: Path = field(init=False)
    return_dump: Path = field(init=False)
    log: Path = field(init=False)

    def __post_init__(self):
        if self.root.exists():
            if not self.root.is_dir():
                raise ValueError(f"{self.root} is not a directory")
            if list(self.root.iterdir()):
                raise ValueError(f"{self.root} is not empty")
        self.job_script = self.root / "job.sh"
        self.function_call_dump = self.root / "func.pkl"
        self.return_dump = self.root / "ret.pkl"
        self.log = self.root / "log.txt"

        self.root = self.root.absolute()
        self.job_script = self.job_script.absolute()
        self.function_call_dump = self.function_call_dump.absolute()
        self.return_dump = self.return_dump.absolute()
        self.log = self.log.absolute()

    def create(self, function_call: FunctionCall, script_template: Union[str, Template]) -> None:
        """
        Create the job assets.

        Args:
            function_call (FunctionCall): Function call to be executed.
            script_template (str): Template for the job script.
        """
        self.job_script.write_text(
            script_template.format(pickle_path=self.function_call_dump, ret_path=self.return_dump),
            encoding="utf-8",
        )
        function_call.dump(self.function_call_dump)

    def clean_up(self):
        """
        Remove the job assets.
        """
        if self.root.exists():
            shutil.rmtree(self.root)

    def __str__(self):
        return json.dumps(asdict(self), indent=2)


@dataclass
class JobStatus:
    status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED"]
    create_time: datetime.datetime
    start_time: Union[datetime.datetime, None]
    end_time: Union[datetime.datetime, None]
    timeout: datetime.timedelta = datetime.timedelta(minutes=10)

    def set_start(self) -> None:
        """Set the start time of the job."""
        self.start_time = datetime.datetime.now()
        self.status = "RUNNING"

    def set_end(self, success: bool = True) -> None:
        """Set the end time of the job."""
        self.end_time = datetime.datetime.now()
        self.status = "COMPLETED" if success else "FAILED"

    def is_timeout(self) -> bool:
        """Check if the job has timed out."""
        if self.start_time is None:
            return False
        return datetime.datetime.now() - self.start_time > self.timeout


def tail_output(file_path: Path, name: str = "", end_pattern: str = END_OF_JOB) -> threading.Event:
    """
    Tail the output of a file in a separate thread.

    Args:
        file_path (Path): Path to the file to tail.
        end_pattern (str, optional): Pattern to signal the end of the output. Defaults to "JOBEND".

    Returns:
        threading.Event: Event to signal when the tail output has finished.
    """
    finished_event = threading.Event()

    def tail_thread():
        # Wait for the file to exist
        while not file_path.exists():
            time.sleep(0.1)

        # Tail the file
        for line in sh.Command("tail")("-f", file_path, _iter=True):  # type: ignore
            assert isinstance(line, str)
            if line.strip() == end_pattern.strip():
                break
            line = f"{name} | {line}"
            print(line, end="")

        # Signal that the tail output has finished
        finished_event.set()

    # Start the tail thread
    thread = threading.Thread(target=tail_thread)
    thread.start()

    return finished_event


class JobFailedError(Exception):
    """Error raised when a job fails."""


class Job:
    """Class to manage a job on the cluster."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
        assets_root: Union[str, Path, None] = None,
    ):
        """
        Initialize the job.

        Args:
            function_call (FunctionCall): Function call to be executed.
            script_template (str, optional): Template for job script. Defaults to str(Template()).
            timeout (datetime.timedelta, optional): Timeout for the job. Defaults to 10 minutes.
            assets_root (Path, optional): Root directory for job assets. Defaults to None.
                Leave as None to use a temporary directory.
                User is responsible for cleaning up the directory if set.
        """
        self.function_call = function_call
        self.script_template = script_template
        self.status: JobStatus = JobStatus("PENDING", datetime.datetime.now(), None, None, timeout)
        self.save_assets = False

        if isinstance(assets_root, str):
            assets_root = Path(assets_root)

        # if assets_root is specified, check if it does not exist or is an empty directory
        if assets_root and assets_root.exists():
            if not assets_root.is_dir():
                raise ValueError(f"{assets_root} is not a directory")
            if list(assets_root.iterdir()):
                raise ValueError(f"{assets_root} is not empty")
            self.save_assets = True  # Save assets to the specified directory

        # Create assets
        self.resources = JobAssets(assets_root or Path(tempfile.mkdtemp(dir=".")))
        self.resources.create(self.function_call, self.script_template)

        self.assets_root = assets_root
        self.name: str = self.function_call.func.__name__
        self.id: Any = None

    def _load_return(self) -> Any:
        """Load the return value of the job."""
        if not self.resources.return_dump.exists():
            raise FileNotFoundError(f"Return dump {self.resources.return_dump} not found")

        ret = cloudpickle.loads(self.resources.return_dump.read_bytes())

        # Clean up assets if not saving them
        if not self.save_assets:
            self.resources.clean_up()

        # Raise the same exception that was raised in the job
        if isinstance(ret, Exception):
            self.status.set_end(False)
            raise JobFailedError(
                f"Job {self.name} with id {self.id} failed with exception: {ret}"
            ) from ret

        self.status.set_end(True)
        return ret

    def run(self) -> Any:
        """Run the job and return the result."""
        self.submit()
        return self.result()

    def result(self) -> Any:
        """Return the result of the job."""
        while True:
            if self.status.is_timeout():
                self.status.set_end(False)
                if not self.save_assets:
                    self.resources.clean_up()
                raise TimeoutError(
                    f"Job {self.name} timed out after {self.status.timeout.seconds} seconds"
                )
            if self.resources.return_dump.exists():
                break
        return self._load_return()

    def submit(self) -> Any:
        """Run job without waiting for it to finish."""
        raise NotImplementedError("Method submit must be implemented in a subclass")

    def __del__(self):
        if not self.save_assets:
            self.resources.clean_up()
