"""Base classes for managing jobs on the cluster."""

import base64
import datetime
import tempfile
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable, Literal, Union

import cloudpickle

from pyjob.utils import END_OF_JOB, wait_for_file

# Note: Python script template must always use single quotes for string literals
PYTHON_SCRIPT = """
import sys
import cloudpickle
from pathlib import Path
import base64

PICKLE_BASE64 = '{pickle_base64}'

func = cloudpickle.loads(base64.b64decode(PICKLE_BASE64))
ret_path = Path('{ret_path}')

try:
    ret = func()
    ret_path.write_bytes(cloudpickle.dumps(ret))
except Exception as e:
    ret_path.write_bytes(cloudpickle.dumps(e))
    raise e
finally:
    ret_path.chmod(0o400)
"""


@dataclass
class Template:
    """Class to manages the template for a job script."""

    python_script: str = PYTHON_SCRIPT
    python_exec: str = "python"
    before: str = ""
    after: str = f"echo {END_OF_JOB}"

    def __str__(self) -> str:
        return (
            f"{self.before}\n" f'{self.python_exec} -c "{self.python_script}"\n' f"{self.after}\n"
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
        """Dump the function call with its arguments baked in to a file."""
        func = partial(self.func, *self.args, **self.kwargs)
        path.write_bytes(cloudpickle.dumps(func))

    def dump_base64(self) -> str:
        """Dump the function call with its arguments baked in to a base64 string."""
        func = partial(self.func, *self.args, **self.kwargs)
        return base64.b64encode(cloudpickle.dumps(func)).decode()


@dataclass
class JobStatus:
    status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED"]
    create_time: datetime.datetime
    start_time: Union[datetime.datetime, None]
    end_time: Union[datetime.datetime, None]

    def set_start(self) -> None:
        """Set the start time of the job."""
        self.start_time = datetime.datetime.now()
        self.status = "RUNNING"

    def set_end(self, success: bool = True) -> None:
        """Set the end time of the job."""
        self.end_time = datetime.datetime.now()
        self.status = "COMPLETED" if success else "FAILED"


class JobFailedError(Exception):
    """Error raised when a job fails."""


class Job:
    """Class to manage a job on the cluster."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
    ):
        """
        Initialize the job.

        Args:
            function_call (FunctionCall): Function call to be executed.
            script_template (str, optional): Template for job script. Defaults to str(Template()).
            timeout (datetime.timedelta, optional): Timeout for the job. Defaults to 10 minutes.
        """
        self.function_call = function_call
        self.script_template = script_template
        self.timeout = timeout
        self.status = JobStatus("PENDING", datetime.datetime.now(), None, None)
        self.return_path = Path(tempfile.mktemp(dir="./", prefix="ret_"))
        self.name: str = self.function_call.func.__name__
        self.id: Any = None

    @property
    def script(self) -> str:
        """Dynamically generate the script for the job."""
        s = self.script_template.format(
            pickle_base64=self.function_call.dump_base64(),
            ret_path=self.return_path,
        )
        return s

    def _load_return(self) -> Any:
        """Load the return value of the job."""
        ret = cloudpickle.loads(self.return_path.read_bytes())
        self.cleanup()
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
        try:
            self.submit()
            return self.result()
        except Exception as e:
            raise e
        finally:
            self.cleanup()

    def result(self) -> Any:
        """Return the result of the job."""
        try:
            wait_for_file(self.return_path, timeout=self.timeout)
        except TimeoutError as e:
            self.status.set_end(False)
            raise JobFailedError(
                f"Job {self.name} with id {self.id} failed with exception: {e}"
            ) from e

        return self._load_return()

    def submit(self) -> Any:
        """Run job without waiting for it to finish."""
        raise NotImplementedError("Method submit must be implemented in a subclass")

    def cleanup(self) -> None:
        """Clean up the job."""
        if self.return_path.exists():
            self.return_path.unlink()

    def __del__(self):
        """Clean up the return path."""
        self.cleanup()
