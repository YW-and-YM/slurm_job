"""Base classes for managing jobs on the cluster."""

import json
import shutil
import tempfile
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, Literal

import cloudpickle

TEMPLATE = """#!/bin/bash

python << EOT
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
EOT
"""


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

    def create(self, function_call: FunctionCall, script_template: str) -> None:
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


JobStatus = Literal["PENDING", "RUNNING", "COMPLETED", "FAILED"]


class Job:
    """Class to manage a job on the cluster."""

    def __init__(self, function_call: FunctionCall, script_template: str = TEMPLATE):
        self.function_call = function_call
        self.script_template = script_template
        self.resources = JobAssets(root=Path(tempfile.mkdtemp()))
        self.resources.create(self.function_call, self.script_template)
        self.status: JobStatus = "PENDING"

    @property
    def name(self) -> str:
        """Return the name of the job."""
        return self.function_call.func.__name__

    def _load_return(self) -> Any:
        """Load the return value of the job."""
        ret = cloudpickle.loads(self.resources.return_dump.read_bytes())
        self.resources.clean_up()
        if isinstance(ret, Exception):
            self.status = "FAILED"
            raise ret
        self.status = "COMPLETED"
        return ret

    def run(self) -> Any:
        """Run the job and return the result."""
        raise NotImplementedError("Method run must be implemented in a subclass")

    def result(self) -> Any:
        """Return the result of the job."""
        while True:
            if self.resources.return_dump.exists():
                break
        return self._load_return()

    def submit(self) -> Any:
        """Run job without waiting for it to finish."""
        raise NotImplementedError("Method submit must be implemented in a subclass")

    def __del__(self):
        self.resources.clean_up()
