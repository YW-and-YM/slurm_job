"""
Simple interface to execute python functions locally
(This does not do anything special, just as an example of how to extend Job class)
"""

import datetime
import subprocess
import sys
from functools import wraps
from typing import Any, Callable, Union

from slurm_job.core import FunctionCall, Job, Template


class LocalJob(Job):
    """Class to manage a local job."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
    ):
        super().__init__(function_call, script_template, timeout)

    def submit(self) -> int:
        proc = subprocess.Popen(self.script, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        self.status.set_start()
        self.id = proc.pid
        return self.id


def local_job() -> Callable[..., Any]:
    """Decorator to create a local job."""

    def decorator(func: Callable[..., Any]):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            function_call = FunctionCall(func, args, kwargs)
            job = LocalJob(function_call)
            return job.run()

        return wrapper

    return decorator
