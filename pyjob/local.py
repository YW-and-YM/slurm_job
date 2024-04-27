"""
Simple interface to execute python functions locally
(This does not do anything special, just as an example of how to extend Job class)
"""

import datetime
import sys
from functools import wraps
from typing import Callable, Union

import sh

from pyjob.core import FunctionCall, Job, Template


class LocalJob(Job):
    """Class to manage a local job."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
    ):
        super().__init__(function_call, script_template, timeout)

    def run(self):
        self.status.set_start()
        sh.Command("bash")(
            self.resources.job_script,
            _out=self.resources.log,
            _err=self.resources.log,
            _tee=(sys.stdout, sys.stderr),  # duplicate output to stdout and stderr
        )
        return self.result()

    def submit(self) -> int:
        proc = sh.Command("bash")(
            self.resources.job_script,
            _out=self.resources.log,
            _err=self.resources.log,
            _bg=True,
        )
        self.status.set_start()
        self.id = proc.pid  # type: ignore # pylint: disable=no-member
        return self.id


def local_job():
    """Decorator to create a local job."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            function_call = FunctionCall(func, args, kwargs)
            job = LocalJob(function_call)
            return job.run()

        return wrapper

    return decorator
