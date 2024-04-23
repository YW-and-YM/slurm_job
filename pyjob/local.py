"""
Simple interface to execute python functions locally
(This does not do anything special, just as an example of how to extend Job class)
"""

import sys
from functools import wraps
from typing import Callable

import sh

from pyjob.core import TEMPLATE, FunctionCall, Job


class LocalJob(Job):
    """Class to manage a local job."""

    def __init__(self, function_call: FunctionCall, script_template: str = TEMPLATE):
        super().__init__(function_call, TEMPLATE)

    def run(self):
        self.status = "RUNNING"
        sh.Command("bash")(
            self.resources.job_script,
            _out=self.resources.log,
            _err=self.resources.log,
            _tee=(sys.stdout, sys.stderr),
        )
        return self._load_return()

    def submit(self) -> int:
        proc = sh.Command("bash")(
            self.resources.job_script,
            _out=self.resources.log,
            _err=self.resources.log,
            _bg=True,
        )
        self.status = "RUNNING"
        return proc.pid  # type: ignore # pylint: disable=no-member


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
