"""Tests for the slurmpy.local module."""

import datetime
import time

import pytest

from pyjob.core import JobFailedError
from pyjob.local import FunctionCall, LocalJob, local_job


def test_job():
    """Test the Job class."""

    def add(a, b):
        return a + b

    function_call = FunctionCall(add, (1, 2), {})
    job = LocalJob(function_call)
    assert job.status.status == "PENDING"
    assert job.name == "add"
    ret = job.run()
    assert ret == 3
    assert job.status.status == "COMPLETED"


def test_job_failed():
    """Test the LocalJob class when the job fails."""

    def fail():
        raise ValueError("fail")

    function_call = FunctionCall(fail, (), {})
    job = LocalJob(function_call)
    assert job.status.status == "PENDING"
    assert job.name == "fail"
    with pytest.raises(JobFailedError):
        job.run()
    assert job.status.status == "FAILED"
    assert not job.return_path.exists()


def test_timeout():
    """Test the LocalJob class when the job times out."""

    def wait():
        time.sleep(2)

    function_call = FunctionCall(wait, (), {})
    job = LocalJob(function_call, timeout=datetime.timedelta(seconds=1))
    assert job.status.status == "PENDING"
    assert job.name == "wait"
    with pytest.raises(JobFailedError):
        job.run()
    assert job.status.status == "FAILED"
    assert not job.return_path.exists()


def test_submit():
    """Test the submit method of the LocalJob class."""

    def no_wait():
        return "no wait"

    def wait():
        time.sleep(1)
        return "wait"

    function_call = FunctionCall(wait, (), {})
    job = LocalJob(function_call)
    assert job.status.status == "PENDING"
    assert job.name == "wait"
    pid = job.submit()
    assert isinstance(pid, int) and pid > 0
    assert job.status.status == "RUNNING"
    ret = job.result()
    assert ret == "wait"
    assert job.status.status == "COMPLETED"

    function_call = FunctionCall(no_wait, (), {})
    job = LocalJob(function_call)
    assert job.status.status == "PENDING"
    assert job.name == "no_wait"
    pid = job.submit()
    assert isinstance(pid, int) and pid > 0
    assert job.status.status == "RUNNING"
    ret = job.result()
    assert ret == "no wait"
    assert job.status.status == "COMPLETED"


def test_decorator():
    """Test the local_job decorator."""

    @local_job()
    def add(a, b):
        return a + b

    ret = add(1, 2)
    assert ret == 3
    assert isinstance(ret, int)
    assert add.__name__ == "add"
