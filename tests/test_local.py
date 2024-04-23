"""Tests for the slurmpy.local module."""

import time

import pytest

from pyjob.local import FunctionCall, LocalJob, local_job


def test_job():
    """Test the Job class."""

    def add(a, b):
        return a + b

    function_call = FunctionCall(add, (1, 2), {})
    job = LocalJob(function_call)
    assert job.status == "PENDING"
    assert job.name == "add"
    assert job.resources.root.exists()
    ret = job.run()
    assert ret == 3
    assert job.status == "COMPLETED"
    assert not job.resources.root.exists()


def test_job_failed():
    """Test the LocalJob class when the job fails."""

    def fail():
        raise ValueError("fail")

    function_call = FunctionCall(fail, (), {})
    job = LocalJob(function_call)
    assert job.status == "PENDING"
    assert job.name == "fail"
    assert job.resources.root.exists()
    with pytest.raises(ValueError):
        job.run()
    assert job.status == "FAILED"
    assert not job.resources.root.exists()


def test_submit():
    """Test the submit method of the LocalJob class."""

    def no_wait():
        return "no wait"

    def wait():
        time.sleep(1)
        return "wait"

    function_call = FunctionCall(wait, (), {})
    job = LocalJob(function_call)
    assert job.status == "PENDING"
    assert job.name == "wait"
    assert job.resources.root.exists()
    pid = job.submit()
    assert isinstance(pid, int) and pid > 0
    assert job.status == "RUNNING"
    ret = job.result()
    assert ret == "wait"
    assert job.status == "COMPLETED"

    function_call = FunctionCall(no_wait, (), {})
    job = LocalJob(function_call)
    assert job.status == "PENDING"
    assert job.name == "no_wait"
    assert job.resources.root.exists()
    pid = job.submit()
    assert isinstance(pid, int) and pid > 0
    assert job.status == "RUNNING"
    ret = job.result()
    assert ret == "no wait"
    assert job.status == "COMPLETED"


def test_decorator():
    """Test the local_job decorator."""

    @local_job()
    def add(a, b):
        return a + b

    ret = add(1, 2)
    assert ret == 3
    assert isinstance(ret, int)
    assert add.__name__ == "add"
