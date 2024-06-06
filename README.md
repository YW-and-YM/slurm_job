# PyJob

A simple library to run any python function as a job, designed for XENONnT experiment but can be used for any other purpose.

## Installation

```bash
pip install https://github.com/Microwave-WYB/pyjob.git
```

## Usage

### Basic usage:

```python
import datetime

from pyjob.slurm import SlurmOptions
from pyjob.xenon_slurm import slurm_job


@slurm_job(
    options=SlurmOptions(
        partition="xenon1t", time="10:00", mem_per_cpu="100M", cpus_per_task=1, output="job.out"
    ),
    singularity_image="xenonnt-2024.04.1.simg",
    is_dali=False,
    timeout=datetime.timedelta(seconds=100),
)
def add(a, b):
    print("Adding:", a, b)
    return a + b


ret = add(1, 2)
print("Return:", ret)
```
Output:
```
Submitted batch job 40125577

slurm-40125577-add | Adding: 1 2
Return: 3
```
Where `slurm` describes the type of job, `40125577` is the job id, `add` is the function name, and `3` is the return value.

### Error handling:

Error during job execution will raise an JobFailedError exception. Complete error trace will be printed to the log file.

```python
@slurm_job(options=options, script_template=template, timeout=datetime.timedelta(seconds=60))
def fail():
    raise ValueError("This job will fail")

fail()
```
Output will be something like:
```
Submitted batch job 40125621

slurm-40125621-fail | Traceback (most recent call last):
slurm-40125621-fail |   File "<string>", line 17, in <module>
slurm-40125621-fail |   File "<string>", line 13, in <module>
slurm-40125621-fail |   File "/home/yuem/pyjob/test.py", line 33, in fail
slurm-40125621-fail |     raise ValueError("This job will fail")
slurm-40125621-fail | ValueError: This job will fail
ValueError: This job will fail

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/yuem/pyjob/test.py", line 36, in <module>
    fail()
  File "/home/yuem/pyjob/pyjob/xenon_slurm.py", line 74, in wrapper
    return job.run()
  File "/home/yuem/pyjob/pyjob/slurm.py", line 181, in run
    result = self.result()
  File "/home/yuem/pyjob/pyjob/core.py", line 170, in result
    return self._load_return()
  File "/home/yuem/pyjob/pyjob/core.py", line 143, in _load_return
    raise JobFailedError(
pyjob.core.JobFailedError: Job fail with id 40125621 failed with exception: This job will fail
```

### Advanced usage (with Jupiter notebook started as a Slurm job):

TODO: Add examples
