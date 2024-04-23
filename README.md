# PyJob

A simple library to run any python function as a job.

## Installation

```bash
pip install https://github.com/Microwave-WYB/pyjob.git
```

## Usage

Basic usage:

```python
from pyjob.slurm import SlurmJob, FunctionCall

def add(a, b):
    return a + b

options = SlurmOptions(name="add_job", time="00:01:00", cpus_per_task=1, mem_per_cpu="100M")
job = SlurmJob(FunctionCall(add, 1, 2), options)
result = job.run()
print(result)

# >>> 3
```

Use `job.submit()` to submit the job to the cluster without waiting for the result.

```python
from pyjob.slurm import SlurmJob, FunctionCall, SlurmOptions

def add(a, b):
    return a + b

options = SlurmOptions(name="add_job", time="00:01:00", cpus_per_task=1, mem_per_cpu="100M")
job = SlurmJob(FunctionCall(add, 1, 2), options)
pid = job.submit()
print(pid)

# >>> 123

print(job.result()) # wait for the job to finish and get the result

>>> 3
```

Using decorator:

```python
from pyjob.slurm import slurm_job, SlurmOptions

options = SlurmOptions(name="add_job", time="00:01:00", cpus_per_task=1, mem_per_cpu="100M")

@slurm_job(options)
def add(a, b):
    return a + b

result = add(1, 2)
print(result)

# >>> 3
```

## Error Handling

The job will raise exception just like calling the function directly.

```python
from pyjob.slurm import SlurmJob, FunctionCall, SlurmOptions

options = SlurmOptions(name="add_job", time="00:01:00", cpus_per_task=1, mem_per_cpu="100M")

@slurm_job(options)
def fail():
    int("hello")

fail()

>>> ValueError: invalid literal for int() with base 10: 'hello'
```
