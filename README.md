# PyJob

A simple library to run any python function as a job, designed for XENONnT experiment but can be used for any other purpose.

## Installation

```bash
pip install https://github.com/Microwave-WYB/pyjob.git
```

## Usage

### Basic usage:

```python
from pyjob.slurm import slurm_job
from pyjob.xenon_templates import xenon_template

options = SlurmOptions(
    account="pi-lgrandi",
    partition="xenon1t",
    time="1:00:00",
    mem_per_cpu="100M",
    cpus_per_task=1,
    output="job.out",
)

template = xenon_template(singularity_image="xenonnt-2024.04.1.simg", is_dali=False)


@slurm_job(options=options, script_template=template, timeout=datetime.timedelta(seconds=60))
def add(a, b):
    print(a + b)
    return a + b

print("return:", add(1, 2))
```
Output:
```
Submitted batch job 38996399

slurm-38996399-add | 3
return: 3
```
Where `slurm` describes the type of job, `38996399` is the job id, `add` is the function name, and `3` is the return value.

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
Submitted batch job 38996399

... # stack trace for the JobFailedError
pyjob.core.JobFailedError: Job add with id 38996399 failed with exception: This job will fail
slurm-38996399-fail | Traceback (most recent call last):
slurm-38996399-fail |   File "<stdin>", line 14, in <module>
slurm-38996399-fail |   File "<stdin>", line 10, in <module>
slurm-38996399-fail |   File "/home/user/test/somefile.py", line 6, in fail
slurm-38996399-fail |     raise ValueError("This job will fail")
slurm-38996399-fail | ValueError: This job will fail
```

### Advanced usage (with Jupiter notebook started as a Slurm job):

TODO: Add examples
