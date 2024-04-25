from pathlib import Path

from pyjob.slurm import slurm_job_listener

slurm_job_listener(Path("slurm_jobs"))
