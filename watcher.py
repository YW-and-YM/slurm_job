from pathlib import Path

from slurm_job.slurm import watch_slurm_jobs

watch_slurm_jobs(Path("slurm_jobs"))
