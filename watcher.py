from pathlib import Path

from pyjob.slurm import watch_slurm_jobs

watch_slurm_jobs(Path("slurm_jobs"))
