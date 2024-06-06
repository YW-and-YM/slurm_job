"""Xenon specific SlurmJob decorator."""

import datetime
import getpass
from pathlib import Path
from typing import Any, Callable, Union

from pyjob.core import Template
from pyjob.slurm import SlurmOptions, slurm_job

USER = getpass.getuser()
BIND = {
    "default": ",".join(
        [
            "/project2/lgrandi/xenonnt/dali",
            "/project2",
            "/project",
            f"/scratch/midway2/{USER}",
            f"/scratch/midway3/{USER}",
        ]
    ),
    "dali": ",".join(
        [
            "/dali/lgrandi",
            "/project2/lgrandi/xenonnt/xenon.config",
            "/project2/lgrandi/grid_proxy/xenon_service_proxy",
        ]
    ),
}

SINGULARITY_DIR = {
    "default": "/project2/lgrandi/xenonnt/singularity-images",
    "dali": "/dali/lgrandi/xenonnt/singularity-images",
}


XENON_SLURM_TEMPLATE = Template(
    before=(
        "unset X509_CERT_DIR\n"
        'if [ "$INSTALL_CUTAX" == "1" ]; then unset CUTAX_LOCATION; fi\n'
        "module load singularity\n"
    ),
    python_exec="singularity exec --bind {bind} {singularity_image} python",
)


def xenon_template(singularity_image: Union[str, Path], is_dali=False) -> str:
    """Create a script template for a Xenon job."""
    key = "dali" if is_dali else "default"
    if isinstance(singularity_image, str):
        if Path(singularity_image).exists():
            singularity_image = Path(singularity_image).resolve()
        else:
            singularity_image = Path(SINGULARITY_DIR[key]) / singularity_image
    return XENON_SLURM_TEMPLATE.format(
        bind=BIND[key],
        singularity_image=singularity_image,
        pickle_base64="{pickle_base64}",
        ret_path="{ret_path}",
    )


def xenon_job(
    options: SlurmOptions = SlurmOptions(),
    singularity_image: Union[str, Path] = "xenonnt-2024.04.1.simg",
    is_dali: bool = False,
    timeout: datetime.timedelta = datetime.timedelta(minutes=10),
    wait: bool = True,
) -> Callable[..., Any]:
    """
    Decorator to submit a function as a Xenon job.

    Args:
        options (SlurmOptions, optional): Slurm options. Defaults to SlurmOptions().
        singularity_image (Union[str, Path], optional): Path or name of the singularity image. Defaults to "xenonnt-2024.04.1.simg".
        is_dali (bool, optional): Set to True if using on Dali. Defaults to False.
        timeout (datetime.timedelta, optional): Timeout. Defaults to datetime.timedelta(minutes=10).
        wait (bool, optional): Set to False to return with job id immediately after submit. Defaults to True.

    Returns:
        Callable[..., Any]: Decorated function.
    """

    return slurm_job(
        options=options,
        script_template=xenon_template(singularity_image, is_dali),
        timeout=timeout,
        wait=wait,
    )
