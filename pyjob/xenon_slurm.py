"""Xenon specific SlurmJob decorator."""

import datetime
import getpass
from functools import wraps
from typing import Any, Callable

from pyjob.core import FunctionCall, Template
from pyjob.slurm import SlurmJob, SlurmOptions

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
    python_exec="singularity exec --bind {bind} {singularity_dir}/{image} python",
)


def xenon_template(singularity_image: str, is_dali=False) -> str:
    """Create a script template for a Xenon job."""
    key = "dali" if is_dali else "default"
    return XENON_SLURM_TEMPLATE.format(
        bind=BIND[key],
        singularity_dir=SINGULARITY_DIR[key],
        image=singularity_image,
        pickle_base64="{pickle_base64}",
        ret_path="{ret_path}",
    )


def slurm_job(
    options: SlurmOptions = SlurmOptions(),
    singularity_image: str = "xenonnt-2024.04.1.simg",
    is_dali: bool = False,
    timeout: datetime.timedelta = datetime.timedelta(minutes=10),
) -> Callable[..., Any]:
    """Decorator to submit a function as a Slurm job."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            function_call = FunctionCall(func, args, kwargs)
            job = SlurmJob(
                function_call, xenon_template(singularity_image, is_dali), timeout, options
            )
            return job.run()

        return wrapper

    return decorator
