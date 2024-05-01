"""SlurmJob for Xenon"""

import getpass

from pyjob.core import Template

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


def xenon_template(singularity_image, is_dali=False):
    """Create a script template for a Xenon job."""
    key = "dali" if is_dali else "default"
    return XENON_SLURM_TEMPLATE.format(
        bind=BIND[key],
        singularity_dir=SINGULARITY_DIR[key],
        image=singularity_image,
        pickle_base64="{pickle_base64}",
        ret_path="{ret_path}",
    )
