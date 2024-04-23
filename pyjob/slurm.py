"""For slurm related code"""

import json
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Any, Callable, Optional, TextIO

from simple_slurm import Slurm

from pyjob.core import TEMPLATE, FunctionCall, Job


@dataclass
class SlurmOptions:  # pylint: disable=too-many-instance-attributes
    """Class to manage slurm options."""

    account: Optional[str] = None
    acctg_freq: Optional[str] = None
    array: Optional[str] = None
    batch: Optional[str] = None
    bb: Optional[str] = None
    bbf: Optional[str] = None
    begin: Optional[str] = None
    chdir: Optional[str] = None
    cluster_constraint: Optional[str] = None
    clusters: Optional[str] = None
    comment: Optional[str] = None
    constraint: Optional[str] = None
    container: Optional[str] = None
    container_id: Optional[str] = None
    contiguous: bool = False
    core_spec: Optional[int] = None
    cores_per_socket: Optional[int] = None
    cpu_freq: Optional[str] = None
    cpus_per_gpu: Optional[int] = None
    cpus_per_task: Optional[int] = None
    deadline: Optional[str] = None
    delay_boot: Optional[int] = None
    dependency: Optional[str] = None
    distribution: Optional[str] = None
    error: Optional[str] = None
    exclude: Optional[str] = None
    exclusive: Optional[str] = None
    export: Optional[str] = None
    export_file: Optional[str] = None
    extra: Optional[str] = None
    extra_node_info: Optional[str] = None
    get_user_env: Optional[str] = None
    gid: Optional[str] = None
    gpu_bind: Optional[str] = None
    gpu_freq: Optional[str] = None
    gpus: Optional[str] = None
    gpus_per_node: Optional[str] = None
    gpus_per_socket: Optional[str] = None
    gpus_per_task: Optional[str] = None
    gres: Optional[str] = None
    gres_flags: Optional[str] = None
    hold: bool = False
    ignore_pbs: bool = False
    input: Optional[str] = None
    job_name: Optional[str] = None
    kill_on_invalid_dep: Optional[str] = None
    licenses: Optional[str] = None
    mail_type: Optional[str] = None
    mail_user: Optional[str] = None
    mcs_label: Optional[str] = None
    mem: Optional[str] = None
    mem_bind: Optional[str] = None
    mem_per_cpu: Optional[str] = None
    mem_per_gpu: Optional[str] = None
    mincpus: Optional[int] = None
    network: Optional[str] = None
    nice: Optional[int] = None
    no_kill: bool = False
    no_requeue: bool = False
    nodefile: Optional[str] = None
    nodelist: Optional[str] = None
    nodes: Optional[str] = None
    ntasks: Optional[int] = None
    ntasks_per_core: Optional[int] = None
    ntasks_per_gpu: Optional[int] = None
    ntasks_per_node: Optional[int] = None
    ntasks_per_socket: Optional[int] = None
    open_mode: Optional[str] = None
    output: Optional[str] = None
    overcommit: bool = False
    partition: Optional[str] = None
    power: Optional[str] = None
    prefer: Optional[str] = None
    priority: Optional[str] = None
    profile: Optional[str] = None
    propagate: Optional[str] = None
    qos: Optional[str] = None
    quiet: bool = False
    reboot: bool = False
    requeue: bool = False
    reservation: Optional[str] = None
    signal: Optional[str] = None
    sockets_per_node: Optional[int] = None
    spread_job: bool = False
    switches: Optional[str] = None
    test_only: bool = False
    thread_spec: Optional[int] = None
    threads_per_core: Optional[int] = None
    time: Optional[str] = None
    time_min: Optional[str] = None
    tmp: Optional[str] = None
    tres_per_task: Optional[str] = None
    uid: Optional[str] = None
    use_min_nodes: bool = False
    verbose: bool = False
    wait: bool = False
    wait_all_nodes: Optional[int] = None
    wckey: Optional[str] = None
    wrap: Optional[str] = None

    @classmethod
    def loads(cls, data: dict[str, Any]) -> "SlurmOptions":
        """
        Load options from a dictionary.

        Args:
            data (dict[str, Any]): dictionary with options

        Returns:
            SlurmOptions: instance with the options
        """
        return cls(**data)

    @classmethod
    def load(cls, file: TextIO) -> "SlurmOptions":
        """
        Load options from a file.

        Args:
            file (TextIO): file with the options

        Returns:
            SlurmOptions: instance with the options
        """
        data = json.load(file)
        return cls.loads(data)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the options to a dictionary.

        Returns:
            Dict[str, Any]: dictionary with the options
        """
        return {k: v for k, v in asdict(self).items() if v is not None}


class SlurmJob(Job):
    """Class to manage a slurm job."""

    def __init__(
        self,
        function_call: Any,
        script_template: str = TEMPLATE,
        options: SlurmOptions = SlurmOptions(),
    ):
        super().__init__(function_call, script_template)
        self.options = options

    def submit(self) -> int:
        """Submit the job."""
        job = Slurm(self.options.to_dict())
        job_id = job.sbatch(f"bash {self.resources.job_script}")
        self.status = "RUNNING"
        return job_id

    def run(self) -> Any:
        """Run the job."""
        job = Slurm(self.options.to_dict())
        self.status = "RUNNING"
        job.srun(f"bash {self.resources.job_script}")
        return self._load_return()


def slurm_job(options: SlurmOptions = SlurmOptions()):
    """Decorator to submit a function as a Slurm job."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            function_call = FunctionCall(func, args, kwargs)
            job = SlurmJob(function_call, options=options)
            job_id = job.submit()
            print(f"Submitted Slurm job with ID: {job_id}")
            return job.run()

        return wrapper

    return decorator
