"""For slurm related code"""

import datetime
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TextIO, Union

import sh
from simple_slurm import Slurm

from pyjob.core import FunctionCall, Job, Template, tail_output

logger = logging.getLogger(__name__)


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
        """Load options from a dictionary."""
        return cls(**data)

    @classmethod
    def load(cls, file: TextIO) -> "SlurmOptions":
        """Load options from a file."""
        data = json.load(file)
        return cls.loads(data)

    def to_dict(self) -> dict[str, Any]:
        """Convert the options to a dictionary."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class SlurmError(Exception):
    """Exception raised for errors in the slurm job."""


class SlurmJob(Job):
    """Class to manage a slurm job."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
        assets_root: Union[str, Path, None] = None,
        options: SlurmOptions = SlurmOptions(),
        listener_target_dir: Optional[Path] = None,
    ):
        super().__init__(function_call, script_template, timeout, assets_root)
        self.options = options
        self.listener_target_dir = listener_target_dir

    def submit(self) -> int:
        """Submit the job."""
        has_sbatch = bool(sh.Command("which")("sbatch", _ok_code=[0, 1]))
        job = Slurm(**self.options.to_dict())
        job.add_cmd(f"bash {self.resources.job_script}")
        if has_sbatch:
            self.id = job.sbatch()
            self.status.set_start()
        else:
            target_dir = self.listener_target_dir or Path("slurm_jobs")
            if self.listener_target_dir and not self.listener_target_dir.exists():
                target_dir.mkdir(parents=True)
            elif self.listener_target_dir and not self.listener_target_dir.is_dir():
                raise ValueError(f"{self.listener_target_dir} is not a directory")
            # generate a unique filename
            script_file = target_dir / f"slurm-{uuid.uuid4()}.sh"
            script_file.write_text(job.script(), encoding="utf-8")
            id_file = script_file.with_suffix(".id")
            while not id_file.exists():
                time.sleep(0.5)
            self.id = int(id_file.read_text(encoding="utf-8").strip())
            id_file.unlink()
            if self.id < 1:
                raise SlurmError(f"Error submitting job:\n{job.script()}")
        return self.id

    def run(self) -> Any:
        """Run the job and return the result."""
        self.submit()
        if self.options.output:
            output_file = Path(self.options.output)
        else:
            output_file = Path(f"slurm-{self.id}.out")
        if output_file.exists():
            output_file.unlink()  # remove the file if it already exists
        finished_event = tail_output(output_file)
        result = self.result()

        finished_event.wait()

        return result


def slurm_job(
    options: SlurmOptions = SlurmOptions(),
    script_template: Union[str, Template] = Template(),
    timeout: datetime.timedelta = datetime.timedelta(minutes=10),
    assets_root: Union[str, Path, None] = None,
):
    """Decorator to submit a function as a Slurm job."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            function_call = FunctionCall(func, args, kwargs)
            job = SlurmJob(function_call, script_template, timeout, assets_root, options)
            return job.run()

        return wrapper

    return decorator


def slurm_job_listener(target_dir: Path = Path("slurm_jobs"), poll_interval: int = 1):
    """Listener for slurm jobs in the target directory."""
    if not target_dir.exists():
        target_dir.mkdir(parents=True)
    print("Starting slurm job listener")
    while True:
        for file in target_dir.iterdir():
            if not file.is_file() or not file.suffix == ".sh":
                continue
            print(f"Submitting {file}")
            job_id = 0
            id_file = file.with_suffix(".id")
            try:
                job_id = int(sh.Command("sbatch")(file).split()[-1])  # type: ignore
                print("Submitted job", job_id, "for", file)
            except sh.ErrorReturnCode as e:
                print(e.stderr)
            finally:
                id_file.write_text(str(job_id))
                file.unlink()

        time.sleep(poll_interval)
