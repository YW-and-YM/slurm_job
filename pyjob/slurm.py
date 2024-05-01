"""For slurm related code"""

import datetime
import logging
import time
import uuid
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypedDict, Union

import rich
import sh
from simple_slurm import Slurm

from pyjob.core import FunctionCall, Job, Template
from pyjob.utils import tail_output

logger = logging.getLogger(__name__)


class SlurmOptions(TypedDict, total=False):
    """Type definition for Slurm options."""

    account: str
    acctg_freq: str
    array: str
    batch: str
    bb: str
    bbf: str
    begin: str
    chdir: str
    cluster_constraint: str
    clusters: str
    comment: str
    constraint: str
    container: str
    container_id: str
    contiguous: bool
    core_spec: int
    cores_per_socket: int
    cpu_freq: str
    cpus_per_gpu: int
    cpus_per_task: int
    deadline: str
    delay_boot: int
    dependency: str
    distribution: str
    error: str
    exclude: str
    exclusive: str
    export: str
    export_file: str
    extra: str
    extra_node_info: str
    get_user_env: str
    gid: str
    gpu_bind: str
    gpu_freq: str
    gpus: str
    gpus_per_node: str
    gpus_per_socket: str
    gpus_per_task: str
    gres: str
    gres_flags: str
    hold: bool
    ignore_pbs: bool
    input: str
    job_name: str
    kill_on_invalid_dep: str
    licenses: str
    mail_type: str
    mail_user: str
    mcs_label: str
    mem: str
    mem_bind: str
    mem_per_cpu: str
    mem_per_gpu: str
    mincpus: int
    network: str
    nice: int
    no_kill: bool
    no_requeue: bool
    nodefile: str
    nodelist: str
    nodes: str
    ntasks: int
    ntasks_per_core: int
    ntasks_per_gpu: int
    ntasks_per_node: int
    ntasks_per_socket: int
    open_mode: str
    output: str
    overcommit: bool
    partition: str
    power: str
    prefer: str
    priority: str
    profile: str
    propagate: str
    qos: str
    quiet: bool
    reboot: bool
    requeue: bool
    reservation: str
    signal: str
    sockets_per_node: int
    spread_job: bool
    switches: str
    test_only: bool
    thread_spec: int
    threads_per_core: int
    time: str
    time_min: str
    tmp: str
    tres_per_task: str
    uid: str
    use_min_nodes: bool
    verbose: bool
    wait: bool
    wait_all_nodes: int
    wckey: str
    wrap: str


class SlurmError(Exception):
    """Exception raised for errors in the slurm job."""


class SlurmJob(Job):
    """Class to manage a slurm job."""

    def __init__(
        self,
        function_call: FunctionCall,
        script_template: Union[str, Template] = Template(),
        timeout: datetime.timedelta = datetime.timedelta(minutes=10),
        options: SlurmOptions = SlurmOptions(),
        listener_target_dir: Optional[Path] = None,
    ):
        super().__init__(function_call, script_template, timeout)
        self.options = options
        self.name = self.options.get("job_name", self.function_call.func.__name__)
        self.listener_target_dir = listener_target_dir

    def submit(self) -> int:
        """Submit the job."""
        has_sbatch = bool(sh.Command("which")("sbatch", _ok_code=[0, 1]))
        job = Slurm(**self.options)
        job.add_cmd("bash", "-c", self.script)
        if has_sbatch:
            self.id = job.sbatch()
            self.status.set_start()
        else:
            target_dir = self.listener_target_dir or Path("slurm_jobs/listener")
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
        output_file = Path(self.options.get("output", f"slurm-{self.id}.out"))
        if output_file.exists():
            output_file.unlink()  # remove the file if it already exists
        finished_event = tail_output(output_file, f"slurm-{self.id}-{self.name}")
        result = self.result()

        finished_event.wait()

        return result


def slurm_job(
    options: SlurmOptions = SlurmOptions(),
    script_template: Union[str, Template] = Template(),
    timeout: datetime.timedelta = datetime.timedelta(minutes=10),
):
    """Decorator to submit a function as a Slurm job."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            function_call = FunctionCall(func, args, kwargs)
            job = SlurmJob(function_call, script_template, timeout, options)
            return job.run()

        return wrapper

    return decorator


def slurm_job_listener(target_dir: Path = Path("slurm_jobs"), poll_interval: int = 1) -> None:
    """Listener for slurm jobs in the target directory."""
    if not target_dir.exists():
        target_dir.mkdir(parents=True)
    rich.print(
        "Listening for slurm jobs in", target_dir, "with poll interval", poll_interval, "seconds..."
    )
    try:
        while True:
            for file in target_dir.iterdir():
                if not file.is_file() or not file.suffix == ".sh":
                    continue
                rich.print(f"Submitting {file}")
                job_id = 0
                id_file = file.with_suffix(".id")
                try:
                    job_id = int(sh.Command("sbatch")(file).split()[-1])  # type: ignore
                    rich.print("Submitted job", job_id, "for", file)
                except sh.ErrorReturnCode as e:
                    rich.print(e.stderr)
                finally:
                    id_file.write_text(str(job_id))
                    file.unlink()

            time.sleep(poll_interval)
    except KeyboardInterrupt:
        rich.print("Stopping slurm job listener")
        return
