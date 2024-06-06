"""Utility functions for file handling"""

import datetime
import threading
import time
from pathlib import Path

import sh
from rich.console import Console

END_OF_JOB = "JOBEND"


def wait_for_file(file_path: Path, timeout: datetime.timedelta, interval: float = 0.1) -> None:
    """Wait for a file to exist."""
    start = datetime.datetime.now()
    while not file_path.exists():
        if datetime.datetime.now() - start > timeout:
            raise TimeoutError(
                f"File {file_path} did not appear within {timeout.total_seconds()} seconds"
            )
        time.sleep(interval)

    print(f"File {file_path} appeared after {datetime.datetime.now() - start}")


def tail_output(file_path: Path, name: str = "", end_pattern: str = END_OF_JOB) -> threading.Event:
    """
    Tail the output of a file in a separate thread.

    Args:
        file_path (Path): Path to the file to tail.
        name (str, optional): Name to prefix each line of the output. Defaults to "".
        end_pattern (str, optional): Pattern to signal the end of the output. Defaults to "JOBEND".

    Returns:
        threading.Event: Event to signal when the tail output has finished.
    """
    finished_event = threading.Event()

    # Define a list of colors for each thread
    colors = ["cyan", "magenta", "yellow", "green", "blue", "red"]

    def tail_thread():
        # Get the color for the current thread
        thread_color = colors[threading.get_ident() % len(colors)]

        # Wait for the file to exist
        while not file_path.exists():
            time.sleep(1)

        # Tail the file
        for line in sh.Command("tail")("-f", file_path, _iter=True):  # type: ignore
            assert isinstance(line, str)
            if line.strip() == end_pattern.strip():
                break
            line = f"{name} | {line}"
            console = Console()
            console.print(line, end="", style=f"bold {thread_color}")

        # Signal that the tail output has finished
        finished_event.set()

    # Start the tail thread
    thread = threading.Thread(target=tail_thread)
    thread.start()

    return finished_event
