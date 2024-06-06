import argparse

from slurm_job.slurm import watch_slurm_jobs


def main():
    parser = argparse.ArgumentParser(description="Watch a directory for Slurm job scripts.")
    parser.add_argument(
        "directory",
        type=str,
        help="Directory to watch for Slurm job scripts.",
        default="slurm_jobs",
    )
    args = parser.parse_args()

    watch_slurm_jobs(args.directory)


if __name__ == "__main__":
    main()
