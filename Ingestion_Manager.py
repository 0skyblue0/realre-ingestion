"""
Central entrypoint for the data ingestion manager.

Usage
-----
python Ingestion_Manager.py --schedule schedules/interval_schedule.json --once

CLI flags mirror :func:`manager.core.build_arg_parser`.
"""

from manager.core import run_from_cli


def main() -> None:
    run_from_cli()


if __name__ == "__main__":
    main()
