from __future__ import annotations

import argparse
import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from utility.Logger import create_logger

from .clients import ClientLoader, ClientLoadError
from .db import DBAdapter
from .scheduler import Scheduler


class IngestionManager:
    """
    Orchestrates scheduled jobs, client access, and persistence.
    """

    def __init__(
        self,
        *,
        db_path: str = "realre_ingestion.db",
        async_mode: bool = False,
        logger_name: str = "ingestion.manager",
    ):
        self.logger = create_logger(logger_name)
        self.db = DBAdapter(db_path)
        self.client_loader = ClientLoader()
        self.async_mode = async_mode
        self._schedule: Scheduler | None = None

    # ---------------------------------------------------------------- schedule
    def load_schedule(self, schedule_path: str | Path) -> None:
        self._schedule = Scheduler.from_file(schedule_path)
        self.logger.info("Loaded schedule from %s", schedule_path)

    # ----------------------------------------------------------------- clients
    def request_source_data(self, client: str, method: str, **params: Any) -> Any:
        try:
            module = self.client_loader.load(client)
        except ClientLoadError as exc:
            self.logger.warning("Falling back to mock client due to: %s", exc)
            module = self.client_loader.load("mock")
        return self.client_loader.call(module, method, **params)

    # --------------------------------------------------------------------- DB
    def log_history(self, **kwargs: Any) -> int:
        return self.db.log_history(**kwargs)

    def fetch_history(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.db.fetch_history(limit)

    def upsert_scd2(
        self,
        table: str,
        records: list[dict[str, Any]],
        key_fields: list[str],
        attribute_fields: list[str] | None = None,
    ) -> int:
        return self.db.upsert_scd2(
            table=table,
            records=records,
            key_fields=key_fields,
            attribute_fields=attribute_fields,
        )

    # -------------------------------------------------------------------- jobs
    def _run_job(self, job_name: str, job_callable: Callable[..., Any], args: dict[str, Any]) -> None:
        start = time.perf_counter()
        started_at = datetime.now(timezone.utc).isoformat()
        history_id = self.log_history(
            job_name=job_name,
            event_type="job_start",
            status="started",
            started_at=started_at,
            details={"args": args},
        )
        try:
            result = job_callable(manager=self, **args)
            duration_ms = int((time.perf_counter() - start) * 1000)
            row_count = result.get("row_count") if isinstance(result, dict) else None
            self.log_history(
                job_name=job_name,
                event_type="job_end",
                status="success",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                row_count=row_count,
                details=result,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            duration_ms = int((time.perf_counter() - start) * 1000)
            self.log_history(
                job_name=job_name,
                event_type="job_error",
                status="failed",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                details={"error": str(exc)},
            )
            self.logger.exception("Job %s failed", job_name)

    async def _run_job_async(self, job_name: str, job_callable: Callable[..., Any], args: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_job, job_name, job_callable, args)

    def _get_job_callable(self, job_name: str) -> Callable[..., Any]:
        from jobs import JOB_REGISTRY

        try:
            return JOB_REGISTRY[job_name]
        except KeyError as exc:
            raise RuntimeError(f"Unknown job '{job_name}'") from exc

    # ------------------------------------------------------------------- loops
    def run_once(self) -> None:
        if self._schedule is None:
            raise RuntimeError("Schedule not loaded.")
        now = datetime.now(timezone.utc)
        jobs = self._schedule.due_jobs(now)
        if not jobs:
            self.logger.info("No jobs due at %s", now.isoformat())
            return
        tasks: list[Any] = []
        for job in jobs:
            job_callable = self._get_job_callable(job.name)
            if self.async_mode:
                tasks.append(self._run_job_async(job.name, job_callable, job.args))
            else:
                self._run_job(job.name, job_callable, job.args)
        if tasks:
            asyncio.run(asyncio.gather(*tasks))

    def run_forever(self, poll_interval: int = 5) -> None:
        if self._schedule is None:
            raise RuntimeError("Schedule not loaded.")
        self.logger.info("Starting scheduler loop (async=%s)", self.async_mode)
        while True:
            self.run_once()
            time.sleep(poll_interval)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingestion manager runner.")
    parser.add_argument("--schedule", required=True, help="Path to schedule JSON file.")
    parser.add_argument("--db", default="realre_ingestion.db", help="SQLite database path.")
    parser.add_argument("--once", action="store_true", help="Run jobs that are due only once and exit.")
    parser.add_argument("--async", dest="async_mode", action="store_true", help="Enable asyncio execution.")
    parser.add_argument("--poll", type=int, default=5, help="Scheduler poll interval (seconds).")
    return parser


def run_from_cli(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    manager = IngestionManager(db_path=args.db, async_mode=args.async_mode)
    manager.load_schedule(args.schedule)
    if args.once:
        manager.run_once()
    else:
        manager.run_forever(poll_interval=args.poll)
