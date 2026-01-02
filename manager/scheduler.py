from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List


def _parse_time(value: str) -> time:
    hour, minute = [int(part) for part in value.split(":", 1)]
    return time(hour=hour, minute=minute, tzinfo=timezone.utc)


def _weekday_to_int(value: str) -> int:
    mapping = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    return mapping[value.lower()]


@dataclass
class ScheduledJob:
    name: str
    args: dict[str, Any] = field(default_factory=dict)
    schedule: dict[str, Any] = field(default_factory=dict)
    next_run: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def compute_next_run(self, now: datetime) -> datetime:
        schedule_type = self.schedule.get("type", "interval")
        if schedule_type == "interval":
            seconds = int(self.schedule.get("seconds", 300))
            return now + timedelta(seconds=seconds)
        if schedule_type == "daily":
            at = _parse_time(self.schedule["time"])
            candidate = datetime.combine(now.date(), at)
            if candidate <= now:
                candidate = candidate + timedelta(days=1)
            return candidate
        if schedule_type == "weekly":
            at = _parse_time(self.schedule["time"])
            target_weekday = _weekday_to_int(self.schedule["weekday"])
            days_ahead = (target_weekday - now.weekday()) % 7
            candidate_date = now.date() + timedelta(days=days_ahead)
            candidate = datetime.combine(candidate_date, at)
            if candidate <= now:
                candidate = candidate + timedelta(days=7)
            return candidate
        return now

    def due(self, now: datetime) -> bool:
        return now >= self.next_run

    def mark_executed(self, now: datetime) -> None:
        self.next_run = self.compute_next_run(now)


class Scheduler:
    def __init__(self, jobs: Iterable[ScheduledJob]):
        self.jobs = list(jobs)

    @classmethod
    def from_file(cls, path: str | Path) -> "Scheduler":
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
        jobs: List[ScheduledJob] = []
        for entry in payload.get("jobs", []):
            jobs.append(
                ScheduledJob(
                    name=entry["name"],
                    args=entry.get("args", {}),
                    schedule=entry.get("schedule", {"type": "interval", "seconds": 300}),
                )
            )
        return cls(jobs)

    def due_jobs(self, now: datetime | None = None) -> list[ScheduledJob]:
        now = now or datetime.now(timezone.utc)
        ready = [job for job in self.jobs if job.due(now)]
        for job in ready:
            job.mark_executed(now)
        return ready
