from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc


def parse_iso_timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def ensure_hour_alignment(dt: datetime) -> None:
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValueError("Timestamps must align exactly on the hour")


def hour_range(start: datetime, end: datetime) -> list[datetime]:
    """Return list of hourly timestamps from start (inclusive) to end (exclusive)."""
    ensure_hour_alignment(start)
    ensure_hour_alignment(end)
    if end <= start:
        return []
    hours = []
    current = start
    while current < end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours

