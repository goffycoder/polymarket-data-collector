from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EASTERN_TZ = ZoneInfo("America/New_York")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip().replace("Z", "+00:00")
    if not normalized:
        return None
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_eastern(value: str | None, *, fallback: str = "") -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        return fallback
    local = parsed.astimezone(EASTERN_TZ)
    month = local.strftime("%b")
    day = str(local.day)
    year = str(local.year)
    clock = local.strftime("%I:%M %p").lstrip("0")
    return f"{month} {day}, {year} {clock} ET"
