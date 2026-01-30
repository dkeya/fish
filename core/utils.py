from __future__ import annotations

from datetime import datetime, date, timezone


def iso_today() -> str:
    return date.today().isoformat()


def iso_now() -> str:
    # Use UTC ISO timestamps for consistency.
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d else 0.0
