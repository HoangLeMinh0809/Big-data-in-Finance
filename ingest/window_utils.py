from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WindowConfig:
    mode: str
    batch_lookback_days: int
    realtime_lookback_minutes: int
    poll_seconds: int
    continuous: bool
    start_override: str
    end_override: str
    state_file: Path


@dataclass(frozen=True)
class WindowRange:
    start_utc: datetime
    end_utc: datetime
    now_utc: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_bool(raw_value: str, default: bool = False) -> bool:
    value = (raw_value or "").strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def parse_iso_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def resolve_mode(raw_mode: str, default_mode: str = "batch") -> str:
    mode = (raw_mode or "").strip().lower()
    if mode in {"batch", "realtime"}:
        return mode
    return default_mode


def resolve_window(config: WindowConfig, now_utc: datetime | None = None) -> WindowRange:
    now = now_utc or utc_now()
    end_utc = parse_iso_datetime(config.end_override) if config.end_override else now

    if config.start_override:
        start_utc = parse_iso_datetime(config.start_override)
    elif config.mode == "realtime":
        start_utc = end_utc - timedelta(minutes=max(config.realtime_lookback_minutes, 1))
    else:
        start_utc = end_utc - timedelta(days=max(config.batch_lookback_days, 1))

    if start_utc >= end_utc:
        raise ValueError("Window start must be earlier than end")

    return WindowRange(start_utc=start_utc, end_utc=end_utc, now_utc=now)


def write_window_state(
    path: Path,
    source: str,
    config: WindowConfig,
    window: WindowRange,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "source": source,
        "mode": config.mode,
        "continuous": config.continuous,
        "batch_lookback_days": config.batch_lookback_days,
        "realtime_lookback_minutes": config.realtime_lookback_minutes,
        "poll_seconds": config.poll_seconds,
        "now_utc": window.now_utc.isoformat(),
        "window_start_utc": window.start_utc.isoformat(),
        "window_end_utc": window.end_utc.isoformat(),
        "updated_at_utc": utc_now().isoformat(),
    }
    if extra:
        payload["extra"] = extra

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_default_window_config(
    *,
    mode: str,
    batch_lookback_days: int,
    realtime_lookback_minutes: int,
    poll_seconds: int,
    continuous: bool,
    start_override: str,
    end_override: str,
    state_file: str,
) -> WindowConfig:
    return WindowConfig(
        mode=resolve_mode(mode),
        batch_lookback_days=max(batch_lookback_days, 1),
        realtime_lookback_minutes=max(realtime_lookback_minutes, 1),
        poll_seconds=max(poll_seconds, 1),
        continuous=continuous,
        start_override=(start_override or "").strip(),
        end_override=(end_override or "").strip(),
        state_file=Path(state_file),
    )


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def day_strings_from_window(window: WindowRange) -> list[str]:
    start_day = window.start_utc.date()
    end_day = window.end_utc.date()
    total_days = (end_day - start_day).days + 1
    return [
        (start_day + timedelta(days=offset)).isoformat()
        for offset in range(total_days)
    ]