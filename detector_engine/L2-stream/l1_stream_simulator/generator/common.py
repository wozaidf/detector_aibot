"""通用时间与数值工具。

提供 ISO 时间解析、UTC 格式化以及可选值四舍五入辅助函数。
"""

from __future__ import annotations

from datetime import datetime, timezone


def parse_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def rounded(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(float(value), digits)
