"""Collect recent job runs from the Databricks Jobs API."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from client import DatabricksClient


def _fmt_duration(ms: int) -> str:
    """Human-friendly H:M:S from a millisecond duration."""
    seconds = max(0, int(ms // 1000))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def _fmt_ts(ms: int) -> str:
    """Convert epoch-millis to an ISO-8601 UTC string."""
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fetch(client: DatabricksClient, hours: int = 24) -> list[dict]:
    """Return a list of recent job runs within the lookback window.

    Hits GET /api/2.1/jobs/runs/list. Databricks expresses times in epoch
    *milliseconds* (not seconds) — a very common gotcha when hand-rolling
    integrations, so the conversion is called out explicitly below.
    """
    # Databricks wants epoch MILLISECONDS, so multiply Python's seconds by 1000.
    start_time_from = int((time.time() - hours * 3600) * 1000)

    rows: list[dict] = []
    params: dict = {"start_time_from": start_time_from, "limit": 25, "expand_tasks": "false"}

    # The API pages results; we follow next_page_token until the server says stop.
    while True:
        payload = client.get("/api/2.1/jobs/runs/list", params=params)
        for run in payload.get("runs", []):
            start_ms = run.get("start_time", 0)
            end_ms = run.get("end_time", 0)
            duration = _fmt_duration(end_ms - start_ms) if end_ms else "in-progress"
            rows.append({
                "run_name": run.get("run_name") or f"job {run.get('job_id', '?')}",
                "run_id": run.get("run_id"),
                "result_state": (run.get("state") or {}).get("result_state") or (run.get("state") or {}).get("life_cycle_state", "UNKNOWN"),
                "start_time": _fmt_ts(start_ms),
                "duration": duration,
            })

        if not payload.get("has_more"):
            break
        # Jobs API 2.1 uses next_page_token for cursor-based pagination.
        token = payload.get("next_page_token")
        if not token:
            break
        params = {"page_token": token}

    return rows
