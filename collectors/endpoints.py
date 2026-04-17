"""Collect Model Serving endpoints from the Databricks Serving API.

Note: detailed traffic metrics (QPS, latency, token usage) are not returned
by this endpoint — they live in the `system.serving.*` / inference tables
and are out of scope for this POC.
"""

from __future__ import annotations

from datetime import datetime, timezone

from client import DatabricksClient


def _fmt_ts(ms: int) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fetch(client: DatabricksClient) -> list[dict]:
    """Return a row per serving endpoint, or an empty list if none exist."""
    # GET /api/2.0/serving-endpoints lists every endpoint in the workspace.
    payload = client.get("/api/2.0/serving-endpoints")

    rows: list[dict] = []
    for ep in payload.get("endpoints", []) or []:
        state = ep.get("state", {}) or {}
        rows.append({
            "name": ep.get("name", "(unnamed)"),
            "state": state.get("ready") or state.get("config_update") or "UNKNOWN",
            "creator": ep.get("creator", "-"),
            "created": _fmt_ts(ep.get("creation_timestamp", 0)),
        })
    return rows
