"""Collect Model Serving endpoints from the Databricks Serving API.

Two views are exposed:
  - fetch():        one row per endpoint (name, state, creator, created)
  - fetch_events(): recent config-change / deployment events per endpoint

Fine-grained per-request traces still live in `system.serving.*` inference
tables (SQL only) and are out of scope for this POC.
"""

from __future__ import annotations

from datetime import datetime, timezone

import requests

from client import DatabricksClient


def _fmt_ts(ms: int) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _list_endpoint_names(client: DatabricksClient) -> list[str]:
    payload = client.get("/api/2.0/serving-endpoints")
    return [ep.get("name") for ep in (payload.get("endpoints") or []) if ep.get("name")]


def fetch(client: DatabricksClient) -> list[dict]:
    """Return a row per serving endpoint, or an empty list if none exist."""
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


def fetch_events(client: DatabricksClient, limit: int = 5) -> list[dict]:
    """Recent config / deployment events across all endpoints."""
    rows: list[dict] = []
    for name in _list_endpoint_names(client):
        try:
            payload = client.get(f"/api/2.0/serving-endpoints/{name}/events")
        except requests.HTTPError:
            continue
        events = payload.get("events") or []
        # API returns newest first; cap per-endpoint so output stays scannable.
        for ev in events[:limit]:
            rows.append({
                "endpoint": name,
                "event_type": ev.get("event_type", "-"),
                "status": ev.get("status", "-"),
                "message": (ev.get("message") or "-").splitlines()[0][:80],
                "timestamp": _fmt_ts(ev.get("timestamp", 0)),
            })
    return rows
