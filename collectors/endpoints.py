"""Collect Model Serving endpoints from the Databricks Serving API.

Three views are exposed:
  - fetch():         one row per endpoint (name, state, creator, created)
  - fetch_metrics(): per-endpoint traffic metrics from the Prometheus
                     /metrics route (request count, errors, latency, CPU/mem)
  - fetch_events():  recent config-change / deployment events per endpoint

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


# Metric names exposed by the public /metrics route. Databricks only surfaces
# request counters here; latency histograms and CPU/mem gauges are not emitted
# (those live in system.serving.* inference tables, SQL only).
_METRIC_REQUEST_TOTAL = "request_count_total"
_METRIC_4XX = "request_4xx_count_total"
_METRIC_5XX = "request_5xx_count_total"


def _parse_prom(text: str) -> dict[str, float]:
    """Sum each metric name across all label sets — small, dependency-free.

    Databricks emits lines as `metric{labels} value timestamp_ms` — the
    trailing token is an epoch-ms timestamp, not the value, so we take the
    first numeric token after the labels block.
    """
    totals: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip the labels block (if any) so we can tokenize the rest cleanly.
        brace_end = line.find("}")
        if brace_end != -1:
            name = line[:line.find("{")]
            tail = line[brace_end + 1:].split()
        else:
            parts = line.split()
            name, tail = parts[0], parts[1:]
        if not tail:
            continue
        try:
            value = float(tail[0])
        except ValueError:
            continue
        totals[name] = totals.get(name, 0.0) + value
    return totals


def fetch_metrics(client: DatabricksClient) -> list[dict]:
    """One row per endpoint with traffic + resource metrics."""
    rows: list[dict] = []
    for name in _list_endpoint_names(client):
        try:
            text = client.get_text(f"/api/2.0/serving-endpoints/{name}/metrics")
        except requests.HTTPError:
            # Brand-new endpoints can 404 on /metrics — skip cleanly.
            continue
        m = _parse_prom(text)
        rows.append({
            "name": name,
            "requests": int(m.get(_METRIC_REQUEST_TOTAL, 0)),
            "errors_4xx": int(m.get(_METRIC_4XX, 0)),
            "errors_5xx": int(m.get(_METRIC_5XX, 0)),
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
