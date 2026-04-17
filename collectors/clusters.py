"""Collect all-purpose cluster state from the Databricks Clusters API."""

from __future__ import annotations

import time

from client import DatabricksClient


def _fmt_uptime(start_ms: int) -> str:
    if not start_ms:
        return "-"
    seconds = int(time.time() - start_ms / 1000)
    if seconds < 0:
        return "-"
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    return f"{h}h {m}m"


def fetch(client: DatabricksClient) -> list[dict]:
    """Return one row per cluster with state + sizing info.

    Hits GET /api/2.0/clusters/list. For RUNNING clusters we compute uptime
    from the reported start_time (epoch ms) so reviewers can spot clusters
    that have been left on for a long time.
    """
    payload = client.get("/api/2.0/clusters/list")

    rows: list[dict] = []
    for c in payload.get("clusters", []):
        # Describe sizing: either a fixed worker count or an autoscale range.
        autoscale = c.get("autoscale")
        if autoscale:
            sizing = f"autoscale {autoscale.get('min_workers')}-{autoscale.get('max_workers')}"
        else:
            sizing = f"fixed {c.get('num_workers', 0)}"

        state = c.get("state", "UNKNOWN")
        uptime = _fmt_uptime(c.get("start_time", 0)) if state == "RUNNING" else "-"

        rows.append({
            "cluster_name": c.get("cluster_name", "(unnamed)"),
            "state": state,
            "node_type": c.get("node_type_id", "-"),
            "sizing": sizing,
            "uptime": uptime,
        })
    return rows
