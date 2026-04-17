"""Collect ACL grants across jobs, clusters, serving endpoints, and the
workspace token ACL.

Produces one row per (object, principal, permission_level) grant. Inherited
grants are skipped so output shows only direct assignments — the grants a
reviewer would act on when tightening access.
"""

from __future__ import annotations

import requests

from client import DatabricksClient


def _list_jobs(client: DatabricksClient) -> list[dict]:
    jobs: list[dict] = []
    params: dict = {"limit": 25, "expand_tasks": "false"}
    while True:
        payload = client.get("/api/2.1/jobs/list", params=params)
        jobs.extend(payload.get("jobs") or [])
        if not payload.get("has_more"):
            break
        params = {"page_token": payload.get("next_page_token")}
    return jobs


def _list_clusters(client: DatabricksClient) -> list[dict]:
    payload = client.get("/api/2.0/clusters/list")
    return payload.get("clusters") or []


def _list_endpoints(client: DatabricksClient) -> list[dict]:
    payload = client.get("/api/2.0/serving-endpoints")
    return payload.get("endpoints") or []


def _principal(entry: dict) -> tuple[str, str] | None:
    if entry.get("user_name"):
        return ("USER", entry["user_name"])
    if entry.get("group_name"):
        return ("GROUP", entry["group_name"])
    if entry.get("service_principal_name"):
        return ("SERVICE_PRINCIPAL", entry["service_principal_name"])
    return None


def _acl_rows(object_type: str, object_name: str, payload: dict) -> list[dict]:
    rows: list[dict] = []
    for entry in payload.get("access_control_list") or []:
        principal = _principal(entry)
        if not principal:
            continue
        ptype, pname = principal
        for perm in entry.get("all_permissions") or []:
            if perm.get("inherited"):
                continue
            level = perm.get("permission_level")
            if not level:
                continue
            rows.append({
                "object_type": object_type,
                "object_name": object_name,
                "principal_type": ptype,
                "principal": pname,
                "permission_level": level,
            })
    return rows


def fetch(client: DatabricksClient) -> list[dict]:
    """Return one row per direct permission grant across covered objects."""
    rows: list[dict] = []

    for job in _list_jobs(client):
        job_id = job.get("job_id")
        if job_id is None:
            continue
        name = (job.get("settings") or {}).get("name") or f"job {job_id}"
        try:
            payload = client.get(f"/api/2.0/permissions/jobs/{job_id}")
        except requests.HTTPError:
            continue
        rows.extend(_acl_rows("JOB", name, payload))

    for cluster in _list_clusters(client):
        cluster_id = cluster.get("cluster_id")
        if not cluster_id:
            continue
        name = cluster.get("cluster_name") or cluster_id
        try:
            payload = client.get(f"/api/2.0/permissions/clusters/{cluster_id}")
        except requests.HTTPError:
            continue
        rows.extend(_acl_rows("CLUSTER", name, payload))

    for ep in _list_endpoints(client):
        ep_id = ep.get("id")
        ep_name = ep.get("name") or ep_id
        if not ep_id:
            continue
        try:
            payload = client.get(f"/api/2.0/permissions/serving-endpoints/{ep_id}")
        except requests.HTTPError:
            continue
        rows.extend(_acl_rows("SERVING_ENDPOINT", ep_name, payload))

    try:
        token_payload = client.get("/api/2.0/permissions/authorization/tokens")
        rows.extend(_acl_rows("TOKEN_ACL", "workspace", token_payload))
    except requests.HTTPError:
        pass

    return rows
