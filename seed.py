"""Seed the Databricks workspace with a cluster, a notebook, and a job run.

Run this once so `python main.py` has real data to display. Uses the same
PAT auth as the rest of the POC (DATABRICKS_HOST / DATABRICKS_TOKEN).

Usage:
    python seed.py
    python seed.py --skip-cluster
    python seed.py --spark-version 14.3.x-scala2.12
"""

from __future__ import annotations

import argparse
import base64
import sys

from dotenv import load_dotenv
from requests import HTTPError

from client import DatabricksClient


NOTEBOOK_SOURCE = 'print("hello from seed")\n'
SINGLE_NODE_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
}
SINGLE_NODE_TAGS = {"ResourceClass": "SingleNode"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create a cluster + job run so main.py has data.")
    p.add_argument("--cluster-name", default="poc-seed-cluster")
    p.add_argument("--job-name", default="poc-seed-job")
    p.add_argument("--node-type-id", default="Standard_DS3_v2", help="Azure default.")
    p.add_argument("--spark-version", default="15.4.x-scala2.12")
    p.add_argument("--notebook-path", default=None, help="Override workspace path for the seed notebook.")
    p.add_argument("--skip-cluster", action="store_true",
                   help="Skip classic all-purpose cluster creation (default: attempt it).")
    p.add_argument("--skip-job", action="store_true")
    p.add_argument("--serverless", action="store_true", default=True,
                   help="Run the job on serverless compute (default: on). "
                        "Use --no-serverless on workspaces that support classic job clusters.")
    p.add_argument("--no-serverless", dest="serverless", action="store_false")
    return p.parse_args()


def create_cluster(client: DatabricksClient, args: argparse.Namespace) -> str:
    payload = {
        "cluster_name": args.cluster_name,
        "spark_version": args.spark_version,
        "node_type_id": args.node_type_id,
        "num_workers": 0,
        "autotermination_minutes": 30,
        "spark_conf": SINGLE_NODE_CONF,
        "custom_tags": SINGLE_NODE_TAGS,
    }
    resp = client.post("/api/2.0/clusters/create", json=payload)
    cluster_id = resp.get("cluster_id", "?")
    print(f"[cluster] created + starting: name={args.cluster_name} id={cluster_id}")
    return cluster_id


def resolve_notebook_path(client: DatabricksClient, override: str | None) -> str:
    if override:
        return override
    me = client.get("/api/2.0/preview/scim/v2/Me")
    username = me.get("userName") or me.get("emails", [{}])[0].get("value")
    if not username:
        raise RuntimeError("Could not resolve current user from SCIM /Me response.")
    return f"/Users/{username}/poc-seed-notebook"


def upload_notebook(client: DatabricksClient, path: str) -> None:
    content_b64 = base64.b64encode(NOTEBOOK_SOURCE.encode("utf-8")).decode("ascii")
    client.post(
        "/api/2.0/workspace/import",
        json={
            "path": path,
            "language": "PYTHON",
            "format": "SOURCE",
            "overwrite": True,
            "content": content_b64,
        },
    )
    print(f"[notebook] uploaded: {path}")


def create_job(client: DatabricksClient, args: argparse.Namespace, notebook_path: str) -> int:
    task: dict = {
        "task_key": "hello",
        "notebook_task": {"notebook_path": notebook_path},
    }
    if not args.serverless:
        task["new_cluster"] = {
            "spark_version": args.spark_version,
            "node_type_id": args.node_type_id,
            "num_workers": 0,
            "spark_conf": SINGLE_NODE_CONF,
            "custom_tags": SINGLE_NODE_TAGS,
        }
    payload = {"name": args.job_name, "tasks": [task]}
    resp = client.post("/api/2.1/jobs/create", json=payload)
    job_id = resp["job_id"]
    print(f"[job] created: name={args.job_name} id={job_id}")
    return job_id


def trigger_run(client: DatabricksClient, job_id: int) -> int:
    resp = client.post("/api/2.1/jobs/run-now", json={"job_id": job_id})
    run_id = resp.get("run_id", "?")
    print(f"[run] triggered: job_id={job_id} run_id={run_id}")
    return run_id


def _report(label: str, exc: HTTPError) -> None:
    status = exc.response.status_code if exc.response is not None else "?"
    url = exc.response.url if exc.response is not None else "?"
    body = exc.response.text if exc.response is not None else ""
    print(f"[{label}] FAILED HTTP {status} {url}\n  {body}", file=sys.stderr)


def main() -> int:
    load_dotenv()
    args = parse_args()

    try:
        client = DatabricksClient()
    except RuntimeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2

    if not args.skip_cluster:
        try:
            create_cluster(client, args)
        except HTTPError as e:
            _report("cluster", e)

    if not args.skip_job:
        try:
            notebook_path = resolve_notebook_path(client, args.notebook_path)
            upload_notebook(client, notebook_path)
            job_id = create_job(client, args, notebook_path)
            trigger_run(client, job_id)
        except HTTPError as e:
            _report("job", e)

    print("\nDone. Run `python main.py` to see the new cluster/job.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
