"""Databricks observability POC — CLI entrypoint.

Usage:
    python main.py                       # all sections, table output
    python main.py --section jobs        # just job runs
    python main.py --output json         # machine-readable
    python main.py --hours 6             # narrow the jobs lookback window
"""

from __future__ import annotations

import argparse
import json
import sys

from dotenv import load_dotenv
from requests import HTTPError

from client import DatabricksClient
from collectors import clusters as clusters_collector
from collectors import endpoints as endpoints_collector
from collectors import jobs as jobs_collector
from collectors import permissions as permissions_collector
from output import render_json, render_table


JOB_COLUMNS = [
    ("run_name", "Job / Run"),
    ("run_id", "Run ID"),
    ("result_state", "Result"),
    ("start_time", "Started"),
    ("duration", "Duration"),
]
CLUSTER_COLUMNS = [
    ("cluster_name", "Cluster"),
    ("state", "State"),
    ("node_type", "Node Type"),
    ("sizing", "Sizing"),
    ("uptime", "Uptime"),
]
ENDPOINT_COLUMNS = [
    ("name", "Endpoint"),
    ("state", "State"),
    ("creator", "Creator"),
    ("created", "Created"),
]
ENDPOINT_EVENTS_COLUMNS = [
    ("endpoint", "Endpoint"),
    ("event_type", "Event"),
    ("status", "Status"),
    ("message", "Message"),
    ("timestamp", "When"),
]
PERMISSIONS_COLUMNS = [
    ("object_type", "Object Type"),
    ("object_name", "Object"),
    ("principal_type", "Principal Type"),
    ("principal", "Principal"),
    ("permission_level", "Permission"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull observability metrics from Databricks via REST.")
    p.add_argument("--output", choices=["table", "json"], default="table")
    p.add_argument("--hours", type=int, default=24, help="Lookback window for job runs (default 24).")
    p.add_argument(
        "--section",
        choices=["jobs", "clusters", "endpoints", "endpoint-events", "permissions", "all"],
        default="all",
    )
    p.add_argument(
        "--raw-output",
        metavar="PATH",
        help="Write raw API responses + request metadata to PATH as JSON.",
    )
    return p.parse_args()


def collect(section: str, client: DatabricksClient, hours: int) -> list[dict]:
    if section == "jobs":
        return jobs_collector.fetch(client, hours=hours)
    if section == "clusters":
        return clusters_collector.fetch(client)
    if section == "endpoints":
        return endpoints_collector.fetch(client)
    if section == "endpoint-events":
        return endpoints_collector.fetch_events(client)
    if section == "permissions":
        return permissions_collector.fetch(client)
    raise ValueError(f"unknown section: {section}")


SECTION_META = {
    "jobs": ("Job Runs", JOB_COLUMNS),
    "clusters": ("Clusters", CLUSTER_COLUMNS),
    "endpoints": ("Serving Endpoints", ENDPOINT_COLUMNS),
    "endpoint-events": ("Serving Endpoint Events", ENDPOINT_EVENTS_COLUMNS),
    "permissions": ("Permissions", PERMISSIONS_COLUMNS),
}


def main() -> int:
    load_dotenv()
    args = parse_args()

    try:
        client = DatabricksClient(record=bool(args.raw_output))
    except RuntimeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2

    all_sections = ["jobs", "clusters", "endpoints", "endpoint-events", "permissions"]
    sections = all_sections if args.section == "all" else [args.section]

    results: dict[str, list[dict]] = {}
    raw: dict[str, list[dict]] = {}
    for section in sections:
        before = len(client.calls)
        try:
            results[section] = collect(section, client, args.hours)
        except HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            url = e.response.url if e.response is not None else "?"
            print(f"API call failed for '{section}': HTTP {status} {url}", file=sys.stderr)
            results[section] = []
        if args.raw_output:
            raw[section] = client.calls[before:]

    if args.raw_output:
        with open(args.raw_output, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, default=str)
        print(f"Wrote raw output to {args.raw_output}", file=sys.stderr)

    if args.output == "json":
        render_json(results)
        return 0

    for section in sections:
        title, cols = SECTION_META[section]
        render_table(title, results[section], cols)
    return 0


if __name__ == "__main__":
    sys.exit(main())
