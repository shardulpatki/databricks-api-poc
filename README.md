# databricks-api-poc

A small Python CLI that pulls observability signals from a Databricks
workspace via REST:

1. **Job runs** — recent runs, with result state and duration
2. **Clusters** — current state, node type, sizing, uptime
3. **Serving endpoints** — Model Serving endpoints and their readiness
4. **Endpoint traffic metrics** — per-endpoint request / 4xx / 5xx counters,
   parsed from the Prometheus `/metrics` route
5. **Endpoint events** — recent config-change and deployment events

Latency, CPU, and memory are deliberately not shown: the public `/metrics`
route doesn't emit them. Those signals plus per-request traces live in the
`system.serving.*` inference tables (SQL only) and are out of scope here.

The code is intentionally small and commented so it can double as a teaching
walkthrough for how Databricks PAT auth works.

## Setup

Requires Python 3.9+.

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env and set DATABRICKS_HOST and DATABRICKS_TOKEN
```

Get a Personal Access Token from **User Settings → Developer → Access tokens**
in your Databricks workspace.

## Usage

```bash
# Everything, pretty tables:
python main.py

# Just job runs in the last 6 hours:
python main.py --section jobs --hours 6

# Machine-readable:
python main.py --output json > snapshot.json
```

### Flags

| Flag        | Values                                | Default |
|-------------|---------------------------------------|---------|
| `--output`  | `table`, `json`                       | `table` |
| `--hours`   | integer (job-runs lookback)           | `24`    |
| `--section` | `jobs`, `clusters`, `endpoints`, `endpoint-metrics`, `endpoint-events`, `all` | `all`   |

## Example output

```
────────────────────────────── Job Runs ──────────────────────────────
┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Job / Run         ┃ Run ID  ┃ Result  ┃ Started          ┃ Duration ┃
┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ nightly_etl       │ 81234   │ SUCCESS │ 2026-04-17 03:00 │ 0h 22m 4s│
│ feature_backfill  │ 81240   │ FAILED  │ 2026-04-17 05:12 │ 0h 1m 8s │
└───────────────────┴─────────┴─────────┴──────────────────┴──────────┘
```

## Structure

```
main.py              # CLI entrypoint
client.py            # DatabricksClient (PAT auth via requests.Session)
collectors/
    jobs.py          # /api/2.1/jobs/runs/list
    clusters.py      # /api/2.0/clusters/list
    endpoints.py     # /api/2.0/serving-endpoints (+ /metrics, /events)
output.py            # rich table + JSON renderers
```

See `DEMO_SCRIPT.md` for a 10-minute walkthrough aimed at a non-technical audience.
