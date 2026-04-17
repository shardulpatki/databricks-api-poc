# Demo Script — Databricks Observability POC

Target time: **under 10 minutes**. Audience: mixed technical/non-technical.

---

## 1. Problem statement (~1 minute)

"Anyone checking the health of a Databricks workspace today has to bounce
between three different UIs — jobs, clusters, and model serving. That's fine
for one engineer on one screen, but it doesn't scale when you want a daily
health digest, an on-call dashboard, or an alert when something fails
overnight."

**The goal of this POC:** prove that a tiny Python script, using only the
public REST API, can centralize those three views in one command. Once the
data is in a script, everything else — scheduling, alerting, dashboards —
becomes easy.

---

## 2. Setup & run (~1 minute)

```bash
pip install -r requirements.txt
cp .env.example .env   # paste HOST + PAT
python main.py
```

Talk track: *"Two environment variables. One command. That's the whole
install story for the demo."*

---

## 3. Code walkthrough (~3 minutes)

### 3a. The auth pattern — open `client.py`

Point at the constructor and say:

> "Databricks uses **bearer token auth** — same pattern as GitHub, Stripe,
> most modern APIs. We read the token from an environment variable, then
> attach it to a `requests.Session` **once**. Every call we make after that
> automatically carries the auth header. No per-call plumbing."

Highlight the `session.headers.update({"Authorization": f"Bearer {token}"})`
line. That's the whole security story.

### 3b. One call end-to-end

Trace a single job-runs call through the files:

1. `main.py` parses `--section jobs` and calls `jobs_collector.fetch(client, hours=24)`.
2. `collectors/jobs.py` converts "last 24 hours" to epoch **milliseconds**
   (Databricks quirk — call this out), then calls
   `client.get("/api/2.1/jobs/runs/list", params={...})`.
3. `client.py` issues the HTTP GET with the pre-attached bearer header and
   returns parsed JSON.
4. The collector pulls `run_name`, `result_state`, and duration out of the
   response and hands a list of dicts back to `main.py`.
5. `output.py` renders it as a colored table with `rich`.

> "That's five files, about 200 lines of code, and it's the entire integration."

---

## 4. Outputs (~3 minutes)

Run each section one at a time so the audience sees a clean, single-purpose table:

```bash
python main.py --section jobs
python main.py --section clusters
python main.py --section endpoints
python main.py --section endpoint-events
```

`endpoint-events` goes a level deeper on Model Serving: it lists recent
config-change and deployment events per endpoint.

Then the full view:

```bash
python main.py
```

Finally, show that the same data is available machine-readable:

```bash
python main.py --output json | jq '.jobs[0]'
```

Talk track: *"Same script, two audiences — humans get the colored table,
pipelines get JSON."*

---

## 5. Limitations + next steps (~1 minute)

**Known limits (be upfront):**
- No historical storage — every run is a fresh snapshot.
- No alerting — just prints to the terminal.
- Coarse request counters for serving endpoints (total / 4xx / 5xx) are
  included via the Prometheus `/metrics` route, but latency, CPU, memory,
  and per-request traces live in Databricks `system.serving.*` inference
  tables.

**Natural next steps:**
- **Schedule it.** Wrap `python main.py --output json` in a nightly cron or
  Airflow task that writes to S3 / a DB.
- **Alert on it.** Add a check: if any job `result_state == FAILED`, post
  to Slack.
- **Dashboard it.** Point Grafana or a Databricks SQL dashboard at the
  stored JSON snapshots.

---

## Backup tip: if the live API is slow

The demo relies on live REST calls and a corporate-network round trip to
Databricks can occasionally stall for 10–20 seconds. Two mitigations:

1. **Pre-run it.** Capture `python main.py --output json > sample_output.json`
   an hour before the demo. If live calls stall, read from the file and
   narrate as if it were live — the tables look identical.
2. **Future flag.** A `DATABRICKS_MOCK=1` switch that reads fixtures from
   disk instead of calling the API would make this a first-class feature;
   it's a clean follow-up if the POC moves forward.
