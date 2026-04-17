# How data retrieval works

This is a file-by-file walkthrough of how the POC authenticates to Databricks and pulls observability data. It's grounded in the actual code — every claim below points at a file and line range you can open.

## One-paragraph overview

The POC is a CLI (`main.py`) that authenticates to a Databricks workspace using a Personal Access Token (PAT) and issues REST GETs against four endpoints: jobs runs, clusters, serving endpoints, and serving-endpoint events. Auth is centralized in a single `DatabricksClient` class (`client.py`). Each collector module owns exactly one endpoint family and normalizes the JSON response into display rows. `main.py` is just glue: parse args, build one client, dispatch to collectors, render.

## The flow

```
.env ─► main.py ─► DatabricksClient (client.py) ─► Databricks REST API
                         │
                         ├─► collectors/jobs.py       (/api/2.1/jobs/runs/list)
                         ├─► collectors/clusters.py   (/api/2.0/clusters/list)
                         └─► collectors/endpoints.py  (/api/2.0/serving-endpoints
                                                        [/{name}/events])
                                                       │
                                                       ▼
                                           output.py (table/JSON render)
```

One token, one session, many GETs.

## Auth — `client.py`

- Secrets come from a local `.env` file. `main.py:93` calls `load_dotenv()` to populate the process env, then `client.py:22-23` reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` via `os.getenv`.
- `DatabricksClient.__init__` (`client.py:20-49`):
  - Validates both env vars are set; raises `RuntimeError` with a friendly "set them in .env" message if not (`client.py:25-30`).
  - Normalizes the host — adds `https://` if missing, strips trailing `/` (`client.py:33-36`).
  - Creates a `requests.Session` and attaches the token **once** as a default header (`client.py:42-46`):
    ```python
    self.session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    ```
    This is the only place the token is touched. Every subsequent `session.get(...)` / `session.post(...)` inherits the header automatically — no per-call plumbing, no token variable passed around.
- Optional call recording: if `record=True` (triggered by `--raw-output`), `client.py:51-61` appends `{method, path, params, status, timestamp, body}` to `self.calls`. That list gets dumped to disk by `main.py:119-122` and is the evidence trail for "which endpoints did we actually hit?".

## The HTTP primitives — `client.py`

Three thin wrappers; all share the authenticated session.

| Method | Lines | Returns | Used by |
|---|---|---|---|
| `get(path, params)` | `client.py:63-74` | parsed JSON dict | all collectors |
| `get_text(path)` | `client.py:76-86` | raw text body | (unused; reserved for Prometheus/OpenMetrics endpoints like `/api/2.0/serving-endpoints/{name}/metrics`) |
| `post(path, json)` | `client.py:88-95` | parsed JSON dict | (unused; kept for parity) |

All three call `raise_for_status()`, so non-2xx responses surface as `requests.HTTPError` and bubble up to the caller.

## Per-endpoint retrieval — `collectors/`

Each collector is a module with a single `fetch(client, ...)` function. They depend on the client but know nothing about auth, the host, or argument parsing.

### `collectors/jobs.py` → `GET /api/2.1/jobs/runs/list`

`fetch(client, hours)` at `jobs.py:26-62`:

1. Computes `start_time_from` as epoch **milliseconds** (`jobs.py:33-34`). This is a known Databricks gotcha — the API wants ms, not seconds — and is called out in a comment.
2. Calls `client.get("/api/2.1/jobs/runs/list", params=...)` in a loop, following cursor-based pagination via `has_more` + `next_page_token` (`jobs.py:40-60`).
3. Normalizes each run into `{run_name, run_id, result_state, start_time, duration}`.
4. Helpers `_fmt_duration` and `_fmt_ts` turn ms into human strings.

### `collectors/clusters.py` → `GET /api/2.0/clusters/list`

`fetch(client)` at `clusters.py:24-52`:

- Single GET, no pagination.
- Walks `payload["clusters"]` and formats sizing as either `autoscale N-M` or `fixed N`.
- For `RUNNING` clusters, computes uptime from `start_time` (epoch ms) via `_fmt_uptime` — so reviewers can spot clusters left on for days.

### `collectors/endpoints.py` → `GET /api/2.0/serving-endpoints` (+ `/{name}/events`)

Two functions:

- `fetch(client)` at `endpoints.py:31-44` — one GET, one row per endpoint: `{name, state, creator, created}`. State is pulled from `state.ready` falling back to `state.config_update`.
- `fetch_events(client, limit=5)` at `endpoints.py:47-65` — lists endpoint names via `_list_endpoint_names`, then fans out to `/{name}/events` per endpoint. Per-endpoint `HTTPError`s are swallowed (`endpoints.py:53-54`) so one broken endpoint doesn't take out the whole section. The API returns newest-first; `limit=5` caps per-endpoint so the output stays scannable.

## Orchestration — `main.py`

- `parse_args` (`main.py:55-69`) defines the CLI surface: `--output`, `--hours`, `--section`, `--raw-output`.
- `main()` (`main.py:92-131`):
  1. `load_dotenv()` → construct `DatabricksClient` exactly once (`main.py:93-97`). If env vars are missing, exit code `2` *before* any HTTP call (`main.py:98-100`).
  2. Resolve the section list (`all` expands to the four known sections).
  3. For each section, dispatch through `collect()` (`main.py:72-81`) which routes to the matching collector.
  4. Catch `HTTPError` per section (`main.py:111-115`) — print `HTTP <status> <url>` to stderr, set that section's rows to empty, and keep going. The overall run still exits 0.
  5. If `--raw-output` is set, dump recorded calls per section to a JSON file (`main.py:119-122`).
  6. Render via `render_json` or `render_table` (`output.py`).

## Error + observability summary

| Failure | Where caught | User sees |
|---|---|---|
| Missing `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | `client.py:25-30` → `main.py:98-100` | `Configuration error: ...`, exit code `2` |
| Non-2xx from any endpoint | `requests.raise_for_status()` → `main.py:111-115` | `API call failed for '<section>': HTTP <code> <url>`, empty rows, other sections still render |
| Per-endpoint failure in `fetch_events` | `endpoints.py:53-54` | that endpoint's events skipped silently |

## Quick reference: where is X?

- **Where is the token attached?** `client.py:42-46`, once, on the session.
- **What calls the jobs API?** `collectors/jobs.py:41`.
- **What calls the clusters API?** `collectors/clusters.py:31`.
- **What calls the serving-endpoints API?** `collectors/endpoints.py:27, 33, 52`.
- **Where does the HTTP error handling live?** `main.py:111-115` (per-section) and `client.py:71, 84, 92` (via `raise_for_status`).
- **How do I see the raw requests?** `python main.py --raw-output raw.json` — see `main.py:116-122` and `client.py:51-61`.
