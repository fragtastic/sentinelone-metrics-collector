# SentinelOne Metrics Collector + API

This service:

1. Continuously collects SentinelOne agent count metrics into DuckDB
2. Serves HTTP API endpoints for reading metrics (Excel Power Query, scripts, future web UI)

Collection runs in a dedicated background thread and is not blocked by API requests.

## Setup

Copy `example_queries.json` to `queries.json` and configure queries.

Copy `example.env` to `.env` and set values:

- `SENTINELONE_URL` (subdomain only, e.g. `your-console`)
- `SENTINELONE_AUTH_TOKEN`

Production query lists live in deployment config (mount `queries.json` on the host); `example_queries.json` in this repo is only a template.

Optional env vars:

- `METRICS_DB_PATH` (default: `metrics.duckdb`)
- `QUERIES_PATH` (default: `queries.json`)
- `COLLECT_INTERVAL_SECONDS` (default: `60`)
- `MAX_QUERY_WORKERS` (default: `8`)
- `PROCESS_NICE_ADJUST` (default: `10`; higher values lower CPU scheduling priority on Linux)
- `API_MAX_RANGE_DAYS` (default: `31`; hard cap for `/metrics/range`, `/metrics/daily-max`, `/metrics/hourly-max`)
- `API_MAX_RESULT_ROWS` (default: `10000`; hard row cap for `/metrics/range` and `limit` max)
- `API_TOKEN` (optional; static shared secret for Excel/scripts; metrics routes accept `Authorization: Bearer <token>`. `/healthz` stays unauthenticated.)
- `OIDC_ISSUER` (optional; e.g. `https://your-org.okta.com/oauth2/default` — enables JWT access-token verification on `/metrics/*`)
- `OIDC_AUDIENCE` (optional; comma-separated allowed `aud` values; recommended when using OIDC)
- `CORS_ALLOWED_ORIGINS` (optional; comma-separated browser origins for a split-origin UI, e.g. `https://metrics-ui.example.com`)
- `STORE_FAILED_AS` (default: `null`; `null` = insert row with `Result` NULL on SentinelOne failure, `omit` = skip row so Excel/API show a gap)

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```bash
pip install -r requirements-dev.txt
pytest
```

Run service:

```bash
python collect_metrics.py --host 0.0.0.0 --port 8080 --initial-run
```

The Docker image runs **gunicorn** (one worker, threaded). For bare-metal dev you can still use `python collect_metrics.py` (Flask dev server). Use one process per DuckDB file.

## Web UI

The repository includes a React SPA under `frontend/` (charts in the browser, no server-side rendering).

**Local development**

```bash
cd frontend && npm ci && cp .env.example .env.local
# Start collector on :8080, then:
npm run dev
```

See [frontend/README.md](frontend/README.md) for proxy and `VITE_DEV_API_TOKEN` when the collector uses `API_TOKEN`.

**Production (Docker)**

The Docker image can serve the UI from `/app/static` (same origin as the API). You may also deploy the built `frontend/dist` to a separate host; set `VITE_API_BASE_URL` and configure **`CORS_ALLOWED_ORIGINS`** on the collector.

When **`API_TOKEN`** and/or **`OIDC_ISSUER`** is set, `/metrics/*` requires `Authorization: Bearer`. Static routes stay open (Option A) when UI is co-located; a split-origin UI relies on OIDC tokens instead of exposing `API_TOKEN` in the browser.

**SSO (deferred, Phase 7b):** browser OIDC login (Okta or any OIDC issuer) → access token presented to the API → JWT verified server-side. See `IMPLEMENTATION_PLAN.md` and `frontend/README.md`.

## API authentication

When `API_TOKEN` and/or OIDC verification is enabled, send a bearer token on `/metrics/*`:

```http
Authorization: Bearer YOUR_API_TOKEN
```

Example with curl (via SSH tunnel to EC2):

```bash
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
  "http://127.0.0.1:8080/metrics/daily-max?days=30"
```

Excel Power Query can pass the header on `Web.Contents`:

```powerquery
Web.Contents(
    "http://127.0.0.1:8080/metrics/daily-max?days=30",
    [Headers = [#"Authorization" = "Bearer YOUR_API_TOKEN"]]
)
```

If `API_TOKEN` is unset, metrics routes are open (rely on network isolation, e.g. SSH tunnel only).

## API Endpoints

### Health

- `GET /healthz`
- Returns JSON with `ok`, `db_ok`, `collector_thread_alive`, `last_collect_at`, `last_success_at`, `last_error`
- HTTP `503` when the DB is unreachable or the collector thread is not running (Docker health check uses this)

### Latest metrics

- `GET /metrics/latest`
- Query params:
  - `limit` (default 200, max 2000; invalid values return `400`)
  - `query` (optional exact query filter)

### Time range metrics (hourly aggregates)

- `GET /metrics/range?from=<date-or-iso>&to=<date-or-iso>`
- `from`/`to` can be either:
  - `YYYY-MM-DD` (day boundaries at 00:00:00 UTC)
  - full ISO timestamp (e.g. `2026-03-10T00:00:00Z`)
- Returns hourly aggregates per query (not raw minute-level rows):
  - `min_result`, `avg_result`, `max_result`, `sample_count`
- Optional query params:
  - `query` (exact query filter)
  - `limit` (max `API_MAX_RESULT_ROWS`, default same)
- Guardrails:
  - rejects ranges larger than `API_MAX_RANGE_DAYS`

Example:

```bash
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
  "http://localhost:8080/metrics/range?from=2026-03-17&to=2026-03-18"
```

(Omit the header if `API_TOKEN` is not configured.)

### Daily max per query

- `GET /metrics/daily-max`
- Query params:
  - `days` (default 30, max `API_MAX_RANGE_DAYS`)

### Hourly max per query (last X days)

- `GET /metrics/hourly-max`
- Query params:
  - `days` (default 7, max `API_MAX_RANGE_DAYS`)
  - `query` (optional exact query filter)

## Local testing with Docker Compose

From the repo root:

```bash
cp example.env .env
# Edit .env: SENTINELONE_URL, SENTINELONE_AUTH_TOKEN (optional API_TOKEN for /metrics/*)
docker compose up --build
```

- **UI + API:** http://localhost:8080/
- **Health:** http://localhost:8080/healthz
- **Data:** DuckDB persisted in `./data/` (gitignored)
- **Queries:** `example_queries.json` is mounted read-only; for production-like config, copy to `queries.json`, change the compose volume to `./queries.json:/app/config/queries.json:ro`, and restart.

Stop with `docker compose down`. Rebuild after code changes: `docker compose up --build`.

## Docker deployment (EC2)

Build image:

```bash
docker build -t s1-metrics-collector .
```

Run container:

```bash
docker run -d \
  --name s1-metrics-collector \
  --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  --env-file .env \
  -v /path/on/host/queries.json:/app/queries.json:ro \
  -v /path/on/host/data:/app/data \
  -e METRICS_DB_PATH=/app/data/metrics.duckdb \
  s1-metrics-collector
```

Binding `8080` to `127.0.0.1` on the host keeps the API off the public interface; use SSH port forwarding for Excel (`ssh -L 8080:127.0.0.1:8080 ec2-user@host`).

The container includes a Docker `HEALTHCHECK` that calls `GET http://127.0.0.1:8080/healthz` and expects HTTP 200.

### Operations runbook

| Task | Notes |
|------|--------|
| **Upgrade** | `docker pull` / rebuild image, `docker stop`, `docker rm`, re-run `docker run` with same volumes and env |
| **Queries** | Edit host `queries.json`; collector reloads each interval without restart |
| **Backup DB** | Copy `/app/data/metrics.duckdb` from the volume, or use `fetch_db_scp.sh` (adjust host alias/path) |
| **Logs** | `docker logs -f s1-metrics-collector` |
| **Scale** | Do not run two containers writing the same DuckDB file |

## Notes on threading/concurrency

- Collector loop runs in its own thread.
- API uses Flask threaded mode so requests are handled concurrently.
- Collector uses a dedicated DuckDB writer connection; API opens a separate connection per request.
- Query fetching from SentinelOne is parallelized with a thread pool each collection cycle.
- DuckDB indexes on `Timestamp` and `(Query, Timestamp)` support range and aggregate queries.
- Run a single collector instance per DuckDB file.

## Planned / deferred

See `IMPLEMENTATION_PLAN.md` for tracking. Notable deferred items:

- **Data retention / compaction:** env-driven roll-up and purge (all raw history kept today). SentinelOne usage is **max count per day**; their metering uses a **5-minute** cadence—future compaction should retain daily (and optionally 5-min/hourly) **max** per query, not necessarily every minute sample forever. See `IMPLEMENTATION_PLAN.md`.
- **SSO gate** for the web UI (Phase 7b in `IMPLEMENTATION_PLAN.md`; UI MVP in `frontend/`).
- **Production WSGI** (e.g. gunicorn) if traffic or hardening requirements grow.

## Low-power tuning

```bash
python collect_metrics.py --nice-adjust 10
```

or via env:

```bash
PROCESS_NICE_ADJUST=10 python collect_metrics.py
```
