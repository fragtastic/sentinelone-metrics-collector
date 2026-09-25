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
- `API_TOKEN` (optional; when set, metrics routes require `Authorization: Bearer <token>`. `/healthz` stays unauthenticated for Docker health checks.)
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

The process uses Flask’s built-in server (`threaded=True`), which is sufficient for a single low-traffic EC2 instance. Use one process per DuckDB file.

## API authentication

When `API_TOKEN` is set, send the same header browsers use for bearer tokens:

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

- **Data retention:** configurable env-driven purge or archive (all history kept today).
- **SSO web UI** with client-side charts (separate frontend).
- **Production WSGI** (e.g. gunicorn) if traffic or hardening requirements grow.

## Low-power tuning

```bash
python collect_metrics.py --nice-adjust 10
```

or via env:

```bash
PROCESS_NICE_ADJUST=10 python collect_metrics.py
```
