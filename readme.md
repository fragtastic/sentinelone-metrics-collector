# SentinelOne Metrics Collector + API

This service now does two things at the same time:

1. Continuously collects SentinelOne count metrics into DuckDB
2. Serves HTTP API endpoints for reading metrics from the DB

Collection runs in a dedicated background thread and is not blocked by API requests.

## Setup

Copy `example_queries.json` to `queries.json` and configure queries.

Copy `example.env` to `.env` and set values:
- `SENTINELONE_URL` (subdomain only, e.g. `your-console`)
- `SENTINELONE_AUTH_TOKEN`

Optional env vars:
- `METRICS_DB_PATH` (default: `metrics.duckdb`)
- `QUERIES_PATH` (default: `queries.json`)
- `COLLECT_INTERVAL_SECONDS` (default: `60`)
- `MAX_QUERY_WORKERS` (default: `8`)
- `PROCESS_NICE_ADJUST` (default: `10`; higher values lower CPU scheduling priority on Linux)
- `API_MAX_RANGE_DAYS` (default: `31`; hard cap for `/metrics/range`, `/metrics/daily-max`, `/metrics/hourly-max`)
- `API_MAX_RESULT_ROWS` (default: `10000`; hard row cap for `/metrics/range` and `limit` max)

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run service:

```bash
python collect_metrics.py --host 0.0.0.0 --port 8080 --initial-run
```

## API Endpoints

### Health
- `GET /healthz`

### Latest metrics
- `GET /metrics/latest`
- Query params:
  - `limit` (default 200, max 2000)
  - `query` (optional exact query filter)

### Time range metrics (hourly aggregates)
- `GET /metrics/range?from=<iso>&to=<iso>`
- Returns hourly aggregates per query (not raw minute-level rows):
  - `min_result`, `avg_result`, `max_result`, `sample_count`
- Optional query params:
  - `query` (exact query filter)
  - `limit` (max `API_MAX_RESULT_ROWS`, default same)
- Guardrails:
  - rejects ranges larger than `API_MAX_RANGE_DAYS`

Example:

```bash
curl "http://localhost:8080/metrics/range?from=2026-03-17T00:00:00Z&to=2026-03-18T00:00:00Z"
```

### Daily max per query
- `GET /metrics/daily-max`
- Query params:
  - `days` (default 30, max `API_MAX_RANGE_DAYS`)

### Hourly max per query (last X days)
- `GET /metrics/hourly-max`
- Query params:
  - `days` (default 7, max `API_MAX_RANGE_DAYS`)
  - `query` (optional exact query filter)

## Docker deployment

Build image:

```bash
docker build -t s1-metrics-collector .
```

Run container:

```bash
docker run -d \
  --name s1-metrics-collector \
  -p 8080:8080 \
  --env-file .env \
  -v $(pwd)/queries.json:/app/queries.json:ro \
  -v $(pwd)/data:/app/data \
  -e METRICS_DB_PATH=/app/data/metrics.duckdb \
  s1-metrics-collector
```

The container includes a Docker `HEALTHCHECK` that calls:

- `GET http://127.0.0.1:8080/healthz`

You can inspect health with:

```bash
docker ps
# or

docker inspect --format='{{json .State.Health}}' s1-metrics-collector
```

## Notes on threading/concurrency

- Collector loop runs in its own thread.
- API uses Flask threaded mode (`threaded=True`) so requests are handled concurrently.
- Request threads are per-request and exit after response (no persistent endpoint threads to TTL-kill).
- Collector uses a dedicated DuckDB writer connection.
- API handlers open a separate DuckDB connection per request using the same DB config.
- Query fetching from SentinelOne is parallelized with a thread pool each collection cycle.

## Low-power tuning

- Lower CPU priority at startup:

```bash
python collect_metrics.py --nice-adjust 10
```

or via env:

```bash
PROCESS_NICE_ADJUST=10 python collect_metrics.py
```
