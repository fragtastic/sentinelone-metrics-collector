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

### Time range metrics
- `GET /metrics/range?from=<iso>&to=<iso>`
- Optional query params:
  - `query` (exact query filter)

Example:

```bash
curl "http://localhost:8080/metrics/range?from=2026-03-17T00:00:00Z&to=2026-03-18T00:00:00Z"
```

### Daily max per query
- `GET /metrics/daily-max`
- Query params:
  - `days` (default 30, max 3650)

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
- Collector uses a dedicated DuckDB writer connection.
- API handlers open a separate read-only DuckDB connection per request.
- Query fetching from SentinelOne is parallelized with a thread pool each collection cycle.
