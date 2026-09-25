import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

import duckdb
import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_from_directory

from metrics_auth import bearer_authorized, metrics_auth_enabled
from s1_upstream import fetch_agent_count, summarize_query_failures

load_dotenv()

DB_PATH = os.getenv("METRICS_DB_PATH", "metrics.duckdb")
QUERIES_PATH = os.getenv("QUERIES_PATH", "queries.json")
DEFAULT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))
MAX_QUERY_WORKERS = int(os.getenv("MAX_QUERY_WORKERS", "2"))
S1_QUERY_STAGGER_SECONDS = float(os.getenv("S1_QUERY_STAGGER_SECONDS", "0.5"))
DEFAULT_NICE_ADJUST = int(os.getenv("PROCESS_NICE_ADJUST", "10"))
MAX_RANGE_DAYS = int(os.getenv("API_MAX_RANGE_DAYS", "31"))
MAX_RANGE_HOURS = MAX_RANGE_DAYS * 24
MAX_RESULT_ROWS = int(os.getenv("API_MAX_RESULT_ROWS", "10000"))
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
_cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
CORS_ALLOWED_ORIGINS = [
    origin.strip() for origin in _cors_origins_raw.split(",") if origin.strip()
]

# TODO: Retention/compaction (see IMPLEMENTATION_PLAN.md): S1 usage = daily max per query;
# roll up raw samples to daily (and optionally 5-min/hourly) max before purge.


def _normalize_store_failed_as(raw: str) -> str:
    value = (raw or "null").strip().lower()
    if value not in ("null", "omit"):
        print(f"Invalid STORE_FAILED_AS={raw!r}; using 'null'.")
        return "null"
    return value


STORE_FAILED_AS = _normalize_store_failed_as(os.getenv("STORE_FAILED_AS", "null"))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS s1_metrics (
            Timestamp TIMESTAMPTZ,
            Query VARCHAR,
            Result INTEGER NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_s1_metrics_timestamp
        ON s1_metrics (Timestamp);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_s1_metrics_query_timestamp
        ON s1_metrics (Query, Timestamp);
        """
    )


class MetricsCollector:
    def __init__(
        self,
        db_path: str,
        queries_path: str,
        interval_seconds: int,
        max_workers: int,
        store_failed_as: str = STORE_FAILED_AS,
    ) -> None:
        self.db_path = db_path
        self.queries_path = queries_path
        self.interval_seconds = interval_seconds
        self.max_workers = max_workers
        self.store_failed_as = store_failed_as

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._queries: List[str] = []
        self._status_lock = threading.Lock()
        self.last_collect_at: Optional[str] = None
        self.last_success_at: Optional[str] = None
        self.last_error: Optional[str] = None

        self._con = duckdb.connect(self.db_path)
        ensure_schema(self._con)

    def start(self, initial_run: bool = False) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._queries = self.load_queries()
        print(f"Loaded queries: {self._queries}")
        self._thread = threading.Thread(
            target=self._run_loop,
            kwargs={"initial_run": initial_run},
            name="metrics-collector",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._con.close()

    def collector_thread_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _set_status(
        self,
        *,
        last_collect_at: Optional[str] = None,
        last_success_at: Optional[str] = None,
        last_error: Optional[str] = None,
        clear_error: bool = False,
    ) -> None:
        with self._status_lock:
            if last_collect_at is not None:
                self.last_collect_at = last_collect_at
            if last_success_at is not None:
                self.last_success_at = last_success_at
            if clear_error:
                self.last_error = None
            elif last_error is not None:
                self.last_error = last_error

    def load_queries(self) -> List[str]:
        with open(self.queries_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("queries.json must contain a JSON array of query strings")
            return [str(x) for x in data]

    def _run_loop(self, initial_run: bool = False) -> None:
        if initial_run:
            try:
                self.collect_once()
            except Exception as e:
                print(f"Collector initial run error: {e}")

        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self.interval_seconds):
                break

            try:
                updated_queries = self.load_queries()
                if updated_queries != self._queries:
                    self._queries = updated_queries
                    print(f"Reloaded queries: {self._queries}")
            except Exception as e:
                print(f"Failed reloading queries: {e}")

            try:
                self.collect_once()
            except Exception as e:
                print(f"Collector loop error: {e}")
                self._set_status(last_error=str(e))

    def collect_once(self) -> None:
        current_time = datetime.now(timezone.utc)
        collect_iso = current_time.isoformat()
        print(f"Collecting metrics @ {collect_iso}")
        self._set_status(last_collect_at=collect_iso)

        if not self._queries:
            print("No queries configured; skipping collection run.")
            self._set_status(last_error="no queries configured")
            return

        rows: List[tuple[str, str, Optional[int]]] = []
        failure_details: List[str] = []
        worker_count = min(len(self._queries), self.max_workers)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for index, query in enumerate(self._queries):
                if index > 0 and S1_QUERY_STAGGER_SECONDS > 0:
                    time.sleep(S1_QUERY_STAGGER_SECONDS)
                futures[executor.submit(self.get_count_query, query)] = query
            for future in as_completed(futures):
                query = futures[future]
                result: Optional[int]
                detail: Optional[str]
                try:
                    result, detail = future.result()
                except Exception as e:
                    print(f"Query failed for '{query}': {e}")
                    result, detail = None, f"{query}: {e}"
                if detail:
                    failure_details.append(detail)
                if result is None and self.store_failed_as == "omit":
                    continue
                rows.append((collect_iso, query, result))

        if rows:
            self._con.executemany(
                "INSERT INTO s1_metrics (Timestamp, Query, Result) VALUES (?, ?, ?)",
                rows,
            )
            print(f"Stored {len(rows)} rows")

        if failure_details:
            self._set_status(last_error=summarize_query_failures(failure_details))
        else:
            self._set_status(last_success_at=collect_iso, clear_error=True)

    def get_count_query(self, params: str) -> Tuple[Optional[int], Optional[str]]:
        base = os.getenv("SENTINELONE_URL")
        token = os.getenv("SENTINELONE_AUTH_TOKEN")
        if not base or not token:
            raise RuntimeError("SENTINELONE_URL and SENTINELONE_AUTH_TOKEN must be set")
        return fetch_agent_count(base, token, params)


app = Flask(__name__)
collector: Optional[MetricsCollector] = None


def _parse_bearer_token(authorization_header: Optional[str]) -> Optional[str]:
    if not authorization_header:
        return None
    parts = authorization_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _cors_origin_allowed() -> bool:
    origin = request.headers.get("Origin")
    return bool(origin and origin in CORS_ALLOWED_ORIGINS)


@app.before_request
def handle_cors_preflight() -> Optional[Response]:
    if request.method != "OPTIONS" or not CORS_ALLOWED_ORIGINS:
        return None
    if not _cors_origin_allowed():
        return None
    response = Response("", status=204)
    response.headers["Access-Control-Allow-Origin"] = request.headers["Origin"]
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@app.after_request
def add_cors_headers(response: Response) -> Response:
    if CORS_ALLOWED_ORIGINS and _cors_origin_allowed():
        response.headers["Access-Control-Allow-Origin"] = request.headers["Origin"]
        response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@app.before_request
def require_api_token() -> Optional[Response]:
    if request.method == "OPTIONS":
        return None
    if not metrics_auth_enabled():
        return None
    if request.path == "/healthz":
        return None
    if not request.path.startswith("/metrics"):
        return None
    provided = _parse_bearer_token(request.headers.get("Authorization"))
    if not bearer_authorized(provided):
        return jsonify({"error": "unauthorized"}), 401
    return None


def get_read_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)
    return con


def _parse_iso_ts(value: str) -> datetime:
    if len(value) == 10:
        return datetime.fromisoformat(value + "T00:00:00+00:00")
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def parse_int_query_param(
    name: str,
    raw: Optional[str],
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> Tuple[Optional[int], Optional[Tuple[dict, int]]]:
    if raw is None or raw == "":
        return max(minimum, min(default, maximum)), None
    try:
        value = int(raw)
    except ValueError:
        return None, ({"error": f"{name} must be an integer"}, 400)
    if value < minimum or value > maximum:
        return None, (
            {"error": f"{name} must be between {minimum} and {maximum}"},
            400,
        )
    return value, None


def _ping_db() -> bool:
    con = get_read_connection()
    try:
        con.execute("SELECT 1").fetchone()
        return True
    except duckdb.Error:
        return False
    finally:
        con.close()


@app.get("/healthz")
def healthz() -> Any:
    db_ok = _ping_db()
    thread_alive = collector.collector_thread_alive() if collector else False
    payload = {
        "ok": db_ok and thread_alive,
        "db_ok": db_ok,
        "collector_thread_alive": thread_alive,
        "last_collect_at": collector.last_collect_at if collector else None,
        "last_success_at": collector.last_success_at if collector else None,
        "last_error": collector.last_error if collector else None,
    }
    status = 200 if payload["ok"] else 503
    return jsonify(payload), status


@app.get("/metrics/latest")
def metrics_latest() -> Any:
    query_filter = request.args.get("query")
    limit, err = parse_int_query_param(
        "limit", request.args.get("limit"), default=200, minimum=1, maximum=2000
    )
    if err:
        body, status = err
        return jsonify(body), status

    con = get_read_connection()
    try:
        if query_filter:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                WHERE Query = ?
                ORDER BY Timestamp DESC
                LIMIT ?
                """,
                (query_filter, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                ORDER BY Timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    finally:
        con.close()

    return jsonify(
        [{"timestamp": str(r[0]), "query": r[1], "result": r[2]} for r in rows]
    )


@app.get("/metrics/range")
def metrics_range() -> Any:
    from_ts = request.args.get("from")
    to_ts = request.args.get("to")
    query_filter = request.args.get("query")
    limit, err = parse_int_query_param(
        "limit",
        request.args.get("limit"),
        default=MAX_RESULT_ROWS,
        minimum=1,
        maximum=MAX_RESULT_ROWS,
    )
    if err:
        body, status = err
        return jsonify(body), status

    if not from_ts or not to_ts:
        return jsonify({"error": "from and to query params are required"}), 400

    try:
        start = _parse_iso_ts(from_ts)
        end = _parse_iso_ts(to_ts)
    except ValueError:
        return jsonify({"error": "from/to must be valid ISO timestamps"}), 400

    if end <= start:
        return jsonify({"error": "to must be greater than from"}), 400

    span_days = (end - start).total_seconds() / 86400
    if span_days > MAX_RANGE_DAYS:
        return jsonify(
            {"error": f"requested range too large; max is {MAX_RANGE_DAYS} days"}
        ), 400

    con = get_read_connection()
    try:
        if query_filter:
            rows = con.execute(
                """
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MIN(Result) AS min_result,
                    AVG(Result)::DOUBLE AS avg_result,
                    MAX(Result) AS max_result,
                    COUNT(Result) AS sample_count
                FROM s1_metrics
                WHERE Timestamp >= ?::TIMESTAMPTZ
                  AND Timestamp < ?::TIMESTAMPTZ
                  AND Query = ?
                GROUP BY hour, Query
                ORDER BY hour ASC, Query
                LIMIT ?
                """,
                (from_ts, to_ts, query_filter, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MIN(Result) AS min_result,
                    AVG(Result)::DOUBLE AS avg_result,
                    MAX(Result) AS max_result,
                    COUNT(Result) AS sample_count
                FROM s1_metrics
                WHERE Timestamp >= ?::TIMESTAMPTZ
                  AND Timestamp < ?::TIMESTAMPTZ
                GROUP BY hour, Query
                ORDER BY hour ASC, Query
                LIMIT ?
                """,
                (from_ts, to_ts, limit),
            ).fetchall()
    finally:
        con.close()

    return jsonify(
        [
            {
                "hour": str(r[0]),
                "query": r[1],
                "min_result": r[2],
                "avg_result": r[3],
                "max_result": r[4],
                "sample_count": r[5],
            }
            for r in rows
        ]
    )


@app.get("/metrics/daily-max")
def daily_max() -> Any:
    days, err = parse_int_query_param(
        "days", request.args.get("days"), default=30, minimum=1, maximum=MAX_RANGE_DAYS
    )
    if err:
        body, status = err
        return jsonify(body), status

    con = get_read_connection()
    try:
        rows = con.execute(
            """
            SELECT
                CAST(Timestamp AS DATE) AS day,
                Query,
                MAX(Result) AS max_result
            FROM s1_metrics
            WHERE Timestamp >= now() - (? * INTERVAL '1 day')
            GROUP BY day, Query
            ORDER BY day DESC, Query
            """,
            (days,),
        ).fetchall()
    finally:
        con.close()

    return jsonify([{"day": str(r[0]), "query": r[1], "max_result": r[2]} for r in rows])


@app.get("/metrics/hourly-max")
def hourly_max() -> Any:
    hours_raw = request.args.get("hours")
    if hours_raw is not None and hours_raw != "":
        window_amount, err = parse_int_query_param(
            "hours", hours_raw, default=24, minimum=1, maximum=MAX_RANGE_HOURS
        )
        if err:
            body, status = err
            return jsonify(body), status
        window_predicate = "Timestamp >= now() - (? * INTERVAL '1 hour')"
    else:
        window_amount, err = parse_int_query_param(
            "days", request.args.get("days"), default=7, minimum=1, maximum=MAX_RANGE_DAYS
        )
        if err:
            body, status = err
            return jsonify(body), status
        window_predicate = "Timestamp >= now() - (? * INTERVAL '1 day')"

    query_filter = request.args.get("query")

    con = get_read_connection()
    try:
        if query_filter:
            rows = con.execute(
                f"""
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MAX(Result) AS max_result
                FROM s1_metrics
                WHERE {window_predicate}
                  AND Query = ?
                GROUP BY hour, Query
                ORDER BY hour ASC, Query
                """,
                (window_amount, query_filter),
            ).fetchall()
        else:
            rows = con.execute(
                f"""
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MAX(Result) AS max_result
                FROM s1_metrics
                WHERE {window_predicate}
                GROUP BY hour, Query
                ORDER BY hour ASC, Query
                """,
                (window_amount,),
            ).fetchall()
    finally:
        con.close()

    return jsonify([{"hour": str(r[0]), "query": r[1], "max_result": r[2]} for r in rows])


@app.get("/metrics/raw")
def metrics_raw() -> Any:
    hours, err = parse_int_query_param(
        "hours", request.args.get("hours"), default=24, minimum=1, maximum=MAX_RANGE_HOURS
    )
    if err:
        body, status = err
        return jsonify(body), status

    limit, err = parse_int_query_param(
        "limit",
        request.args.get("limit"),
        default=MAX_RESULT_ROWS,
        minimum=1,
        maximum=MAX_RESULT_ROWS,
    )
    if err:
        body, status = err
        return jsonify(body), status

    query_filter = request.args.get("query")

    con = get_read_connection()
    try:
        if query_filter:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                WHERE Timestamp >= now() - (? * INTERVAL '1 hour')
                  AND Query = ?
                ORDER BY Timestamp ASC, Query
                LIMIT ?
                """,
                (hours, query_filter, limit),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                WHERE Timestamp >= now() - (? * INTERVAL '1 hour')
                ORDER BY Timestamp ASC, Query
                LIMIT ?
                """,
                (hours, limit),
            ).fetchall()
    finally:
        con.close()

    return jsonify(
        [{"timestamp": str(r[0]), "query": r[1], "result": r[2]} for r in rows]
    )


def _static_dir_ready() -> bool:
    index_path = os.path.join(STATIC_DIR, "index.html")
    return os.path.isfile(index_path)


@app.get("/")
def spa_index() -> Any:
    if not _static_dir_ready():
        return jsonify({"error": "web ui not installed"}), 404
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/<path:asset_path>")
def spa_assets(asset_path: str) -> Any:
    if asset_path.startswith("metrics"):
        return jsonify({"error": "not found"}), 404
    if not _static_dir_ready():
        return jsonify({"error": "web ui not installed"}), 404
    file_path = os.path.join(STATIC_DIR, asset_path)
    if os.path.isfile(file_path):
        return send_from_directory(STATIC_DIR, asset_path)
    return send_from_directory(STATIC_DIR, "index.html")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SentinelOne metrics collector + API server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--initial-run", action="store_true")
    parser.add_argument("--max-query-workers", type=int, default=MAX_QUERY_WORKERS)
    parser.add_argument(
        "--nice-adjust",
        type=int,
        default=DEFAULT_NICE_ADJUST,
        help="Increase process niceness on startup (Linux). Higher = lower CPU priority.",
    )
    return parser.parse_args()


def apply_process_nice(nice_adjust: int) -> None:
    if nice_adjust <= 0:
        return
    try:
        current = os.nice(0)
        new_value = os.nice(nice_adjust)
        print(f"Adjusted process niceness: {current} -> {new_value}")
    except OSError as e:
        print(f"Unable to adjust process niceness by {nice_adjust}: {e}")


def start_collector(
    *,
    initial_run: bool = False,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    max_workers: int = MAX_QUERY_WORKERS,
    nice_adjust: int = DEFAULT_NICE_ADJUST,
) -> None:
    global collector
    if collector is not None and collector.collector_thread_alive():
        return
    apply_process_nice(nice_adjust)
    collector = MetricsCollector(
        db_path=DB_PATH,
        queries_path=QUERIES_PATH,
        interval_seconds=interval_seconds,
        max_workers=max_workers,
    )
    collector.start(initial_run=initial_run)


def stop_collector() -> None:
    global collector
    if collector is not None:
        collector.stop()
        collector = None


def main() -> None:
    args = parse_args()
    start_collector(
        initial_run=args.initial_run,
        interval_seconds=args.interval_seconds,
        max_workers=args.max_query_workers,
        nice_adjust=args.nice_adjust,
    )
    try:
        app.run(host=args.host, port=args.port, threaded=True)
    finally:
        stop_collector()


if __name__ == "__main__":
    main()
