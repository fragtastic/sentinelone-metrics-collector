import argparse
import hmac
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

import duckdb
import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request

load_dotenv()

DB_PATH = os.getenv("METRICS_DB_PATH", "metrics.duckdb")
QUERIES_PATH = os.getenv("QUERIES_PATH", "queries.json")
DEFAULT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))
MAX_QUERY_WORKERS = int(os.getenv("MAX_QUERY_WORKERS", "8"))
DEFAULT_NICE_ADJUST = int(os.getenv("PROCESS_NICE_ADJUST", "10"))
MAX_RANGE_DAYS = int(os.getenv("API_MAX_RANGE_DAYS", "31"))
MAX_RESULT_ROWS = int(os.getenv("API_MAX_RESULT_ROWS", "10000"))
API_TOKEN = os.getenv("API_TOKEN", "").strip() or None

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
        had_failure = False
        with ThreadPoolExecutor(max_workers=min(len(self._queries), self.max_workers)) as executor:
            futures = {executor.submit(self.get_count_query, q): q for q in self._queries}
            for future in as_completed(futures):
                query = futures[future]
                result: Optional[int]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"Query failed for '{query}': {e}")
                    result = None
                if result is None:
                    had_failure = True
                    if self.store_failed_as == "omit":
                        continue
                rows.append((collect_iso, query, result))

        if rows:
            self._con.executemany(
                "INSERT INTO s1_metrics (Timestamp, Query, Result) VALUES (?, ?, ?)",
                rows,
            )
            print(f"Stored {len(rows)} rows")

        if had_failure:
            self._set_status(last_error="one or more SentinelOne count queries failed")
        else:
            self._set_status(last_success_at=collect_iso, clear_error=True)

    def get_count_query(self, params: str) -> Optional[int]:
        base = os.getenv("SENTINELONE_URL")
        token = os.getenv("SENTINELONE_AUTH_TOKEN")
        if not base or not token:
            raise RuntimeError("SENTINELONE_URL and SENTINELONE_AUTH_TOKEN must be set")

        url = f"https://{base}.sentinelone.net/web/api/v2.1/agents/count?{params}"
        headers = {
            "Accept": "application/json",
            "Authorization": "ApiToken " + token,
        }
        try:
            response = requests.get(url, headers=headers, timeout=(3, 10))
            response.raise_for_status()
            return response.json()["data"]["total"]
        except requests.exceptions.RequestException as e:
            print(f"HTTP error for {url}: {e}")
        except (KeyError, ValueError) as e:
            print(f"Invalid JSON from {url}: {e}")
        return None


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


def _bearer_token_valid(provided: Optional[str]) -> bool:
    if API_TOKEN is None or provided is None:
        return False
    if len(provided) != len(API_TOKEN):
        return False
    return hmac.compare_digest(provided, API_TOKEN)


@app.before_request
def require_api_token() -> Optional[Response]:
    if API_TOKEN is None:
        return None
    if request.path == "/healthz":
        return None
    provided = _parse_bearer_token(request.headers.get("Authorization"))
    if not _bearer_token_valid(provided):
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
    days, err = parse_int_query_param(
        "days", request.args.get("days"), default=7, minimum=1, maximum=MAX_RANGE_DAYS
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
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MAX(Result) AS max_result
                FROM s1_metrics
                WHERE Timestamp >= now() - (? * INTERVAL '1 day')
                  AND Query = ?
                GROUP BY hour, Query
                ORDER BY hour DESC, Query
                """,
                (days, query_filter),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT
                    date_trunc('hour', Timestamp) AS hour,
                    Query,
                    MAX(Result) AS max_result
                FROM s1_metrics
                WHERE Timestamp >= now() - (? * INTERVAL '1 day')
                GROUP BY hour, Query
                ORDER BY hour DESC, Query
                """,
                (days,),
            ).fetchall()
    finally:
        con.close()

    return jsonify([{"hour": str(r[0]), "query": r[1], "max_result": r[2]} for r in rows])


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


def main() -> None:
    global collector
    args = parse_args()

    apply_process_nice(args.nice_adjust)

    collector = MetricsCollector(
        db_path=DB_PATH,
        queries_path=QUERIES_PATH,
        interval_seconds=args.interval_seconds,
        max_workers=args.max_query_workers,
    )
    collector.start(initial_run=args.initial_run)

    try:
        app.run(host=args.host, port=args.port, threaded=True)
    finally:
        collector.stop()


if __name__ == "__main__":
    main()
