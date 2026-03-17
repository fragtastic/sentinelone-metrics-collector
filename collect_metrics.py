import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

DB_PATH = os.getenv("METRICS_DB_PATH", "metrics.duckdb")
QUERIES_PATH = os.getenv("QUERIES_PATH", "queries.json")
DEFAULT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))
MAX_QUERY_WORKERS = int(os.getenv("MAX_QUERY_WORKERS", "8"))
DEFAULT_NICE_ADJUST = int(os.getenv("PROCESS_NICE_ADJUST", "10"))


class MetricsCollector:
    def __init__(self, db_path: str, queries_path: str, interval_seconds: int, max_workers: int) -> None:
        self.db_path = db_path
        self.queries_path = queries_path
        self.interval_seconds = interval_seconds
        self.max_workers = max_workers

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._queries: List[str] = []

        # Writer connection is dedicated to collector thread.
        self._con = duckdb.connect(self.db_path)
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS s1_metrics (
                Timestamp TIMESTAMPTZ,
                Query VARCHAR,
                Result INTEGER NULL
            );
            """
        )

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

    def load_queries(self) -> List[str]:
        with open(self.queries_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("queries.json must contain a JSON array of query strings")
            return [str(x) for x in data]

    def _run_loop(self, initial_run: bool = False) -> None:
        if initial_run:
            self.collect_once()

        while not self._stop_event.is_set():
            # Wait supports quick shutdown if stop event is set.
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

    def collect_once(self) -> None:
        current_time = datetime.now(timezone.utc)
        print(f"Collecting metrics @ {current_time.isoformat()}")

        if not self._queries:
            print("No queries configured; skipping collection run.")
            return

        rows: List[tuple[str, str, Optional[int]]] = []
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
                rows.append((current_time.isoformat(), query, result))

        self._con.executemany(
            "INSERT INTO s1_metrics (Timestamp, Query, Result) VALUES (?, ?, ?)",
            rows,
        )
        print(f"Stored {len(rows)} rows")

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


def get_read_connection() -> duckdb.DuckDBPyConnection:
    # Dedicated connection per request thread.
    # IMPORTANT: DuckDB cannot open the same file with mixed configs (e.g. read_only=True
    # on one connection while another connection is read-write). The collector keeps a
    # read-write writer connection open, so request connections must use the same config.
    return duckdb.connect(DB_PATH)


@app.get("/healthz")
def healthz() -> Any:
    return jsonify({"ok": True})


@app.get("/metrics/latest")
def metrics_latest() -> Any:
    query_filter = request.args.get("query")
    limit = max(1, min(int(request.args.get("limit", "200")), 2000))

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
        [
            {"timestamp": str(r[0]), "query": r[1], "result": r[2]}
            for r in rows
        ]
    )


@app.get("/metrics/range")
def metrics_range() -> Any:
    # Example: /metrics/range?from=2026-03-01T00:00:00Z&to=2026-03-02T00:00:00Z
    from_ts = request.args.get("from")
    to_ts = request.args.get("to")
    query_filter = request.args.get("query")

    if not from_ts or not to_ts:
        return jsonify({"error": "from and to query params are required"}), 400

    con = get_read_connection()
    try:
        if query_filter:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                WHERE Timestamp >= ?::TIMESTAMPTZ
                  AND Timestamp < ?::TIMESTAMPTZ
                  AND Query = ?
                ORDER BY Timestamp ASC
                """,
                (from_ts, to_ts, query_filter),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT Timestamp, Query, Result
                FROM s1_metrics
                WHERE Timestamp >= ?::TIMESTAMPTZ
                  AND Timestamp < ?::TIMESTAMPTZ
                ORDER BY Timestamp ASC
                """,
                (from_ts, to_ts),
            ).fetchall()
    finally:
        con.close()

    return jsonify(
        [
            {"timestamp": str(r[0]), "query": r[1], "result": r[2]}
            for r in rows
        ]
    )


@app.get("/metrics/daily-max")
def daily_max() -> Any:
    days = max(1, min(int(request.args.get("days", "30")), 3650))

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

    return jsonify([
        {"day": str(r[0]), "query": r[1], "max_result": r[2]}
        for r in rows
    ])


@app.get("/metrics/hourly-max")
def hourly_max() -> Any:
    days = max(1, min(int(request.args.get("days", "7")), 3650))
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

    return jsonify([
        {"hour": str(r[0]), "query": r[1], "max_result": r[2]}
        for r in rows
    ])


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
        # threaded=True enables concurrent API requests while collector thread runs.
        app.run(host=args.host, port=args.port, threaded=True)
    finally:
        collector.stop()


if __name__ == "__main__":
    main()
