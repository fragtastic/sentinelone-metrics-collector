import importlib
import json
import sys
from unittest.mock import patch

import duckdb
import pytest


@pytest.fixture
def metrics_module(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.duckdb")
    queries_path = tmp_path / "queries.json"
    queries_path.write_text(json.dumps(["q1"]), encoding="utf-8")

    monkeypatch.setenv("METRICS_DB_PATH", db_path)
    monkeypatch.setenv("QUERIES_PATH", str(queries_path))
    monkeypatch.setenv("SENTINELONE_URL", "example")
    monkeypatch.setenv("SENTINELONE_AUTH_TOKEN", "token")
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.setenv("STORE_FAILED_AS", "null")

    if "collect_metrics" in sys.modules:
        del sys.modules["collect_metrics"]
    cm = importlib.import_module("collect_metrics")
    yield cm
    if cm.collector is not None:
        try:
            cm.collector.stop()
        except Exception:
            pass
    cm.collector = None


def test_parse_iso_ts_date_only(metrics_module):
    dt = metrics_module._parse_iso_ts("2026-03-17")
    assert dt.isoformat() == "2026-03-17T00:00:00+00:00"


def test_parse_iso_ts_z_suffix(metrics_module):
    dt = metrics_module._parse_iso_ts("2026-03-17T12:00:00Z")
    assert dt.hour == 12


def test_parse_int_query_param_invalid(metrics_module):
    _, err = metrics_module.parse_int_query_param(
        "limit", "abc", default=10, minimum=1, maximum=100
    )
    assert err is not None
    body, status = err
    assert status == 400
    assert "integer" in body["error"]


def test_api_token_required(metrics_module, monkeypatch):
    monkeypatch.setattr(metrics_module, "API_TOKEN", "secret-token")
    client = metrics_module.app.test_client()
    # /healthz does not require a token (may be 503 when collector is not running).
    assert client.get("/healthz").status_code in (200, 503)
    assert client.get("/metrics/latest").status_code == 401
    assert (
        client.get(
            "/metrics/latest",
            headers={"Authorization": "Bearer secret-token"},
        ).status_code
        == 200
    )


def test_healthz_without_collector(metrics_module, monkeypatch):
    monkeypatch.setattr(metrics_module, "collector", None)
    client = metrics_module.app.test_client()
    resp = client.get("/healthz")
    assert resp.status_code == 503
    data = resp.get_json()
    assert data["db_ok"] is True
    assert data["collector_thread_alive"] is False


def test_healthz_with_collector(metrics_module):
    cm = metrics_module
    collector = cm.MetricsCollector(
        db_path=cm.DB_PATH,
        queries_path=cm.QUERIES_PATH,
        interval_seconds=3600,
        max_workers=1,
    )
    cm.collector = collector
    collector.start(initial_run=False)
    client = cm.app.test_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200
    collector.stop()
    cm.collector = None


def test_store_failed_as_omit(metrics_module):
    cm = metrics_module
    collector = cm.MetricsCollector(
        db_path=cm.DB_PATH,
        queries_path=cm.QUERIES_PATH,
        interval_seconds=60,
        max_workers=1,
        store_failed_as="omit",
    )
    collector._queries = ["q1", "q2"]

    with patch.object(collector, "get_count_query", side_effect=[100, None]):
        collector.collect_once()

    con = duckdb.connect(cm.DB_PATH)
    try:
        count = con.execute("SELECT COUNT(*) FROM s1_metrics").fetchone()[0]
        rows = con.execute("SELECT Query, Result FROM s1_metrics").fetchall()
    finally:
        con.close()

    assert count == 1
    assert rows == [("q1", 100)]


def test_store_failed_as_null(metrics_module):
    cm = metrics_module
    collector = cm.MetricsCollector(
        db_path=cm.DB_PATH,
        queries_path=cm.QUERIES_PATH,
        interval_seconds=60,
        max_workers=1,
        store_failed_as="null",
    )
    collector._queries = ["q1"]

    with patch.object(collector, "get_count_query", return_value=None):
        collector.collect_once()

    con = duckdb.connect(cm.DB_PATH)
    try:
        row = con.execute("SELECT Result FROM s1_metrics").fetchone()
    finally:
        con.close()

    assert row[0] is None


def test_metrics_range_validation(metrics_module):
    client = metrics_module.app.test_client()
    resp = client.get("/metrics/range?from=2026-01-02&to=2026-01-01")
    assert resp.status_code == 400
