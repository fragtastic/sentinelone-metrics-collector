import requests

from s1_upstream import describe_s1_failure, summarize_query_failures


def test_describe_read_timeout_includes_tuning_hints():
    msg = describe_s1_failure("q=a", requests.exceptions.ReadTimeout("timed out"))
    assert "read timeout" in msg
    assert "MAX_QUERY_WORKERS" in msg
    assert "q=a" in msg


def test_describe_http_401():
    exc = requests.HTTPError("401")
    exc.response = type("R", (), {"status_code": 401})()
    msg = describe_s1_failure("q1", exc, status_code=401)
    assert "401" in msg
    assert "SENTINELONE_AUTH_TOKEN" in msg


def test_summarize_failures_truncates():
    failures = [f"q{i}: err" for i in range(6)]
    summary = summarize_query_failures(failures, max_lines=2)
    assert "S1 count failures (6)" in summary
    assert "+4 more" in summary
