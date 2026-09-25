"""SentinelOne agents/count client with pacing, retries, and actionable errors."""

from __future__ import annotations

import os
import time
from typing import Optional, Tuple

import requests

S1_HTTP_CONNECT_TIMEOUT = float(os.getenv("S1_HTTP_CONNECT_TIMEOUT_SECONDS", "5"))
S1_HTTP_READ_TIMEOUT = float(os.getenv("S1_HTTP_READ_TIMEOUT_SECONDS", "45"))
S1_QUERY_RETRIES = max(0, int(os.getenv("S1_QUERY_RETRIES", "1")))
S1_QUERY_RETRY_DELAY = float(os.getenv("S1_QUERY_RETRY_DELAY_SECONDS", "2"))


def describe_s1_failure(
    query: str,
    exc: BaseException,
    *,
    status_code: Optional[int] = None,
) -> str:
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return (
            f"{query}: connect timeout after {S1_HTTP_CONNECT_TIMEOUT}s "
            f"(check network/DNS to SentinelOne; S1_HTTP_CONNECT_TIMEOUT_SECONDS)"
        )
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return (
            f"{query}: read timeout after {S1_HTTP_READ_TIMEOUT}s "
            f"(S1 slow or too many parallel queries; lower MAX_QUERY_WORKERS, "
            f"raise S1_HTTP_READ_TIMEOUT_SECONDS, increase S1_QUERY_STAGGER_SECONDS)"
        )
    if isinstance(exc, requests.exceptions.Timeout):
        return (
            f"{query}: request timeout "
            f"(connect={S1_HTTP_CONNECT_TIMEOUT}s read={S1_HTTP_READ_TIMEOUT}s)"
        )
    if isinstance(exc, requests.exceptions.SSLError):
        return f"{query}: TLS/SSL error ({exc})"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return f"{query}: connection failed ({exc})"

    if status_code is not None:
        if status_code == 401:
            return f"{query}: HTTP 401 unauthorized (verify SENTINELONE_AUTH_TOKEN)"
        if status_code == 403:
            return f"{query}: HTTP 403 forbidden (token lacks agents/count permission)"
        if status_code == 429:
            return (
                f"{query}: HTTP 429 rate limited "
                f"(reduce MAX_QUERY_WORKERS, increase S1_QUERY_STAGGER_SECONDS)"
            )
        if status_code >= 500:
            return f"{query}: HTTP {status_code} SentinelOne server error (retry later)"
        return f"{query}: HTTP {status_code} ({exc})"

    if isinstance(exc, (KeyError, ValueError, TypeError)):
        return f"{query}: unexpected response JSON ({exc})"

    return f"{query}: {exc}"


def _retryable(exc: BaseException, status_code: Optional[int]) -> bool:
    if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    if status_code in (429, 502, 503, 504):
        return True
    return False


def fetch_agent_count(
    console_subdomain: str,
    api_token: str,
    query: str,
) -> Tuple[Optional[int], Optional[str]]:
    url = (
        f"https://{console_subdomain}.sentinelone.net/web/api/v2.1/agents/count?{query}"
    )
    headers = {
        "Accept": "application/json",
        "Authorization": "ApiToken " + api_token,
    }
    timeout = (S1_HTTP_CONNECT_TIMEOUT, S1_HTTP_READ_TIMEOUT)
    last_detail: Optional[str] = None

    for attempt in range(S1_QUERY_RETRIES + 1):
        if attempt > 0:
            time.sleep(S1_QUERY_RETRY_DELAY)
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            total = response.json()["data"]["total"]
            return int(total), None
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            last_detail = describe_s1_failure(query, exc, status_code=status)
            if attempt < S1_QUERY_RETRIES and _retryable(exc, status):
                print(f"S1 retry {attempt + 1}/{S1_QUERY_RETRIES} for {query}: {last_detail}")
                continue
            print(f"S1 HTTP error: {last_detail}")
            return None, last_detail
        except requests.exceptions.RequestException as exc:
            last_detail = describe_s1_failure(query, exc)
            if attempt < S1_QUERY_RETRIES and _retryable(exc, None):
                print(f"S1 retry {attempt + 1}/{S1_QUERY_RETRIES} for {query}: {last_detail}")
                continue
            print(f"S1 HTTP error: {last_detail}")
            return None, last_detail
        except (KeyError, ValueError, TypeError) as exc:
            last_detail = describe_s1_failure(query, exc)
            print(f"S1 HTTP error: {last_detail}")
            return None, last_detail

    return None, last_detail


def summarize_query_failures(failures: list[str], *, max_lines: int = 4) -> str:
    if not failures:
        return "one or more SentinelOne count queries failed"
    shown = failures[:max_lines]
    body = "; ".join(shown)
    extra = len(failures) - len(shown)
    if extra > 0:
        body += f"; +{extra} more"
    return f"S1 count failures ({len(failures)}): {body}"
