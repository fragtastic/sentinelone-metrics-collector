# Implementation plan

Tracking document for hardening and operational improvements.

## Context (product decisions)

- Single Docker collector on EC2; one writer per DuckDB file.
- Production `queries.json` is deployment config (not in repo).
- Reporting: Excel via HTTP API today; future SSO frontend with client-side charts.
- API auth: optional `API_TOKEN` with `Authorization: Bearer` (Excel-compatible); optional OIDC JWT verification for browser SSO (Phase 7b).
- Failed SentinelOne reads: `STORE_FAILED_AS` = `null` (default) or `omit`.
- Data retention / compaction: **deferred** (see [Retention design](#retention--compaction-design) below).

## Phases

| Phase | Scope | Status |
|-------|--------|--------|
| 1 | Operability: runbook, `/healthz` depth, query param validation, pin deps, `.dockerignore`, `.gitignore` | done |
| 2 | `STORE_FAILED_AS` collector behavior + docs | done |
| 3 | API bearer token auth | done |
| 4 | pytest suite + GitHub Actions (test + docker build) | done |
| 5 | Repo hygiene: remove obsolete export scripts; docs point to API | done |
| 6 | Performance: DuckDB indexes on read paths | done |
| 7a | Web UI (same-origin SPA, client-side charts) | done on `feature/metrics-web-ui` |
| 7b | Browser OIDC login + API JWT verification (split-origin UI OK) | deferred subtask |
| 8 | Data retention / compaction job | deferred |
| 9 | Production WSGI (gunicorn) | deferred (documented in readme) |

## Phase 7b — SSO (deferred)

**Target model (IdP-agnostic; Okta is one instance):**

1. UI is hosted separately from the collector (static CDN, S3, or any origin).
2. Browser runs **OIDC authorization code + PKCE** against the SSO server (issuer URL + client id).
3. Browser receives an **access token** (JWT) from the IdP.
4. UI calls the metrics API with `Authorization: Bearer <access_token>` (see `frontend/src/auth/`).
5. Collector verifies the JWT via **`OIDC_ISSUER`** JWKS (`metrics_auth.py`): signature, `iss`, `exp`, and optional **`OIDC_AUDIENCE`** (API/client id).
6. Set **`CORS_ALLOWED_ORIGINS`** on the collector to the UI origin(s).

**Still supported:** `API_TOKEN` for Excel, scripts, and automation (can coexist with OIDC).

**Remaining work:**

- Integrate an OIDC client in the SPA (`VITE_OIDC_*`), call `setAccessToken` after login, handle refresh/sign-out.
- Configure Okta (or other) app: SPA redirect URIs, allowed origins, access token audience for the API.
- Production `VITE_API_BASE_URL` pointing at the collector host.
- Retire dev-only `VITE_DEV_API_TOKEN` for human UI users once OIDC is live.

## Validation checklist

- [x] `pytest` passes locally
- [x] `docker build` succeeds
- [ ] CI workflow green on push (after push to GitHub)
- [ ] `frontend/` lint, test, and build pass in CI
- [ ] Docker image serves UI at `/` with metrics auth on `/metrics/*` only

## Retention / compaction design

**SentinelOne usage semantics (from ops):**

- Billing / usage is based on the **maximum agent count per day** (per query / filter), not every raw sample.
- SentinelOne’s own metering samples on a **5-minute** cadence.

**This collector today:**

- Stores one row per query per collection cycle (`COLLECT_INTERVAL_SECONDS`, default **60s**).
- `/metrics/daily-max` and hourly/range aggregates already compute **max** (and min/avg for hourly range) from raw rows on read.

**Implication for a future retention job (Phase 8):**

Compaction should preserve what matters for SentinelOne alignment and reporting, not necessarily every minute-level point forever.

| Tier | Granularity | Purpose |
|------|-------------|---------|
| Hot | Raw samples (1-min or whatever `COLLECT_INTERVAL_SECONDS` is) | Recent troubleshooting, Excel detail, short-range charts |
| Optional mid | 5-min or hourly **max** per query | Closer to S1 sampling grid; smaller than raw |
| Cold | **Daily max** per query | Matches S1 usage definition; long-term history |

Suggested env knobs (not implemented):

- `RETENTION_RAW_DAYS` — drop or roll up raw rows older than N days.
- `RETENTION_DAILY_MAX_DAYS` — keep daily max rows for M days (or `0` = forever).
- Roll-up: before deleting raw rows, insert into `s1_metrics_daily_max` (or aggregate table) so cold tier is lossless for **max-per-day** reporting.

**Optional alignment:** setting `COLLECT_INTERVAL_SECONDS=300` reduces volume and matches S1’s 5-minute grid without losing daily-max accuracy (max of maxes ≥ true daily max if samples cover the day). Finer intervals still help catch intraday peaks if the collection window misses the true daily peak.

## Changelog

- Added bearer API auth, collector status fields, richer `/healthz`, integer param validation.
- Added `STORE_FAILED_AS`, DuckDB indexes, pinned requirements, tests, CI workflow.
- Removed legacy export scripts and untracked scratch/migration files.
- Expanded EC2/Docker runbook and Excel Power Query docs.
- Added `frontend/` React SPA, Flask static hosting, metrics-only bearer auth (Option A), multi-stage Docker build, frontend CI job.
- Added `metrics_auth.py` (API token + optional OIDC JWT verification), CORS env, browser-OIDC SSO design for split-origin UI (Phase 7b).
