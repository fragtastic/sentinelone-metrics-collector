# Implementation plan

Tracking document for hardening and operational improvements.

## Context (product decisions)

- Single Docker collector on EC2; one writer per DuckDB file.
- Production `queries.json` is deployment config (not in repo).
- Reporting: Excel via HTTP API today; future SSO frontend with client-side charts.
- API auth: optional `API_TOKEN` with `Authorization: Bearer` (Excel-compatible).
- Failed SentinelOne reads: `STORE_FAILED_AS` = `null` (default) or `omit`.
- Data retention: **deferred** (env-driven purge/archive — not implemented).

## Phases

| Phase | Scope | Status |
|-------|--------|--------|
| 1 | Operability: runbook, `/healthz` depth, query param validation, pin deps, `.dockerignore`, `.gitignore` | done |
| 2 | `STORE_FAILED_AS` collector behavior + docs | done |
| 3 | API bearer token auth | done |
| 4 | pytest suite + GitHub Actions (test + docker build) | done |
| 5 | Repo hygiene: remove obsolete export scripts; docs point to API | done |
| 6 | Performance: DuckDB indexes on read paths | done |
| 7 | SSO frontend + browser charts | deferred (separate project) |
| 8 | Data retention job | deferred |
| 9 | Production WSGI (gunicorn) | deferred (documented in readme) |

## Validation checklist

- [x] `pytest` passes locally
- [x] `docker build` succeeds
- [ ] CI workflow green on push (after push to GitHub)

## Changelog

- Added bearer API auth, collector status fields, richer `/healthz`, integer param validation.
- Added `STORE_FAILED_AS`, DuckDB indexes, pinned requirements, tests, CI workflow.
- Removed legacy export scripts and untracked scratch/migration files.
- Expanded EC2/Docker runbook and Excel Power Query docs.
