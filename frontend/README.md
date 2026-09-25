# SentinelOne metrics web UI

React SPA for charts and tables against the collector HTTP API. Charts are rendered entirely in the browser.

## Prerequisites

- Node.js 22+
- Collector running on `http://127.0.0.1:8080` (local or SSH tunnel)

## Setup

```bash
cd frontend
npm ci
cp .env.example .env.local
# Edit .env.local if the collector uses API_TOKEN
```

## Development

```bash
npm run dev
```

Open the URL Vite prints (default `http://localhost:5173`). API calls to `/healthz` and `/metrics/*` are proxied to port 8080; when `VITE_DEV_API_TOKEN` is set, the proxy adds `Authorization: Bearer`.

## Scripts

| Command | Purpose |
|---------|---------|
| `npm run dev` | Vite dev server with API proxy |
| `npm run build` | Production bundle to `dist/` |
| `npm run test` | Vitest unit tests |
| `npm run lint` | oxlint |

## Deployment models

**Same origin:** Docker builds `dist/` into the collector image (`/app/static`). Leave `VITE_API_BASE_URL` unset.

**Split origin (target for SSO):** Host `dist/` on any static platform. Set at build time:

- `VITE_API_BASE_URL` — collector URL (e.g. `https://metrics-api.example.com`)
- On the collector: `CORS_ALLOWED_ORIGINS` includes the UI origin

## SSO (Phase 7b — browser OIDC)

The UI is designed to authenticate with any OIDC provider (Okta, Entra ID, etc.) in the **browser**, obtain an **access token**, and send it on API requests. The collector validates JWTs via `OIDC_ISSUER` / `OIDC_AUDIENCE` (see root `readme.md`).

Configure the SPA (build-time env):

| Variable | Purpose |
|----------|---------|
| `VITE_OIDC_AUTHORITY` | Issuer URL (same as collector `OIDC_ISSUER`) |
| `VITE_OIDC_CLIENT_ID` | Public OIDC client id for the SPA |
| `VITE_OIDC_REDIRECT_URI` | Login redirect URI registered with the IdP |
| `VITE_OIDC_SCOPE` | Optional; default `openid profile email` |

Implementation hooks live in `src/auth/oidc.ts` and `src/auth/accessToken.ts`. After login, call `setAccessToken(accessToken)`; `src/api/client.ts` attaches `Authorization: Bearer` automatically.

Until OIDC is wired, use same-origin Docker, SSH tunnel, or `VITE_DEV_API_TOKEN` for local dev.

## Query labels

Optional overrides for legend text live in `src/queryLabels.json`, keyed by the full query string from `queries.json`.
