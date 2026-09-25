/**
 * Returns the access token for API calls.
 * Phase 7b: populate via browser OIDC (see oidc.ts / README); until then null in production
 * same-origin builds, or dev proxy uses VITE_DEV_API_TOKEN instead.
 */
let cachedAccessToken: string | null = null

export function setAccessToken(token: string | null): void {
  cachedAccessToken = token
}

export function getAccessToken(): string | null {
  return cachedAccessToken
}
