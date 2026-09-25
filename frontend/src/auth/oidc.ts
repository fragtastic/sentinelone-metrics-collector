/**
 * Generic OIDC settings for browser login (Okta, Entra ID, etc.).
 * Wire an OIDC client library in Phase 7b; config is IdP-agnostic.
 */
export type OidcConfig = {
  authority: string
  clientId: string
  redirectUri: string
  scope: string
}

export function getOidcConfig(): OidcConfig | null {
  const authority = import.meta.env.VITE_OIDC_AUTHORITY as string | undefined
  const clientId = import.meta.env.VITE_OIDC_CLIENT_ID as string | undefined
  const redirectUri = import.meta.env.VITE_OIDC_REDIRECT_URI as string | undefined
  const scope = (import.meta.env.VITE_OIDC_SCOPE as string | undefined) ?? 'openid profile email'

  if (!authority || !clientId || !redirectUri) {
    return null
  }

  return { authority: authority.replace(/\/$/, ''), clientId, redirectUri, scope }
}

export function isOidcConfigured(): boolean {
  return getOidcConfig() !== null
}
