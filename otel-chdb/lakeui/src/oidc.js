// OIDC sign-in for a static page: authorization code + PKCE (S256), no
// client secret. The token is the query service's bearer token; the page
// never verifies it (the service does), it only reads its claims to show who
// is signed in and when the session ends.

const b64url = bytes => {
  let s = ''
  for (const b of bytes) s += String.fromCharCode(b)
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

export function randomString(bytes = 32, crypto = globalThis.crypto) {
  const a = new Uint8Array(bytes)
  crypto.getRandomValues(a)
  return b64url(a)
}

/** RFC 7636: verifier (43+ chars) and its S256 challenge. */
export async function pkcePair(crypto = globalThis.crypto) {
  const verifier = randomString(32, crypto)
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))
  return { verifier, challenge: b64url(new Uint8Array(digest)) }
}

export async function discover(issuer, { fetch = globalThis.fetch } = {}) {
  const url = issuer.replace(/\/$/, '') + '/.well-known/openid-configuration'
  const resp = await fetch(url, { cache: 'no-store' })
  if (!resp.ok) throw new Error(`OIDC discovery ${resp.status} at ${url}`)
  const doc = await resp.json()
  if (doc.issuer !== issuer.replace(/\/$/, '') && doc.issuer !== issuer) throw new Error(`OIDC discovery: issuer ${doc.issuer} is not ${issuer}`)
  if (!doc.authorization_endpoint || !doc.token_endpoint) throw new Error('OIDC discovery: no authorization or token endpoint')
  if (doc.code_challenge_methods_supported && !doc.code_challenge_methods_supported.includes('S256')) {
    throw new Error('OIDC: the issuer does not support PKCE S256')
  }
  return doc
}

export function authorizeURL(doc, { clientId, redirectUri, state, challenge, scope = 'openid', extra = {} }) {
  const u = new URL(doc.authorization_endpoint)
  const p = { response_type: 'code', client_id: clientId, redirect_uri: redirectUri, scope, state,
    code_challenge: challenge, code_challenge_method: 'S256', ...extra }
  for (const [k, v] of Object.entries(p)) u.searchParams.set(k, v)
  return u.toString()
}

/** Reads ?code=&state= (or an error) off the redirect URL. */
export function parseRedirect(href) {
  const u = new URL(href)
  const code = u.searchParams.get('code')
  const state = u.searchParams.get('state')
  const error = u.searchParams.get('error')
  if (!code && !error) return null
  return { code, state, error, description: u.searchParams.get('error_description') }
}

export async function exchangeCode(doc, { code, verifier, clientId, redirectUri }, { fetch = globalThis.fetch } = {}) {
  const body = new URLSearchParams({ grant_type: 'authorization_code', code, code_verifier: verifier, client_id: clientId, redirect_uri: redirectUri })
  const resp = await fetch(doc.token_endpoint, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body, cache: 'no-store' })
  const j = await resp.json().catch(() => ({}))
  if (!resp.ok || !j.access_token) throw new Error(`token endpoint ${resp.status}: ${j.error ?? 'no access_token'}`)
  return j.access_token
}

/** The token's claims, unverified (display only). */
export function claimsOf(jwt) {
  const parts = String(jwt).split('.')
  if (parts.length !== 3) throw new Error('not a JWT')
  const pad = parts[1].replace(/-/g, '+').replace(/_/g, '/')
  const json = decodeURIComponent(escape(atob(pad + '==='.slice((pad.length + 3) % 4))))
  return JSON.parse(json)
}

/** Seconds of validity left (negative: expired). */
export function secondsLeft(claims, nowMs = Date.now()) {
  return typeof claims.exp === 'number' ? claims.exp - nowMs / 1000 : -Infinity
}
