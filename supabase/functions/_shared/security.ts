export async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const enc = new TextEncoder()

  const aBuf = enc.encode(a)
  const bBuf = enc.encode(b)

  if (aBuf.length !== bBuf.length) return false

  let result = 0
  for (let i = 0; i < aBuf.length; i++) {
    result |= aBuf[i] ^ bBuf[i]
  }

  return result === 0
}

export async function hashToken(token: string): Promise<string> {
  const key = Deno.env.get("SENTRA_HMAC_KEY")

  if (!key) {
    throw new Error("Missing SENTRA_HMAC_KEY")
  }

  const enc = new TextEncoder()

  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    enc.encode(key),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  )

  const signature = await crypto.subtle.sign(
    "HMAC",
    cryptoKey,
    enc.encode(token)
  )

  return Array.from(new Uint8Array(signature))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
}

export function validateOrigin(req: Request): boolean {
  const origin = req.headers.get("origin");
  
  if (!origin) {
    return true;
  }

  const allowed = Deno.env.get("CORS_ALLOWED_ORIGINS")?.split(",") ?? [];
  return allowed.includes(origin);
}

export interface InternalAuthResult {
  authorized: boolean
  orgId?: string
  error?: string
}

export async function authenticateInternal(req: Request): Promise<InternalAuthResult> {
  const relayKey = req.headers.get("x-relay-key")
  const configuredRelayKey = Deno.env.get("RELAY_WEBHOOK_SECRET")
  const orgIdHeader = req.headers.get("x-org-id")

  if (!configuredRelayKey) {
    return { authorized: false, error: "RELAY_WEBHOOK_SECRET not configured" }
  }

  if (!relayKey) {
    return { authorized: false, error: "Missing x-relay-key header" }
  }

  if (!orgIdHeader) {
    return { authorized: false, error: "Missing x-org-id header" }
  }

  if (!isValidUUID(orgIdHeader)) {
    return { authorized: false, error: "Invalid x-org-id format" }
  }

  const isValid = await timingSafeEqual(relayKey, configuredRelayKey)
  if (!isValid) {
    return { authorized: false, error: "Invalid x-relay-key" }
  }

  return { authorized: true, orgId: orgIdHeader }
}

function isValidUUID(str: string): boolean {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
  return uuidRegex.test(str)
}

export async function hashRelayKey(key: string): Promise<string> {
  return await hashToken(key)
}

export function extractBearerToken(req: Request): string | null {
  const authHeader = req.headers.get("Authorization")
  if (!authHeader) {
    return null
  }

  const parts = authHeader.split(" ")
  if (parts.length !== 2 || parts[0] !== "Bearer") {
    return null
  }

  return parts[1]
}

export function extractAgentTokenHeader(req: Request): string | null {
  return req.headers.get("x-agent-token")
}
