import { createClient } from "https://esm.sh/@supabase/supabase-js@2"
import { hashToken, validateOrigin } from "./security.ts"

const ALLOWED_DEVICE_STATUSES = ['online', 'available', 'busy']
const REQUIRED_AUTH_FIELDS = ["id", "org_id", "status", "revoked_at"]

export interface AuthDevice {
  id: string
  org_id: string
  name?: string
  status?: string
  total_cpu_cores?: number
  total_memory_gb?: number
  max_concurrency?: number
  last_policy_update?: string
  revoked_at?: string | null
}

export interface AuthResult {
  device?: AuthDevice
  error?: string
}

function extractAgentToken(req: Request): { token: string | null; error: string | null } {
  const token = req.headers.get("x-agent-token")

  if (!token) {
    return { token: null, error: "Missing x-agent-token header" }
  }

  if (token.trim() === "") {
    return { token: null, error: "x-agent-token cannot be empty" }
  }

  return { token, error: null }
}

async function validateAndGetDevice(
  supabase: ReturnType<typeof createClient>,
  hashedToken: string,
  selectFields: string = "id, org_id, name, status, revoked_at"
): Promise<{ device: AuthDevice | null; error: string | null }> {
  const fieldSet = new Set([
    ...selectFields.split(",").map((f) => f.trim()),
    ...REQUIRED_AUTH_FIELDS,
  ])
  const finalSelect = Array.from(fieldSet).join(", ")

  const { data: device, error: dbError } = await supabase
    .from("devices")
    .select(finalSelect)
    .eq("access_token_hash", hashedToken)
    .maybeSingle()

  console.log("[auth] Query result:", { 
    hasData: !!device, 
    hasError: !!dbError,
    errorCode: dbError?.code,
    device: device ? { id: device.id, status: device.status, revoked_at: device.revoked_at } : null
  })

  if (dbError) {
    console.error("[auth] Database error:", dbError.message, dbError.code)
    return { device: null, error: "Auth query failed" }
  }

  if (!device) {
    console.warn("[auth] No device found for token hash:", hashedToken.substring(0, 8) + "...")
    return { device: null, error: "Unauthorized — invalid token" }
  }

  if (device.revoked_at !== null && device.revoked_at !== undefined) {
    console.warn("[auth] Device revoked:", device.id)
    return { device: null, error: "Device has been revoked" }
  }

  if (device.status === null || device.status === undefined) {
    console.error("[auth] CRITICAL: Device has null/undefined status - DB corruption!", device.id)
    return { device: null, error: "Device missing status (DB corruption)" }
  }

  if (typeof device.status !== 'string') {
    console.error("[auth] CRITICAL: Device status is not a string!", device.id, typeof device.status)
    return { device: null, error: "Device status has invalid type (DB corruption)" }
  }

  if (!ALLOWED_DEVICE_STATUSES.includes(device.status)) {
    console.warn("[auth] Device not operational:", device.id, "status:", device.status)
    return { device: null, error: `Device is not operational (status: ${device.status})` }
  }

  console.log("[auth] Device validated:", device.id, "status:", device.status)
  return { device: device as AuthDevice, error: null }
}

async function updateLastSeen(
  supabase: ReturnType<typeof createClient>,
  deviceId: string
): Promise<void> {
  try {
    await supabase
      .from("devices")
      .update({
        last_seen: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq("id", deviceId)
  } catch (err) {
    console.error("[auth] Failed to update last_seen:", err)
  }
}

export async function authenticateDevice(req: Request): Promise<AuthResult> {
  const origin = req.headers.get("origin")
  if (origin && !validateOrigin(req)) {
    return { error: "Forbidden" }
  }

  const { token, error: tokenError } = extractAgentToken(req)
  if (tokenError) {
    return { error: tokenError }
  }

  const hashedToken = await hashToken(token!)
  console.log("[auth] Token hashed, length:", hashedToken.length)

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  )

  const { device, error } = await validateAndGetDevice(supabase, hashedToken)

  if (error || !device) {
    return { error: error || "Authentication failed" }
  }

  await updateLastSeen(supabase, device.id)

  return { device }
}

export async function authenticateDeviceWithDetails(
  req: Request,
  selectFields?: string
): Promise<AuthResult> {
  const origin = req.headers.get("origin")
  if (origin && !validateOrigin(req)) {
    return { error: "Forbidden" }
  }

  const { token, error: tokenError } = extractAgentToken(req)
  if (tokenError) {
    return { error: tokenError }
  }

  const hashedToken = await hashToken(token!)
  console.log("[auth] Token hashed for authWithDetails, length:", hashedToken.length)

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  )

  const select = selectFields || "id, org_id, name, status, revoked_at"
  const { device, error } = await validateAndGetDevice(supabase, hashedToken, select)

  if (error || !device) {
    return { error: error || "Authentication failed" }
  }

  await updateLastSeen(supabase, device.id)

  return { device }
}

export async function requireAuth(req: Request): Promise<AuthDevice> {
  const result = await authenticateDevice(req)

  if (!result.device) {
    throw new Error(result.error || "Unauthorized")
  }

  return result.device
}

export async function requireAuthWithDetails(
  req: Request,
  selectFields?: string
): Promise<AuthDevice> {
  const result = await authenticateDeviceWithDetails(req, selectFields)

  if (!result.device) {
    throw new Error(result.error || "Unauthorized")
  }

  return result.device
}
