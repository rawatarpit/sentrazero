import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { hashToken } from "../_shared/security.ts";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
export async function verifyAgentToken(req) {
  const authHeader = req.headers.get("Authorization");
  const agentToken = req.headers.get("x-agent-token");
  if (!authHeader) {
    return {
      error: "Missing Authorization header"
    };
  }
  if (!agentToken) {
    return {
      error: "Missing x-agent-token header"
    };
  }
  const tokenHash = await hashToken(agentToken);
  const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);
  const { data: device, error: dbError } = await supabase.from("devices").select("id, org_id, name, status, revoked_at").eq("access_token_hash", tokenHash).maybeSingle();
  console.log("[verifyAgentToken] Query result:", {
    hasData: !!device,
    hasError: !!dbError,
    errorCode: dbError?.code
  });
  if (dbError) {
    console.error("[verifyAgentToken] Database error:", dbError.message, dbError.code);
    return {
      error: "Auth query failed"
    };
  }
  if (!device) {
    return {
      error: "Unauthorized — invalid or expired token"
    };
  }
  if (device.revoked_at !== null && device.revoked_at !== undefined) {
    return {
      error: "Device has been revoked"
    };
  }
  if (device.status === null || device.status === undefined) {
    console.error("[verifyAgentToken] CRITICAL: Device has null status:", device.id);
    return {
      error: "Device missing status (DB corruption)"
    };
  }
  return {
    device
  };
}
