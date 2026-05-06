import { createClient } from "npm:@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";
Deno.serve(async (req)=>{
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }
  const start = Date.now();
  try {
    const authResult = await authenticateDevice(req);
    if (!authResult.device) {
      return jsonResponse({
        ok: false,
        error: authResult.error
      }, 401, corsHeaders);
    }
    const device = authResult.device;
    const org_id = device.org_id;
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
      auth: {
        persistSession: false
      },
      global: {
        headers: {
          "x-client-info": "kickin-edge-assign-agent-job/1.0.0"
        }
      }
    });
    const { data, error } = await supabase.rpc("assign_agent_job", {
      _org_id: org_id
    });
    if (error) {
      console.error("[assign_agent_job] RPC error:", error);
      await logError(supabase, org_id, "assign_agent_job", error.message);
      return jsonResponse({
        ok: false,
        error: error.message
      }, 500, corsHeaders);
    }
    if (!data || data.ok === false) {
      console.warn("[assign_agent_job] No available device or pending jobs:", data);
      return jsonResponse({
        ok: true,
        message: data?.error || "no_available_device_or_jobs",
        timestamp: new Date().toISOString()
      }, 200, corsHeaders);
    }
    console.log("[assign_agent_job] Job assigned:", data);
    return jsonResponse({
      ok: true,
      result: data,
      exec_time_ms: Date.now() - start
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[assign_agent_job] Unexpected error:", err);
    return jsonResponse({
      ok: false,
      error: err.message
    }, 500, corsHeaders);
  }
});
async function logError(supabase, org_id, source, message) {
  try {
    await supabase.from("system_logs").insert([
      {
        event_type: "edge_error",
        message: `[${source}] ${message}`,
        org_id,
        created_at: new Date().toISOString()
      }
    ]);
  } catch (logErr) {
    console.error("[assign_agent_job] Failed to log error:", logErr);
  }
}
