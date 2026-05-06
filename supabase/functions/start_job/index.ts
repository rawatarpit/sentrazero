import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { authenticateDevice } from "../_shared/auth.ts";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type, x-device-id"
};
function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    },
    status
  });
}
serve(async (req)=>{
  // Skip Supabase JWT verification by NOT using the Authorization header for Supabase auth
  // Our custom auth uses x-agent-token header instead
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: corsHeaders
    });
  }
  try {
    const authResult = await authenticateDevice(req);
    if (!authResult.device) {
      console.error("[start_job] Auth failed:", authResult.error);
      return jsonResponse({
        ok: false,
        error: authResult.error
      }, 401);
    }
    const body = await req.json();
    const jobId = body?.p_job_id || body?.job_id || "";
    if (!jobId) {
      return jsonResponse({
        ok: false,
        error: "job_id required"
      }, 400);
    }
    // Use service role key but call via REST API directly to bypass Supabase edge function JWT check
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    console.log("[start_job] Calling RPC with jobId:", jobId, "agent:", authResult.device.id);
    // Call RPC directly via fetch to avoid Supabase client JWT issues
    const url = `${supabaseUrl}/rest/v1/rpc/start_job`;
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "apikey": serviceKey,
        "Authorization": `Bearer ${serviceKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        p_job_id: jobId,
        p_agent_id: authResult.device.id
      })
    });
    if (!response.ok) {
      const errorText = await response.text();
      console.error("[start_job] RPC HTTP error:", response.status, errorText);
      return jsonResponse({
        ok: false,
        error: errorText
      }, 500);
    }
    const data = await response.json();
    console.log("[start_job] RPC success:", data);
    if (!data || data.length === 0) {
      return jsonResponse({
        ok: true,
        job_id: jobId,
        status: "running"
      });
    }
    return jsonResponse({
      ok: data[0].success !== false,
      job_id: data[0].job_id || jobId,
      status: data[0].status || "running",
      started_at: data[0].started_at,
      error: data[0].error
    });
  } catch (error) {
    console.error("[start_job] FATAL:", error);
    return jsonResponse({
      ok: false,
      error: String(error)
    }, 500);
  }
});
