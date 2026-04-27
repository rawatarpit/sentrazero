import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { authenticateDevice } from "../_shared/auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-client-info, apikey, content-type",
};

function jsonResponse(
  payload: Record<string, unknown>,
  status = 200
): Response {
  return new Response(JSON.stringify(payload), {
    headers: { "Content-Type": "application/json", ...corsHeaders },
    status,
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  console.log("[start_job] START - checking auth...");

  try {
    console.log("[start_job] Calling authenticateDevice...");
    const authResult = await authenticateDevice(req);
    console.log("[start_job] Auth result:", JSON.stringify(authResult));
    
    if (!authResult.device) {
      console.log("[start_job] Auth failed - returning 401");
      return jsonResponse({ ok: false, error: authResult.error }, 401);
    }

    console.log("[start_job] Auth success, device:", authResult.device.id);

    // Get job_id from request body
    let body;
    try {
      body = await req.json();
      console.log("[start_job] Parsed body:", JSON.stringify(body));
    } catch (e) {
      console.log("[start_job] Failed to parse body:", e);
      body = {};
    }
    
    const jobId = body?.p_job_id || body?.job_id || "";

    console.log("[start_job] Extracted job_id:", jobId);

    if (!jobId) {
      return jsonResponse({ ok: false, error: "job_id required" }, 400);
    }

    // Create Supabase client with service role
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    
    console.log("[start_job] SUPABASE_URL:", supabaseUrl);
    console.log("[start_job] Has SERVICE_KEY:", !!supabaseKey);
    
    if (!supabaseUrl || !supabaseKey) {
      return jsonResponse({ ok: false, error: "Missing env vars" }, 500);
    }

    const supabase = createClient(supabaseUrl, supabaseKey);
    console.log("[start_job] Supabase client created");

    // Call the RPC
    console.log("[start_job] Calling RPC start_job...");
    const result = await supabase.rpc("start_job", {
      p_job_id: jobId,
      p_agent_id: null
    });
    
    console.log("[start_job] RPC result:", JSON.stringify(result));

    if (result.error) {
      console.error("[start_job] RPC error:", result.error.message);
      return jsonResponse({ ok: false, error: result.error.message }, 500);
    }

    if (!result.data) {
      console.log("[start_job] No data from RPC - returning success");
      return jsonResponse({
        ok: true,
        job_id: jobId,
        status: "running"
      });
    }

    console.log("[start_job] Final result:", result.data);
    return jsonResponse({
      ok: true,
      ...result.data
    });

  } catch (error) {
    console.error("[start_job] FATAL ERROR:", error);
    console.error("[start_job] Stack:", error.stack);
    return jsonResponse({
      ok: false,
      error: String(error)
    }, 500);
  }
});