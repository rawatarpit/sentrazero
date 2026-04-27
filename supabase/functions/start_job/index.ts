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

  console.log("[start_job] START");

  try {
    const authResult = await authenticateDevice(req);
    if (!authResult.device) {
      console.log("[start_job] auth failed:", authResult.error);
      return jsonResponse({ ok: false, error: authResult.error }, 401);
    }

    console.log("[start_job] device:", authResult.device.id);

    const body = await req.json();
    const jobId = body?.p_job_id || body?.job_id || "";

    console.log("[start_job] received job_id:", jobId);

    if (!jobId) {
      return jsonResponse({ ok: false, error: "job_id required" }, 400);
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    console.log("[start_job] calling RPC with job_id:", jobId);

    // Call the RPC with exact parameter names matching DB
    const result = await supabase.rpc("start_job", {
      p_job_id: jobId,
      p_agent_id: null
    });
    
    console.log("[start_job] RPC completed, result:", JSON.stringify(result));

    if (result.error) {
      console.error("[start_job] RPC error:", result.error.message);
      return jsonResponse({ ok: false, error: result.error.message }, 500);
    }

    if (!result.data) {
      console.log("[start_job] no data from RPC");
      // Still return success - update may have happened
      return jsonResponse({
        ok: true,
        job_id: jobId,
        status: "running"
      });
    }

    console.log("[start_job] returning:", result.data);
    return jsonResponse({
      ok: true,
      ...result.data
    });

  } catch (error) {
    console.error("[start_job] FATAL:", error);
    return jsonResponse({
      ok: false,
      error: String(error)
    }, 500);
  }
});