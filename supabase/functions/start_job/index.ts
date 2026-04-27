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
      return jsonResponse({ success: false, error: authResult.error }, 401);
    }

    const body = await req.json();
    const { p_job_id, p_agent_id } = body;

    if (!p_job_id) {
      return jsonResponse({ success: false, error: "p_job_id required" }, 400);
    }

    console.log("[start_job] job_id:", p_job_id);

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    console.log("[start_job] calling RPC...");

    // Call the RPC
    const { data, error } = await supabase.rpc("start_job", {
      p_job_id: p_job_id,
      p_agent_id: p_agent_id || null,
    });

    console.log("[start_job] RPC result:", data, "error:", error);

    if (error) {
      console.error("[start_job] RPC error:", error.message);
      return jsonResponse({
        success: false,
        error: error.message,
      }, 500);
    }

    // Handle case where data might be null/undefined
    if (!data) {
      console.log("[start_job] no data returned, treating as success");
      return jsonResponse({
        success: true,
        job_id: p_job_id,
        status: "running",
      });
    }

    console.log("[start_job] result:", data);
    return jsonResponse(data);

  } catch (error) {
    console.error("[start_job] FATAL:", error);
    return jsonResponse({
      success: false,
      error: String(error),
    }, 500);
  }
});