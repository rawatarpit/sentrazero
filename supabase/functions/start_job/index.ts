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
      return jsonResponse({ ok: false, error: authResult.error }, 401);
    }

    let body;
    const contentType = req.headers.get("content-type") || "";
    
    if (contentType.includes("application/json")) {
      try {
        body = await req.json();
      } catch (e) {
        body = {};
      }
    }
    
    const p_job_id = body?.p_job_id || body?.job_id || "";
    const p_agent_id = body?.p_agent_id || body?.agent_id || null;

    console.log("[start_job] body:", JSON.stringify(body));
    console.log("[start_job] job_id:", p_job_id);

    if (!p_job_id) {
      return jsonResponse({ ok: false, error: "p_job_id required" }, 400);
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    console.log("[start_job] calling RPC for job:", p_job_id);

    // Call the RPC
    const { data, error } = await supabase.rpc("start_job", {
      p_job_id: p_job_id,
      p_agent_id: p_agent_id,
    });

    console.log("[start_job] RPC done, data:", data, "error:", error);

    if (error) {
      console.error("[start_job] RPC error:", error.message);
      return jsonResponse({
        ok: false,
        error: error.message,
      }, 500);
    }

    console.log("[start_job] success, data:", data);
    return jsonResponse({
      ok: true,
      ...data,
    });

  } catch (error) {
    console.error("[start_job] FATAL:", error);
    return jsonResponse({
      ok: false,
      error: String(error),
    }, 500);
  }
});