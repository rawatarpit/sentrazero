import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { extractBearerToken, timingSafeEqual } from "../_shared/security.ts";
async function validateInternalRequest(req) {
  const cronSecret = Deno.env.get("CRON_SECRET");
  if (!cronSecret) return false;
  const token = extractBearerToken(req);
  if (!token) return false;
  return await timingSafeEqual(token, cronSecret);
}
serve(async (req)=>{
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }
  if (!await validateInternalRequest(req)) {
    return jsonResponse({
      ok: false,
      error: "Unauthorized"
    }, 401, corsHeaders);
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    let p_org_id = null;
    if (req.method === "POST") {
      const body = await req.json().catch(()=>({}));
      p_org_id = body.org_id || null;
    }
    const { data, error } = await supabase.rpc("cleanup_stuck_jobs", {
      p_max_retries: 3,
      p_org_id
    });
    if (error) throw error;
    console.log("[cleanup_stuck_jobs]", JSON.stringify(data));
    return jsonResponse({
      ok: true,
      ...data
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[cleanup_stuck_jobs]", err.message);
    return jsonResponse({
      ok: false,
      error: err.message
    }, 500, corsHeaders);
  }
});
