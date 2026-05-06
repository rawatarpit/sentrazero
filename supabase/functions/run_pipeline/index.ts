import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin, authenticateInternal } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", {
      status: 403,
      headers: corsHeaders
    });
  }
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  const authResult = await authenticateInternal(req);
  if (!authResult.authorized) {
    return new Response(JSON.stringify({
      ok: false,
      error: authResult.error
    }), {
      status: 401,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const { dataset_id, pipeline_template_id, created_by } = await req.json();
    if (!dataset_id) throw new Error("Missing dataset_id");
    if (!pipeline_template_id) throw new Error("Missing pipeline_template_id");
    // Call the database function to activate pipeline
    const { data, error } = await supabase.rpc("activate_pipeline", {
      p_pipeline_template_id: pipeline_template_id,
      p_dataset_id: dataset_id,
      p_org_id: authResult.orgId,
      p_created_by: created_by ?? null
    });
    if (error) throw error;
    return new Response(JSON.stringify(data), {
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      status: 400,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders
      }
    });
  }
});
