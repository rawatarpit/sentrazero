import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  try {
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const { dataset_id, pipeline_template_id, created_by } = await req.json();
    if (!dataset_id) throw new Error("Missing dataset_id");
    if (!pipeline_template_id) throw new Error("Missing pipeline_template_id");
    const org_id = "b26fa5cc-4beb-43c7-aa3d-621b9cb353a3";
    const { data, error } = await supabase.rpc("activate_pipeline", {
      p_pipeline_template_id: pipeline_template_id,
      p_dataset_id: dataset_id,
      p_org_id: org_id,
      p_created_by: created_by ?? null
    });
    if (error) throw error;
    const advRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/advance_pipeline`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET") ?? "",
        "x-org-id": org_id
      },
      body: JSON.stringify({ execution_id: data.execution_id })
    });
    const advBody = await advRes.json();
    return new Response(JSON.stringify({
      ok: true,
      ...data,
      advance_pipeline: advBody
    }), {
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
