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
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
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
    const { dataset_id, device_id, steps, pipeline_template_id, created_by } = await req.json();
    if (!dataset_id) {
      throw new Error("Missing dataset_id");
    }
    const { data: dataset, error: datasetErr } = await supabase.from("datasets").select("id, org_id").eq("id", dataset_id).single();
    if (datasetErr || !dataset) {
      throw new Error("Dataset not found");
    }
    if (dataset.org_id !== authResult.orgId) {
      throw new Error("Access denied: dataset belongs to different org");
    }
    await supabase.from("datasets").update({
      status: "processing"
    }).eq("id", dataset_id);
    const defaultSteps = [
      {
        type: "process",
        plugin_id: null,
        script_id: null,
        config: {}
      }
    ];
    const stepsToUse = steps && Array.isArray(steps) && steps.length > 0 ? steps : defaultSteps;
    const planRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/plan_dataset_chunks`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
        "x-org-id": authResult.orgId
      },
      body: JSON.stringify({
        dataset_id,
        steps: stepsToUse,
        device_id,
        pipeline_template_id,
        created_by
      })
    });
    if (!planRes.ok) {
      const text = await planRes.text();
      throw new Error(`plan_dataset_chunks failed (${planRes.status}): ${text}`);
    }
    const assignRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/auto_assign_best_device`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
        "x-org-id": authResult.orgId
      },
      body: JSON.stringify({
        org_id: authResult.orgId,
        dataset_id,
        job_type: "process"
      })
    });
    if (!assignRes.ok) {
      console.warn("[approve_dataset_and_plan_chunks] auto_assign_best_device failed, will be retried by dispatch cron");
    }
    return new Response(JSON.stringify({
      ok: true,
      dataset_id
    }), {
      headers: corsHeaders
    });
  } catch (err) {
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      status: 400,
      headers: corsHeaders
    });
  }
});
