import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);
  
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", { status: 403, headers: corsHeaders });
  }

  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const authResult = await authenticateDeviceWithDetails(req, "id, org_id");
  if (!authResult.device) {
    return new Response(
      JSON.stringify({ ok: false, error: authResult.error }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    const body = await req.json();
    const { dataset_id, total_size_gb, file_count, avg_file_size_mb, file_type, steps, device_id, pipeline_template_id, created_by } = body;

    if (!dataset_id) throw new Error("Missing dataset_id");

    const { data: dataset, error: datasetErr } = await supabase
      .from("datasets")
      .select("id, org_id")
      .eq("id", dataset_id)
      .single();

    if (datasetErr) throw new Error("Dataset not found");

    if (dataset.org_id !== authResult.device.org_id) {
      throw new Error("Access denied: dataset belongs to different org");
    }

    const { error } = await supabase
      .from("datasets")
      .update({
        total_size_gb,
        file_count,
        avg_file_size_mb,
        file_type,
        status: "scanned",
        scanned_at: new Date().toISOString(),
      })
      .eq("id", dataset_id)
      .eq("org_id", dataset.org_id);

    if (error) throw error;

    const defaultSteps = [{ type: "process", plugin_id: null, script_id: null, config: {} }];
    const stepsToUse = (steps && Array.isArray(steps) && steps.length > 0) ? steps : defaultSteps;

    const planRes = await fetch(
      `${Deno.env.get("SUPABASE_URL")}/functions/v1/plan_dataset_chunks`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET")!,
          "x-org-id": dataset.org_id,
        },
        body: JSON.stringify({
          dataset_id,
          steps: stepsToUse,
          device_id,
          pipeline_template_id,
          created_by,
        }),
      }
    );

    if (!planRes.ok) {
      const text = await planRes.text();
      throw new Error(`plan_dataset_chunks failed (${planRes.status}): ${text}`);
    }

    const planData = await planRes.json();

    return new Response(
      JSON.stringify({
        ok: true,
        message: "Dataset metadata recorded",
        plan_result: planData,
      }),
      { headers: corsHeaders }
    );
  } catch (err) {
    console.error("[record_dataset_metadata] ❌", err);
    return new Response(JSON.stringify({ ok: false, error: err.message }), {
      headers: corsHeaders,
      status: 400,
    });
  }
});
