import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin, authenticateInternal } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";

function toVector(chunkIndex: number, totalChunks: number, chunkSizeGb: number, totalSizeGb: number): number[] {
  const vec = new Array(16).fill(0);
  vec[0] = chunkIndex / Math.max(totalChunks, 1);
  vec[1] = chunkSizeGb / 100;
  vec[2] = totalSizeGb / 1000;
  vec[3] = totalChunks > 50 ? 1 : 0;
  vec[4] = chunkSizeGb > 10 ? 1 : 0;
  const norm = Math.sqrt(vec.reduce((s, v) => s + v * v, 0));
  return norm > 0 ? vec.map(v => v / norm) : vec;
}

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);
  
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", { status: 403, headers: corsHeaders });
  }

  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const authResult = await authenticateInternal(req);
  if (!authResult.authorized) {
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
    const {
      dataset_id,
      device_id,
      steps,
      execution_id: existingExecutionId,
      step_index = 0,
      pipeline_template_id = null,
      created_by = null,
    } = await req.json();

    if (!dataset_id) throw new Error("Missing dataset_id");
    if (!Array.isArray(steps) || steps.length === 0) {
      throw new Error("Steps array is required");
    }
    if (step_index < 0 || step_index >= steps.length) {
      throw new Error("Invalid step_index");
    }

    const { data: existingChunks } = await supabase
      .from("batch_chunks")
      .select("id")
      .eq("dataset_id", dataset_id)
      .limit(1);

    if (existingChunks && existingChunks.length > 0) {
      return new Response(
        JSON.stringify({
          ok: true,
          message: "Chunks already exist for this dataset",
          idempotent: true,
        }),
        {
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }

    const { data: dataset, error: datasetErr } = await supabase
      .from("datasets")
      .select("id, org_id, total_size_gb, status")
      .eq("id", dataset_id)
      .single();

    if (datasetErr) throw datasetErr;

    if (dataset.org_id !== authResult.orgId) {
      throw new Error("Access denied: dataset belongs to different org");
    }

    if (dataset.status !== "scanned") {
      throw new Error("Dataset must be scanned before execution");
    }

    const { data: storageConfig } = await supabase
      .from("org_storage_configs")
      .select("storage_mode, bucket_name")
      .eq("org_id", dataset.org_id)
      .eq("is_default", true)
      .maybeSingle();

    const storageMode = storageConfig?.storage_mode ?? "shared_mount";

    if (storageMode !== "shared_mount" && !storageConfig?.bucket_name) {
      throw new Error(`Storage mode is '${storageMode}' but bucket_name is not configured. Please configure S3 storage in org settings.`);
    }

    let chunkSizeGb = 2;

    if (device_id) {
      const { data: device } = await supabase
        .from("devices")
        .select("preferred_chunk_size_gb")
        .eq("id", device_id)
        .single();

      if (device?.preferred_chunk_size_gb) {
        chunkSizeGb = device.preferred_chunk_size_gb;
      }
    }

    const numChunks = Math.ceil(
      dataset.total_size_gb / chunkSizeGb
    );

    const execution_id =
      existingExecutionId ?? crypto.randomUUID();

    if (!existingExecutionId) {
      const { error: execErr } = await supabase
        .from("executions")
        .insert({
          id: execution_id,
          org_id: dataset.org_id,
          dataset_id,
          pipeline_template_id,
          total_steps: steps.length,
          current_step_index: step_index,
          created_by,
        });

      if (execErr) throw execErr;
    } else {
      await supabase
        .from("executions")
        .update({ current_step_index: step_index })
        .eq("id", execution_id)
        .eq("org_id", authResult.orgId);
    }

    const step = steps[step_index];

    await supabase
      .from("execution_steps")
      .upsert({
        execution_id,
        step_index,
        step_type: step.type,
        plugin_id: step.plugin_id ?? null,
        script_id: step.script_id ?? null,
        config: step.config ?? {},
        status: "running",
      });

    const chunks = [];

    for (let chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
      chunks.push({
        dataset_id,
        org_id: dataset.org_id,
        chunk_index: chunkIndex,
        status: "pending",
        job_type: step.type,
        chunk_size_gb: chunkSizeGb,
        chunk_vector: toVector(chunkIndex, numChunks, chunkSizeGb, dataset.total_size_gb),
        payload: {
          execution_id,
          step_index,
          step_type: step.type,
          plugin_id: step.plugin_id ?? null,
          script_id: step.script_id ?? null,
          config: step.config ?? {},
          storage_mode: storageMode,
        },
      });
    }

    const { error: insertErr } = await supabase
      .from("batch_chunks")
      .insert(chunks);

    if (insertErr) throw insertErr;

    return new Response(
      JSON.stringify({
        ok: true,
        execution_id,
        step_index,
        chunks_created: numChunks,
        storage_mode: storageMode,
      }),
      {
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  } catch (err) {
    return new Response(
      JSON.stringify({
        ok: false,
        error: err.message,
      }),
      {
        status: 400,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  }
});
