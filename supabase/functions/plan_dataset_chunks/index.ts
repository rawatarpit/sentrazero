import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
function toVector(chunkIndex, totalChunks, chunkSizeGb) {
  const vec = new Array(16).fill(0);
  vec[0] = chunkIndex / Math.max(totalChunks, 1);
  vec[1] = chunkSizeGb / 100;
  vec[3] = totalChunks > 50 ? 1 : 0;
  vec[4] = chunkSizeGb > 10 ? 1 : 0;
  const norm = Math.sqrt(vec.reduce((s, v)=>s + v * v, 0));
  return norm > 0 ? vec.map((v)=>v / norm) : vec;
}
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  try {
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const { dataset_id, device_id, steps, execution_id: existingExecutionId, step_index = 0, pipeline_template_id = null, created_by = null } = await req.json();
    if (!dataset_id) throw new Error("Missing dataset_id");
    if (!Array.isArray(steps) || steps.length === 0) throw new Error("Steps array required");
    const { data: dataset } = await supabase.from("datasets").select("org_id, total_size_gb, status").eq("id", dataset_id).single();
    if (!dataset) throw new Error("Dataset not found");
    const org_id = dataset.org_id;
    const totalSizeGb = dataset.total_size_gb || 0.001;
    const step = steps[step_index];
    if (!step) throw new Error("Step not found");
    const plugin_id = step.plugin_id;
    const config = step.config || {};
    const chunkSizeGb = 0.001;
    const totalChunks = 1;
    const chunkVector = toVector(0, 1, chunkSizeGb);
    const { data: chunk, error: chunkError } = await supabase.from("batch_chunks").insert({
      dataset_id,
      org_id,
      chunk_index: 0,
      status: "pending",
      job_type: "process",
      chunk_size_gb: chunkSizeGb,
      chunk_vector: chunkVector,
      payload: {
        step_index,
        plugin_id,
        config,
        execution_id: existingExecutionId,
        dataset_id,
        org_id
      }
    }).select().single();
    if (chunkError) {
      if (chunkError.code === "23505") {
        return new Response(JSON.stringify({
          ok: true,
          message: "Chunks already exist",
          chunks_created: 0
        }), {
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders
          }
        });
      }
      throw new Error("Chunk insert failed: " + chunkError.message);
    }
    await supabase.from("datasets").update({
      status: "chunked"
    }).eq("id", dataset_id);
    const storageMode = "object_storage";
    const { data: job, error: jobError } = await supabase.from("agent_jobs").insert({
      org_id,
      job_type: "process",
      status: "pending",
      execution_id: existingExecutionId,
      payload: {
        chunk_id: chunk.id,
        chunk_index: 0,
        dataset_id,
        plugin_id,
        step_index,
        config,
        execution_id: existingExecutionId,
        storage_mode: storageMode
      }
    }).select().single();
    if (jobError) throw new Error("Job insert failed: " + jobError.message);
    return new Response(JSON.stringify({
      ok: true,
      chunks_created: 1,
      job_id: job.id,
      chunk_id: chunk.id,
      execution_id: existingExecutionId
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
