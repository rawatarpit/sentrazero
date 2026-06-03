import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type"
};
function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    },
    status
  });
}
serve(async (req)=>{
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: corsHeaders
    });
  }
  try {
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const { execution_id } = await req.json();
    if (!execution_id) {
      return jsonResponse({
        ok: false,
        error: "Missing execution_id"
      }, 400);
    }
    const { data: execution } = await supabase.from("executions").select("id, org_id, dataset_id, status, current_step_index, total_steps").eq("id", execution_id).single();
    if (!execution) {
      return jsonResponse({
        ok: false,
        error: "Execution not found"
      }, 404);
    }
    if (execution.status === "failed") {
      await supabase.from("executions").update({
        status: "running"
      }).eq("id", execution_id);
    }
    if (execution.status !== "running" && execution.status !== "failed") {
      return jsonResponse({
        ok: true,
        execution_id,
        status: execution.status,
        message: "Not running"
      });
    }
    const stepIndex = execution.current_step_index;
    const { data: stepLevelJobs } = await supabase.from("agent_jobs").select("id, payload").eq("execution_id", execution_id).eq("job_type", "process");
    const stepLevelJobsCompleted = (stepLevelJobs || []).filter((j)=>j.payload && !j.payload.chunk_id && j.payload.step_index === stepIndex);
    const { data: stepLevelJob } = await supabase.from("agent_jobs").select("id").eq("execution_id", execution_id).eq("job_type", "process").is("payload->>chunk_id", null).limit(1);
    const hasStepLevelJob = stepLevelJob && stepLevelJob.length > 0;
    if (stepIndex === 0) {
      const { data: existingChunks } = await supabase.from("batch_chunks").select("id, status").eq("dataset_id", execution.dataset_id).eq("payload->>step_index", "0").limit(1);
      if (!existingChunks || existingChunks.length === 0) {
        console.log("[advance_pipeline] Step 0: Creating chunks via plan_dataset_chunks");
        const { data: stepDefs } = await supabase.from("execution_steps").select("step_index, step_type, plugin_id, script_id, config").eq("execution_id", execution_id).order("step_index");
        const plannerRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/plan_dataset_chunks`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            dataset_id: execution.dataset_id,
            execution_id,
            step_index: 0,
            steps: stepDefs || []
          })
        });
        if (!plannerRes.ok) {
          const text = await plannerRes.text();
          throw new Error(`plan_dataset_chunks failed: ${text}`);
        }
        const planBody = await plannerRes.json();
        if (planBody.chunks_created === 0) {
          // Chunks already existed from a prior call — re-check on next pass
          console.log("[advance_pipeline] plan_dataset_chunks returned 0 chunks created (already exist)");
        } else {
          return jsonResponse({
            ok: true,
            execution_id,
            step_index: 0,
            message: "Chunks created, waiting for processing"
          });
        }
      }
    }
    // For any step, check if chunk-level jobs exist and are done before advancing
    const { data: chunkJobs } = await supabase.from("agent_jobs").select("status").eq("execution_id", execution_id).eq("job_type", "process").not("payload->>chunk_id", "is", null).eq("payload->>step_index", stepIndex);
    const hasChunkJobs = (chunkJobs || []).length > 0;
    const allChunkJobsDone = hasChunkJobs && (chunkJobs || []).every(j => j.status === "completed" || j.status === "failed");
    if (hasChunkJobs && !allChunkJobsDone) {
      return jsonResponse({
        ok: true,
        execution_id,
        step_index: stepIndex,
        message: "Waiting for chunk processing to complete"
      });
    }
    await supabase.from("execution_steps").update({
      status: "completed",
      completed_at: new Date().toISOString()
    }).eq("execution_id", execution_id).eq("step_index", stepIndex);
    const nextStepIndex = stepIndex + 1;
    if (nextStepIndex >= execution.total_steps) {
      await supabase.from("executions").update({
        status: "completed",
        completed_at: new Date().toISOString()
      }).eq("id", execution_id);
      console.log("[advance_pipeline] All steps complete, scheduling merge job for dataset:", execution.dataset_id);
      const mergeRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/schedule_merge_job`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
          "x-org-id": execution.org_id
        },
        body: JSON.stringify({
          dataset_id: execution.dataset_id,
          execution_id: execution_id
        })
      });
      const mergeBody = await mergeRes.json();
      console.log("[advance_pipeline] schedule_merge_job result:", JSON.stringify(mergeBody));
      if (!mergeRes.ok || !mergeBody?.ok) {
        console.error("[advance_pipeline] schedule_merge_job failed:", mergeBody?.error || mergeBody);
        await supabase.from("datasets").update({
          status: "merge_pending",
          updated_at: new Date().toISOString()
        }).eq("id", execution.dataset_id);
      }
      return jsonResponse({
        ok: true,
        execution_id,
        message: "Execution completed, merge scheduled",
        merge_result: mergeBody
      });
    }
    await supabase.from("executions").update({
      current_step_index: nextStepIndex
    }).eq("id", execution_id);
    const { data: stepDefs } = await supabase.from("execution_steps").select("step_index, step_type, plugin_id, script_id, config").eq("execution_id", execution_id).order("step_index");
    const plannerRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/plan_dataset_chunks`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        dataset_id: execution.dataset_id,
        execution_id,
        step_index: nextStepIndex,
        steps: stepDefs || []
      })
    });
    if (!plannerRes.ok) {
      const text = await plannerRes.text();
      throw new Error(`plan_dataset_chunks failed: ${text}`);
    }
    return jsonResponse({
      ok: true,
      execution_id,
      step_index: nextStepIndex,
      message: "Advanced to next step"
    });
  } catch (err) {
    console.error("[advance_pipeline] error:", err);
    return jsonResponse({
      ok: false,
      error: err.message
    }, 500);
  }
});
