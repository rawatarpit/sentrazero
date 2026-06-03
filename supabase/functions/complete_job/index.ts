import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type, x-device-id"
};
function jsonResponse(payload, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders,
      ...extraHeaders
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
  console.log("[complete_job] === START ===");
  try {
    // --- Auth using shared module ---
    const authResult = await authenticateDeviceWithDetails(req);
    if (!authResult.device) {
      console.log("[complete_job] auth failed:", authResult.error);
      return jsonResponse({
        ok: false,
        error: authResult.error
      }, 401);
    }
    console.log("[complete_job] device:", authResult.device.id);
    // --- Parse body ---
    const body = await req.json();
    const { execution_id, status, duration_ms, output, error, job_id } = body;
    console.log("[complete_job] input:", {
      job_id,
      execution_id,
      status
    });
    if (!job_id && !execution_id) {
      return jsonResponse({
        ok: false,
        error: "Missing id"
      }, 400);
    }
    // --- Create service role client ---
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    // --- Build update data ---
    const jobUpdateData = {
      status: status === "completed" ? "completed" : "failed",
      updated_at: new Date().toISOString(),
      last_transition_at: new Date().toISOString()
    };
    if (duration_ms) {
      jobUpdateData.duration_ms = duration_ms;
      jobUpdateData.finished_at = new Date().toISOString();
    }
    if (output) {
      jobUpdateData.output_token = JSON.stringify(output);
    }
    if (error) {
      jobUpdateData.error = error;
      jobUpdateData.last_error = error;
    }
    if (status === "completed") {
      jobUpdateData.completed = true;
    }
    let execId = execution_id;
    // --- Update agent_jobs ---
    if (job_id) {
      console.log("[complete_job] processing job:", job_id);
      const { data: currJob } = await supabase.from("agent_jobs").select("status, execution_id, payload").eq("id", job_id).maybeSingle();
      if (currJob?.status === "completed" || currJob?.status === "failed") {
        console.log("[complete_job] job already in terminal state, triggering advance_pipeline");
        const termExecId = execution_id || currJob?.execution_id;
        if (termExecId) {
          try {
            const advRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/advance_pipeline`, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
                "x-org-id": authResult.device.org_id
              },
              body: JSON.stringify({
                execution_id: termExecId
              })
            });
            console.log("[complete_job] advance_pipeline result (idempotent):", await advRes.text());
          } catch (e) {
            console.log("[complete_job] advance_pipeline call failed (non-fatal):", e.message);
          }
        }
        return jsonResponse({
          ok: true,
          job_id,
          execution_id: termExecId,
          status: currJob.status,
          idempotent: true
        });
      }
      const { data: jobWithPayload } = await supabase.from("agent_jobs").select("status, execution_id, payload").eq("id", job_id).maybeSingle();
      // Update job (no .select() to avoid issues)
      const { error: updateErr, count } = await supabase.from("agent_jobs").update(jobUpdateData).eq("id", job_id).select("id", {
        count: "exact",
        head: true
      });
      console.log("[complete_job] job update result:", {
        error: updateErr,
        rowsUpdated: count
      });
      if (updateErr) {
        console.error("[complete_job] job update FAILED:", updateErr.message);
      } else {
        console.log("[complete_job] JOB UPDATED SUCCESSFULLY, rows:", count);
        if (!execId && jobWithPayload?.execution_id) {
          execId = jobWithPayload.execution_id;
        }
        if (status === "completed") {
          const chunkId = jobWithPayload?.payload?.chunk_id;
          if (chunkId) {
            const { error: chunkErr } = await supabase.from("batch_chunks").update({
              status: "processed",
              updated_at: new Date().toISOString()
            }).eq("id", chunkId);
            if (chunkErr) {
              console.error("[complete_job] batch_chunk update failed:", chunkErr.message);
            } else {
              console.log("[complete_job] batch_chunk", chunkId, "marked processed");
            }
          }
        }
        if (status === "completed" && jobWithPayload?.payload?.job_type === "merge_dataset") {
          const { error: dsErr } = await supabase.from("datasets").update({
            status: "merged",
            merged_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          }).eq("id", jobWithPayload.payload.dataset_id);
          if (dsErr) {
            console.error("[complete_job] dataset merge update failed:", dsErr.message);
          } else {
            console.log("[complete_job] dataset", jobWithPayload.payload.dataset_id, "marked merged");
          }
        }
      }
    }
    // --- Update executions ---
    if (execId) {
      console.log("[complete_job] processing execution:", execId);
      const execUpdate = {
        completed_at: new Date().toISOString()
      };
      if (status === "failed") {
        execUpdate.status = "failed";
      }
      const { error: execErr } = await supabase.from("executions").update(execUpdate).eq("id", execId);
      if (execErr) {
        console.error("[complete_job] execution update FAILED:", execErr.message);
      } else {
        console.log("[complete_job] EXECUTION UPDATED SUCCESSFULLY");
      }
    }
    // --- Advance pipeline if this job belongs to an execution ---
    if (execId) {
      try {
        console.log("[complete_job] calling advance_pipeline for execution:", execId);
        const advRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/advance_pipeline`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
            "x-org-id": authResult.device.org_id
          },
          body: JSON.stringify({
            execution_id: execId
          })
        });
        const advBody = await advRes.json();
        console.log("[complete_job] advance_pipeline result:", JSON.stringify(advBody));
      } catch (e) {
        console.log("[complete_job] advance_pipeline call failed (non-fatal):", e.message);
      }
    }
    // --- Return success ---
    console.log("[complete_job] === END (SUCCESS) ===");
    return jsonResponse({
      ok: true,
      job_id,
      execution_id: execId,
      status
    });
  } catch (error) {
    console.error("[complete_job] FATAL:", error);
    return jsonResponse({
      ok: false,
      error: String(error)
    }, 500);
  }
});
