import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type, x-device-id",
};

function jsonResponse(
  payload: Record<string, unknown>, 
  status = 200, 
  extraHeaders: Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders,
      ...extraHeaders,
    },
    status,
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  console.log("[complete_job] === START ===");

  try {
    // --- Auth using shared module ---
    const authResult = await authenticateDeviceWithDetails(req);
    if (!authResult.device) {
      console.log("[complete_job] auth failed:", authResult.error);
      return jsonResponse({ ok: false, error: authResult.error }, 401);
    }
    console.log("[complete_job] device:", authResult.device.id);

    // --- Parse body ---
    const body = await req.json();
    const { execution_id, status, duration_ms, output, error, job_id } = body;
    console.log("[complete_job] input:", { job_id, execution_id, status });

    if (!job_id && !execution_id) {
      return jsonResponse({ ok: false, error: "Missing id" }, 400);
    }

    // --- Create service role client ---
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // --- Build update data ---
    const jobUpdateData: Record<string, unknown> = {
      status: status === "completed" ? "completed" : "failed",
      updated_at: new Date().toISOString(),
      last_transition_at: new Date().toISOString(),
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
      
      // Check if already completed (idempotent)
      const { data: currJob } = await supabase
        .from("agent_jobs")
        .select("status, execution_id")
        .eq("id", job_id)
        .maybeSingle();

      if (currJob?.status === "completed" || currJob?.status === "failed") {
        return jsonResponse({ 
          ok: true, 
          job_id, 
          execution_id: execution_id || currJob?.execution_id,
          status: currJob.status,
          idempotent: true 
        });
      }

      // Update job (no .select() to avoid issues)
      const { error: updateErr, count } = await supabase
        .from("agent_jobs")
        .update(jobUpdateData)
        .eq("id", job_id)
        .select("id", { count: "exact", head: true });

      console.log("[complete_job] job update result:", { error: updateErr, rowsUpdated: count });

      if (updateErr) {
        console.error("[complete_job] job update FAILED:", updateErr.message);
      } else {
        console.log("[complete_job] JOB UPDATED SUCCESSFULLY, rows:", count);
        if (!execId && currJob?.execution_id) {
          execId = currJob.execution_id;
        }
      }
    }

    // --- Update executions ---
    if (execId) {
      console.log("[complete_job] processing execution:", execId);
      
      const execUpdate: Record<string, unknown> = {
        status: status,
        finished_at: new Date().toISOString(),
      };
      if (duration_ms) execUpdate.duration_ms = duration_ms;
      if (error) execUpdate.error_message = error;

      const { error: execErr } = await supabase
        .from("executions")
        .update(execUpdate)
        .eq("id", execId);

      if (execErr) {
        console.error("[complete_job] execution update FAILED:", execErr.message);
      } else {
        console.log("[complete_job] EXECUTION UPDATED SUCCESSFULLY");
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
    return jsonResponse({ ok: false, error: String(error) }, 500);
  }
});