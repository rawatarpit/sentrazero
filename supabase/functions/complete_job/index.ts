import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const authResult = await authenticateDevice(req);
    if (!authResult.device) {
      return jsonResponse({ ok: false, error: authResult.error }, 401, corsHeaders);
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const body = await req.json();
    const { execution_id, status, duration_ms, output, error, job_id } = body;

    console.log("[complete_job] request:", { execution_id, status, job_id });

    // Build job update data
    const jobUpdateData: any = {
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

    // Update job status (PRIMARY - always do this)
    let jobUpdated = false;
    if (job_id) {
      const { data: job, error: jobErr } = await supabase
        .from("agent_jobs")
        .update(jobUpdateData)
        .eq("id", job_id)
        .select("id, execution_id, org_id")
        .single();
      
      if (!jobErr && job) {
        jobUpdated = true;
        console.log("[complete_job] job updated:", job_id, "execution_id:", job.execution_id);
        
        // Now try to update/create execution if we have execution_id
        let execId = execution_id || job.execution_id;
        
        if (execId) {
          // Try to update execution
          const execUpdateData: any = {
            status: status,
            finished_at: new Date().toISOString(),
          };
          if (duration_ms) execUpdateData.duration_ms = duration_ms;
          if (error) execUpdateData.error_message = error;

          await supabase
            .from("executions")
            .update(execUpdateData)
            .eq("id", execId);
          
          console.log("[complete_job] execution updated:", execId);
        }
        
        return jsonResponse({ 
          success: true, 
          job_id: job_id,
          execution_id: execId,
          status: status
        }, 200, corsHeaders);
      }
      
      console.log("[complete_job] job update error:", jobErr?.message);
    }

    // Fallback: try with execution_id only
    if (execution_id) {
      const execUpdateData: any = {
        status: status,
        finished_at: new Date().toISOString(),
      };
      if (duration_ms) execUpdateData.duration_ms = duration_ms;
      if (error) execUpdateData.error_message = error;

      const { error: execErr } = await supabase
        .from("executions")
        .update(execUpdateData)
        .eq("id", execution_id);

      if (!execErr) {
        console.log("[complete_job] execution updated:", execution_id);
        return jsonResponse({ 
          success: true, 
          execution_id: execution_id,
          status: status
        }, 200, corsHeaders);
      }
      
      console.log("[complete_job] execution update error:", execErr?.message);
    }

    // Job update failed
    return jsonResponse({ 
      success: false, 
      error: "Failed to update job", 
      job_id,
      execution_id 
    }, 500, corsHeaders);

  } catch (error) {
    console.error("[complete_job] error:", error);
    return jsonResponse({ 
      success: false, 
      error: "Internal server error", 
      details: String(error) 
    }, 500, corsHeaders);
  }
});