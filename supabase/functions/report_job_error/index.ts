import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

function sanitizeErrorMessage(msg: string): string {
  let sanitized = msg;
  
  sanitized = sanitized.replace(/\/[^\/\s]+\/[^\/\s]+/g, '[PATH]');
  sanitized = sanitized.replace(/[A-Za-z]:\\[^\\]+\\[^\\]+/g, '[PATH]');
  sanitized = sanitized.replace(/s3:\/\/[^\s]+/g, 's3://[BUCKET]');
  sanitized = sanitized.replace(/gs:\/\/[^\s]+/g, 'gs://[BUCKET]');
  sanitized = sanitized.replace(/https:\/\/[^/]+\/[^?\s]+/g, 'https://[STORAGE]/[OBJECT]');
  sanitized = sanitized.replace(/~[^\/\s]+/g, '~[USER]');
  sanitized = sanitized.replace(/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\/[^\s]+/g, '[IP]/[PATH]');
  sanitized = sanitized.replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, '[EMAIL]');
  
  return sanitized;
}

serve(async (req) => {
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  };

  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }

  const authResult = await authenticateDeviceWithDetails(req, "id, org_id");
  if (!authResult.device) {
    return jsonResponse({ ok: false, error: authResult.error }, 401, corsHeaders);
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    const body = await req.json().catch(() => ({}));
    const { job_id, error_message, force_dead_letter } = body;

    if (!job_id) {
      return jsonResponse({ ok: false, error: "job_id is required" }, 400, corsHeaders);
    }

    const sanitizedError = error_message ? sanitizeErrorMessage(error_message) : 'Unknown error';

    const { data: job, error: fetchError } = await supabase
      .from("agent_jobs")
      .select("id, org_id, retry_count, max_retries, status")
      .eq("id", job_id)
      .eq("org_id", authResult.device.org_id)
      .maybeSingle();

    if (fetchError) {
      console.error("[report_job_error] Job query error:", fetchError.message, fetchError.code);
      return jsonResponse({ ok: false, error: "Failed to fetch job: " + fetchError.message }, 500, corsHeaders);
    }

    if (!job) {
      return jsonResponse({ ok: false, error: "Job not found or access denied" }, 404, corsHeaders);
    }

    const retryCount = (job.retry_count || 0) + 1;
    const maxRetries = job.max_retries || 3;
    const shouldDeadLetter = force_dead_letter === true || retryCount >= maxRetries;

    if (shouldDeadLetter) {
      await supabase.rpc("move_job_to_dead_letter", { 
        p_job_id: job_id,
        p_org_id: authResult.device.org_id
      });

      await supabase.from("system_logs").insert({
        event_type: "job_dead_lettered",
        message: `Job ${job_id} moved to dead letter after ${retryCount} retries`,
        org_id: authResult.device.org_id,
      });

      return jsonResponse({ 
        ok: true, 
        job_id, 
        action: "dead_lettered",
        retry_count: retryCount
      }, 200, corsHeaders);
    }

    const { error: updateError } = await supabase
      .from("agent_jobs")
      .update({
        retry_count: retryCount,
        last_error: sanitizedError,
        status: 'pending',
        agent_id: null,
        assigned_at: null,
        lease_expires_at: null,
        updated_at: new Date().toISOString()
      })
      .eq("id", job_id)
      .eq("org_id", authResult.device.org_id);

    if (updateError) throw updateError;

    await supabase.from("agent_worker_activity").upsert({
      job_id: job_id,
      device_id: authResult.device.id,
      status: 'failed',
      error: sanitizedError,
      finished_at: new Date().toISOString()
    });

    return jsonResponse({ 
      ok: true, 
      job_id, 
      action: "requeued",
      retry_count: retryCount,
      max_retries: maxRetries,
      can_retry: retryCount < maxRetries
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[report_job_error]", err.message);
    
    try {
      await supabase.rpc("log_agent_error", {
        _device_id: authResult.device.id,
        _job_id: null,
        _error_message: `report_job_error failed: ${err.message}`
      });
    } catch {}

    return jsonResponse({ ok: false, error: err.message }, 500, corsHeaders);
  }
});
