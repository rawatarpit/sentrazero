import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";
serve(async (req)=>{
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type, x-device-id",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }
  const authResult = await authenticateDevice(req);
  if (!authResult.device) {
    return jsonResponse({
      ok: false,
      error: authResult.error
    }, 401, corsHeaders);
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const body = await req.json();
    const { limit = 10, lease_ttl_seconds = 1800 } = body;
    const deviceId = authResult.device.id;
    const orgId = authResult.device.org_id;
    const { data: jobs, error } = await supabase.rpc("claim_jobs_for_device", {
      p_device_id: deviceId,
      p_org_id: orgId,
      p_limit: limit,
      p_lease_ttl_seconds: lease_ttl_seconds
    });
    if (error) throw error;
    if (!jobs || jobs.length === 0) {
      return jsonResponse({
        ok: true,
        jobs: [],
        message: "No jobs available"
      }, 200, corsHeaders);
    }
    // Normalize the response to use field names that the agent expects
    // Agent struct (ClaimedJob) expects: job_id, job_type, payload, execution_id
    const normalizedJobs = jobs.map((job)=>({
        job_id: job.job_id,
        job_type: job.job_type,
        payload: job.job_payload,
        execution_id: job.exec_id,
        dataset_id: job.dataset_id,
        chunk_index: job.chunk_index,
        lease_expires_at: job.lease_expires_at,
        is_reclaimed: job.is_reclaimed || false,
        retry_count: job.retry_count || 0
      }));
    return jsonResponse({
      ok: true,
      jobs: normalizedJobs
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[claim_jobs_for_device]", err.message);
    return jsonResponse({
      ok: false,
      error: err.message
    }, 500, corsHeaders);
  }
});
