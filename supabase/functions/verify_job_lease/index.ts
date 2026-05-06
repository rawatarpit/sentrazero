import { serve } from "https://deno.land/std@0.192.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";
serve(async (req)=>{
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }
  if (req.method !== "POST") {
    return jsonResponse({
      ok: false,
      error: "Method not allowed"
    }, 405, corsHeaders);
  }
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SERVICE_KEY) {
    return jsonResponse({
      ok: false,
      error: "server misconfigured"
    }, 500, corsHeaders);
  }
  const authResult = await authenticateDevice(req);
  if (!authResult.device) {
    return jsonResponse({
      ok: false,
      error: authResult.error
    }, 401, corsHeaders);
  }
  const deviceId = authResult.device.id;
  const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
    global: {
      headers: {
        "x-client-info": "verify_job_lease/1.0.0"
      }
    }
  });
  let body;
  try {
    body = await req.json();
  } catch  {
    return jsonResponse({
      ok: false,
      error: "invalid json"
    }, 400, corsHeaders);
  }
  const { job_id } = body ?? {};
  if (!job_id) {
    return jsonResponse({
      ok: false,
      error: "missing job_id"
    }, 400, corsHeaders);
  }
  const { data: job, error: jobErr } = await supabase.from("agent_jobs").select("id, status, agent_id, lease_expires_at, org_id").eq("id", job_id).maybeSingle();
  if (jobErr) {
    console.error("[verify_job_lease] Job query error:", jobErr.message, jobErr.code);
    return jsonResponse({
      ok: true,
      job_id,
      status: "not_found",
      is_valid: false
    }, 200, corsHeaders);
  }
  if (!job) {
    return jsonResponse({
      ok: true,
      job_id,
      status: "not_found",
      is_valid: false
    }, 200, corsHeaders);
  }
  if (job.org_id !== authResult.device.org_id) {
    return jsonResponse({
      ok: true,
      job_id,
      status: "access_denied",
      is_valid: false
    }, 200, corsHeaders);
  }
  const { data: lease, error: leaseErr } = await supabase.from("leases").select("job_id, device_id, lease_expires_at, status").eq("job_id", job_id).eq("device_id", deviceId).maybeSingle();
  let isValid = false;
  let leaseStatus = "valid";
  if (leaseErr) {
    leaseStatus = "no_lease_row";
  } else if (!lease) {
    // No leases table row — fall back to agent_jobs.lease_expires_at,
    // which is set by claim_jobs_for_device and trigger-based assignment
    if (job.agent_id === deviceId && (job.status === "assigned" || job.status === "running") && job.lease_expires_at && new Date(job.lease_expires_at) > new Date()) {
      isValid = true;
      leaseStatus = "valid";
    } else {
      leaseStatus = "no_lease_row";
    }
  } else if (lease.lease_expires_at && new Date(lease.lease_expires_at) < new Date()) {
    leaseStatus = "expired";
  } else if (job.agent_id !== deviceId) {
    leaseStatus = "owned_by_other";
  } else if (job.status !== "assigned" && job.status !== "running") {
    leaseStatus = "terminal";
  } else {
    isValid = true;
    leaseStatus = "valid";
  }
  return jsonResponse({
    ok: true,
    job_id: job.id,
    status: leaseStatus,
    agent_id: job.agent_id,
    is_valid: isValid
  }, 200, corsHeaders);
});
