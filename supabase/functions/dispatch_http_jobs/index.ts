import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { timingSafeEqual } from "../_shared/security.ts";
const MAX_RETRIES = 5;
const BATCH_SIZE = 50;
const RETRY_DELAYS = [
  2,
  5,
  10,
  30,
  60
];
const CONCURRENT_REQUESTS = 10;
const MIN_CRON_SECRET_LENGTH = 32;
async function authenticateRequest(req) {
  const cronSecret = req.headers.get("x-cron-secret");
  const configuredCronSecret = Deno.env.get("CRON_SECRET");
  if (!cronSecret || !configuredCronSecret) return false;
  if (configuredCronSecret.length < MIN_CRON_SECRET_LENGTH) {
    console.error("[dispatch_http_jobs] CRON_SECRET is too short (minimum %d characters)", MIN_CRON_SECRET_LENGTH);
    return false;
  }
  return await timingSafeEqual(cronSecret, configuredCronSecret);
}
function buildUrl(url) {
  if (url.startsWith("http://") || url.startsWith("https://")) {
    return url;
  }
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "http://localhost:54321";
  return `${supabaseUrl}/functions/v1${url.startsWith("/") ? url : "/" + url}`;
}
async function processJobWithRetry(supabase, job, retryIndex) {
  try {
    const headers = {
      "Content-Type": "application/json"
    };
    if (job.headers) for (const [k, v] of Object.entries(job.headers))headers[k] = v;
    if (job.idempotency_key) {
      headers["X-Idempotency-Key"] = job.idempotency_key;
    }
    const res = await fetch(buildUrl(job.url), {
      method: "POST",
      headers,
      body: JSON.stringify(job.body)
    });
    const text = await res.text();
    const success = res.ok;
    if (success) {
      await supabase.from("http_queue").update({
        processed: true,
        processed_at: new Date().toISOString(),
        status_code: res.status,
        result: text.slice(0, 500)
      }).eq("id", job.id);
      console.log(`✅ Job ${job.id} → ${res.status}`);
      return {
        id: job.id,
        status: "success",
        code: res.status
      };
    } else {
      const nextRetryCount = (job.retry_count ?? 0) + 1;
      const delayMinutes = RETRY_DELAYS[Math.min(nextRetryCount - 1, RETRY_DELAYS.length - 1)];
      const retryAt = new Date(Date.now() + delayMinutes * 60 * 1000).toISOString();
      await supabase.from("http_queue").update({
        retry_count: nextRetryCount,
        retry_at: retryAt,
        status_code: res.status,
        result: text.slice(0, 500)
      }).eq("id", job.id);
      console.warn(`⚠️ Job ${job.id} failed (HTTP ${res.status}), retry #${nextRetryCount} at ${retryAt}`);
      return {
        id: job.id,
        status: "retry",
        code: res.status,
        retry_at: retryAt
      };
    }
  } catch (err) {
    const nextRetryCount = (job.retry_count ?? 0) + 1;
    const delayMinutes = RETRY_DELAYS[Math.min(nextRetryCount - 1, RETRY_DELAYS.length - 1)];
    const retryAt = new Date(Date.now() + delayMinutes * 60 * 1000).toISOString();
    await supabase.from("http_queue").update({
      retry_count: nextRetryCount,
      retry_at: retryAt,
      result: err.message,
      status_code: 500
    }).eq("id", job.id);
    console.error(`❌ Network error job ${job.id}, retry #${nextRetryCount} at ${retryAt}`);
    return {
      id: job.id,
      status: "retry_error",
      error: err.message
    };
  }
}
async function batchProcessJobs(supabase, jobs) {
  const results = [];
  for(let i = 0; i < jobs.length; i += CONCURRENT_REQUESTS){
    const batch = jobs.slice(i, i + CONCURRENT_REQUESTS);
    const batchResults = await Promise.all(batch.map((job, idx)=>processJobWithRetry(supabase, job, i + idx)));
    results.push(...batchResults);
  }
  return results;
}
serve(async (req)=>{
  const isAuthenticated = await authenticateRequest(req);
  if (!isAuthenticated) {
    return new Response(JSON.stringify({
      error: "Unauthorized"
    }), {
      status: 401
    });
  }
  const supabaseUrl = Deno.env.get("SUPABASE_URL");
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!supabaseUrl || !serviceKey) {
    return new Response(JSON.stringify({
      error: "server misconfigured"
    }), {
      status: 500
    });
  }
  const supabase = createClient(supabaseUrl, serviceKey);
  try {
    const { data: jobs, error } = await supabase.from("http_queue").select("id, url, body, headers, processed, status_code, retry_count, idempotency_key").eq("processed", false).or(`retry_at.is.null,retry_at.lte.${new Date().toISOString()}`).order("created_at", {
      ascending: true
    }).limit(BATCH_SIZE);
    if (error) throw error;
    if (!jobs?.length) return new Response("✅ No pending jobs");
    console.log(`🚀 Dispatching ${jobs.length} jobs...`);
    const processedJobs = jobs.filter((job)=>!(job.processed && job.status_code >= 200 && job.status_code < 300));
    const alreadyProcessed = jobs.filter((job)=>job.processed && job.status_code >= 200 && job.status_code < 300);
    alreadyProcessed.forEach((job)=>{
      console.log(`⏭️ Job ${job.id} already processed, skipping (idempotency check)`);
    });
    const jobsToRetry = processedJobs.filter((job)=>(job.retry_count ?? 0) < MAX_RETRIES);
    const deadLettered = processedJobs.filter((job)=>(job.retry_count ?? 0) >= MAX_RETRIES);
    for (const job of deadLettered){
      console.log(`⏭️ Job ${job.id} exceeded max retries, marking as dead-lettered`);
      await supabase.from("http_queue").update({
        processed: true,
        processed_at: new Date().toISOString(),
        result: "Max retries exceeded",
        status_code: -1
      }).eq("id", job.id);
      await supabase.from("system_logs").insert({
        event_type: "http_job_dead_lettered",
        message: `Job ${job.id} exceeded max retries`
      });
    }
    const results = await batchProcessJobs(supabase, jobsToRetry);
    const successCount = results.filter((r)=>r.status === "success").length;
    const retryCount = results.filter((r)=>r.status?.startsWith("retry")).length;
    await supabase.from("system_logs").insert({
      event_type: "dispatch_http_jobs_summary",
      message: `Processed ${jobs.length} jobs: ${successCount} success, ${retryCount} retry, ${alreadyProcessed.length} skipped, ${deadLettered.length} dead-lettered`
    });
    return new Response(JSON.stringify({
      ok: true,
      processed: jobs.length,
      success: successCount,
      retry: retryCount,
      skipped: alreadyProcessed.length,
      dead_lettered: deadLettered.length
    }));
  } catch (err) {
    console.error("[dispatch_http_jobs] ❌", err);
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      status: 500
    });
  }
});
