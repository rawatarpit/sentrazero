import { serve } from "https://deno.land/std@0.192.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
  if (req.method !== "GET") {
    return new Response(JSON.stringify({
      error: "Method not allowed"
    }), {
      status: 405,
      headers: corsHeaders
    });
  }
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SERVICE_KEY) {
    return new Response(JSON.stringify({
      error: "server misconfigured"
    }), {
      status: 500,
      headers: corsHeaders
    });
  }
  const authResult = await authenticateDeviceWithDetails(req, "id, org_id, name");
  if (!authResult.device) {
    return new Response(JSON.stringify({
      error: authResult.error
    }), {
      status: 401,
      headers: corsHeaders
    });
  }
  const device = authResult.device;
  const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
    global: {
      headers: {
        "x-client-info": "agent_stream/2.0.0-realtime"
      }
    }
  });
  const sentJobIds = new Set();
  const stream = new ReadableStream({
    start (controller) {
      const encoder = new TextEncoder();
      const sendEvent = (event, data)=>{
        try {
          controller.enqueue(encoder.encode(`event: ${event}\n`));
          controller.enqueue(encoder.encode(`data: ${data}\n\n`));
        } catch  {
        // Controller closed
        }
      };
      sendEvent("hello", JSON.stringify({
        device_id: device.id,
        org_id: device.org_id,
        message: "connected",
        realtime_enabled: true
      }));
      const sendInitialJobs = async ()=>{
        try {
          const { data: jobs, error } = await supabase.from("agent_jobs").select("id, job_type, payload, status, execution_step_id, execution_id").eq("agent_id", device.id).eq("org_id", device.org_id).eq("status", "assigned");
          if (error) {
            console.error("[agent_stream] initial jobs fetch error:", error);
            return;
          }
          if (jobs && jobs.length > 0) {
            for (const job of jobs){
              if (!sentJobIds.has(job.id)) {
                sentJobIds.add(job.id);
                sendEvent("job", JSON.stringify({
                  id: job.id,
                  agent_id: device.id,
                  job_type: job.job_type,
                  status: job.status,
                  payload: job.payload,
                  execution_id: job.execution_id,
                  source: "initial"
                }));
              }
            }
          }
          sendEvent("sync", JSON.stringify({
            jobs_sent: jobs?.length ?? 0,
            timestamp: new Date().toISOString()
          }));
        } catch (err) {
          console.error("[agent_stream] initial jobs error:", err);
        }
      };
      sendInitialJobs();
      const pollJobs = async ()=>{
        try {
          const { data: jobs, error } = await supabase.from("agent_jobs").select("id, job_type, payload, status, execution_step_id, execution_id").eq("agent_id", device.id).eq("org_id", device.org_id).eq("status", "assigned");
          if (error) {
            console.error("[agent_stream] fallback poll error:", error);
            return;
          }
          if (jobs && jobs.length > 0) {
            for (const job of jobs){
              if (!sentJobIds.has(job.id)) {
                sentJobIds.add(job.id);
                sendEvent("job", JSON.stringify({
                  id: job.id,
                  agent_id: device.id,
                  job_type: job.job_type,
                  status: job.status,
                  payload: job.payload,
                  execution_id: job.execution_id,
                  source: "fallback"
                }));
              }
            }
          }
        } catch (err) {
          console.error("[agent_stream] fallback poll error:", err);
        }
      };
      const pollInterval = setInterval(pollJobs, 30000);
      const keepAlive = setInterval(()=>{
        try {
          controller.enqueue(encoder.encode(": keepalive\n\n"));
        } catch  {
          clearInterval(keepAlive);
          clearInterval(pollInterval);
        }
      }, 15000);
      req.signal.addEventListener("abort", ()=>{
        clearInterval(keepAlive);
        clearInterval(pollInterval);
        try {
          controller.close();
        } catch  {}
      });
    }
  });
  return new Response(stream, {
    headers: {
      ...corsHeaders,
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive"
    }
  });
});
