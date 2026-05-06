import { serve } from "https://deno.land/std@0.192.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
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
  const authResult = await authenticateDeviceWithDetails(req, "id, org_id, name, total_cpu_cores, total_memory_gb, max_concurrency, last_policy_update");
  if (!authResult.device) {
    return jsonResponse({
      ok: false,
      error: authResult.error
    }, 401, corsHeaders);
  }
  const device = authResult.device;
  const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
    global: {
      headers: {
        "x-client-info": "agent_health_policy/3.1.0"
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
  const lastUpdate = device.last_policy_update ? new Date(device.last_policy_update).getTime() : 0;
  if (Date.now() - lastUpdate < 5000) {
    return jsonResponse({
      ok: true,
      concurrency: device.max_concurrency ?? 1,
      notes: "cooldown active"
    }, 200, corsHeaders);
  }
  const totalCores = Number(body.total_cpu_cores ?? device.total_cpu_cores ?? 2);
  const totalMemory = Number(body.total_memory_gb ?? device.total_memory_gb ?? 4);
  const metrics = {
    device_id: device.id,
    org_id: device.org_id,
    cpu_cores_free: Math.max(0, Number(body.cpu_cores_free ?? 1)),
    total_cpu_cores: totalCores,
    memory_free_gb: Math.max(0, Number(body.memory_free_gb ?? 1)),
    total_memory_gb: totalMemory,
    network_latency_ms: Number(body.network_latency_ms ?? 50),
    gpu_available: Boolean(body.gpu_available ?? false),
    cpu_usage_percent: Number(body.cpu_usage_percent ?? 0),
    incoming_workload_weight: Number(body.incoming_workload_weight ?? 0)
  };
  const status = metrics.cpu_usage_percent > 90 ? "busy" : metrics.memory_free_gb < totalMemory * 0.2 ? "busy" : "available";
  await supabase.from("devices").update({
    cpu_usage_percent: metrics.cpu_usage_percent,
    memory_free_gb: metrics.memory_free_gb,
    network_latency_ms: metrics.network_latency_ms,
    gpu_available: metrics.gpu_available,
    status,
    last_heartbeat: new Date().toISOString()
  }).eq("id", device.id);
  let concurrency = Math.max(1, Math.floor(metrics.total_cpu_cores / 2));
  const memRatio = metrics.total_memory_gb > 0 ? metrics.memory_free_gb / metrics.total_memory_gb : 0.5;
  if (memRatio < 0.25) concurrency = Math.max(1, Math.floor(concurrency / 2));
  else if (memRatio > 0.75) concurrency += 1;
  if (metrics.cpu_usage_percent > 85) concurrency = Math.max(1, concurrency - 1);
  else if (metrics.cpu_usage_percent < 40) concurrency += 1;
  if (metrics.network_latency_ms > 200) concurrency = Math.max(1, concurrency - 1);
  concurrency = Math.min(concurrency, metrics.total_cpu_cores * 2);
  concurrency = Math.max(1, concurrency);
  await supabase.from("devices").update({
    max_concurrency: concurrency,
    last_policy_update: new Date().toISOString()
  }).eq("id", device.id);
  try {
    await supabase.from("agent_metrics").insert({
      device_id: device.id,
      org_id: device.org_id,
      metrics,
      concurrency_returned: concurrency,
      load_factor: Number((1 - memRatio).toFixed(3)),
      source: "agent_health_policy"
    });
  } catch  {
  // ignore logging failures
  }
  return jsonResponse({
    ok: true,
    concurrency,
    load_factor: Number((1 - memRatio).toFixed(3))
  }, 200, corsHeaders);
});
