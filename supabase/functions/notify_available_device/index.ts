import { serve } from "https://deno.land/std@0.192.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

serve(async (req) => {
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  };

  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }

  if (req.method !== "POST") {
    return jsonResponse({ ok: false, error: "Method not allowed" }, 405, corsHeaders);
  }

  const authResult = await authenticateDeviceWithDetails(
    req,
    "id, org_id, name, total_cpu_cores, total_memory_gb"
  );

  if (!authResult.device) {
    return jsonResponse({ ok: false, error: authResult.error }, 401, corsHeaders);
  }

  const device = authResult.device;

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    const body = await req.json().catch(() => ({}));
    const metrics = body?.metrics;

    if (!metrics) {
      return jsonResponse({ ok: false, error: "Missing metrics object" }, 400, corsHeaders);
    }

    const {
      cpu_usage_percent,
      cpu_cores_free,
      total_cpu_cores,
      memory_free_gb,
      total_memory_gb,
      io_bandwidth_mb_s,
      network_latency_ms,
      gpu_available,
    } = metrics;

    const derived_status =
      cpu_usage_percent > 90
        ? "busy"
        : memory_free_gb < total_memory_gb * 0.2
        ? "busy"
        : "available";

    const { error: updateErr } = await supabase
      .from("devices")
      .update({
        cpu_usage_percent,
        cpu_cores_free,
        memory_free_gb,
        network_latency_ms,
        gpu_available,
        status: derived_status,
        active_workers: body.active_workers ?? 0,
        last_heartbeat: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq("id", device.id)
      .eq("org_id", device.org_id);

    if (updateErr) {
      throw new Error(updateErr.message);
    }

    await supabase.from("agent_metrics").insert({
      device_id: device.id,
      org_id: device.org_id,
      metrics,
      source: "notify_available_device",
    });

    return jsonResponse({
      ok: true,
      message: `Heartbeat OK for ${device.name}`,
      device_id: device.id,
      status: derived_status,
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[notify_available_device]", err.message);
    return jsonResponse({ ok: false, error: err.message }, 500, corsHeaders);
  }
});
