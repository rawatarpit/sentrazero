import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin, authenticateInternal } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
const SAFE_DEFAULT_VECTOR = Array(16).fill(0.1);
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", {
      status: 403,
      headers: corsHeaders
    });
  }
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  const authResult = await authenticateInternal(req);
  if (!authResult.authorized) {
    return new Response(JSON.stringify({
      ok: false,
      error: authResult.error
    }), {
      status: 401,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const body = await req.json();
    const { dataset_id, job_type = "process" } = body;
    if (!dataset_id) {
      throw new Error("Missing dataset_id");
    }
    console.log(`[auto_assign_best_device] 🔍 Matching best device for dataset ${dataset_id}`);
    const { data: dataset } = await supabase.from("datasets").select("id, org_id, file_type").eq("id", dataset_id).maybeSingle();
    if (!dataset) throw new Error("Dataset not found");
    if (dataset.org_id !== authResult.orgId) {
      throw new Error("Access denied: dataset belongs to different org");
    }
    const { data: match, error: matchErr } = await supabase.rpc("match_best_device", {
      _org_id: dataset.org_id,
      _chunk_vector: SAFE_DEFAULT_VECTOR,
      _job_type: job_type
    });
    if (matchErr) throw matchErr;
    const best = match?.[0];
    const selectedDeviceId = best?.device_id ?? best?.id;
    if (!selectedDeviceId) {
      throw new Error("No suitable online device found for this dataset");
    }
    console.log(`[auto_assign_best_device] ✅ Selected device ${selectedDeviceId}` + ` (score: ${best.score?.toFixed(3) ?? "n/a"})`);
    const { data: device } = await supabase.from("devices").select("id, name, active_jobs, max_concurrency, cpu_cores_free, total_cpu_cores, memory_free_gb, total_memory_gb").eq("id", selectedDeviceId).single();
    if (device) {
      const cpuRatio = device.cpu_cores_free != null && device.total_cpu_cores ? device.cpu_cores_free / device.total_cpu_cores : null;
      const memRatio = device.memory_free_gb != null && device.total_memory_gb ? device.memory_free_gb / device.total_memory_gb : null;
      const concurRatio = device.max_concurrency ? Math.max(0, (device.max_concurrency - (device.active_jobs ?? 0)) / device.max_concurrency) : null;
      console.log(`[auto_assign_best_device] 📊 Device ${device.name} —` + ` cpu: ${cpuRatio != null ? (cpuRatio * 100).toFixed(1) + "%" : "n/a"}` + ` memory: ${memRatio != null ? (memRatio * 100).toFixed(1) + "%" : "n/a"}` + ` concurrency: ${concurRatio != null ? (concurRatio * 100).toFixed(1) + "%" : "n/a"}`);
      await supabase.from("system_logs").insert({
        event_type: "device_selected",
        message: `Device ${device.name} selected for dataset ${dataset_id} (score: ${best.score?.toFixed(3) ?? "n/a"})`
      });
    }
    const relayUrl = `${Deno.env.get("SUPABASE_URL")}/functions/v1/relay_job_event`;
    const relayPayload = {
      channel: `agent-${selectedDeviceId}`,
      data: {
        type: "process_dataset",
        dataset_id,
        org_id: dataset.org_id,
        job_type
      }
    };
    const relayRes = await fetch(relayUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
        "x-org-id": dataset.org_id
      },
      body: JSON.stringify(relayPayload)
    });
    let relayData;
    const relayContentType = relayRes.headers.get("content-type") ?? "";
    if (relayContentType.includes("application/json")) {
      relayData = await relayRes.json();
    } else {
      relayData = {
        raw: await relayRes.text(),
        status: relayRes.status
      };
    }
    if (!relayRes.ok) {
      console.warn(`[auto_assign_best_device] ⚠️ Relay returned ${relayRes.status}:`, relayData);
    } else {
      console.log(`[auto_assign_best_device] 📡 Relay dispatched:`, relayData);
    }
    const { error: datasetUpdateErr } = await supabase.from("datasets").update({
      status: "processing",
      updated_at: new Date().toISOString()
    }).eq("id", dataset_id);
    if (datasetUpdateErr) {
      console.warn("[auto_assign_best_device] dataset status update failed:", datasetUpdateErr.message);
    }
    const cpuRatio = device?.cpu_cores_free != null && device?.total_cpu_cores ? device.cpu_cores_free / device.total_cpu_cores : null;
    const memRatio = device?.memory_free_gb != null && device?.total_memory_gb ? device.memory_free_gb / device.total_memory_gb : null;
    const concurRatio = device?.max_concurrency ? Math.max(0, (device.max_concurrency - (device.active_jobs ?? 0)) / device.max_concurrency) : null;
    return new Response(JSON.stringify({
      ok: true,
      assigned_device: selectedDeviceId,
      dataset_id,
      capacity_metrics: {
        cpu_available_ratio: cpuRatio,
        memory_available_ratio: memRatio,
        concurrency_available_ratio: concurRatio,
        overall_score: best.score ?? null
      }
    }), {
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders
      }
    });
  } catch (err) {
    console.error("[auto_assign_best_device] ❌", err.message);
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders
      },
      status: 400
    });
  }
});
