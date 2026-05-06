import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
const MAX_CHUNK_COUNT_THRESHOLD = 200;
const DATASET_SIZE_GB_THRESHOLD = 50;
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  const origin = req.headers.get("origin");
  if (origin) {
    const allowedOrigins = Deno.env.get("ALLOWED_ORIGINS")?.split(",") ?? [];
    if (allowedOrigins.length > 0 && !allowedOrigins.some((o)=>origin.trim() === o.trim())) {
      return new Response("Forbidden", {
        status: 403,
        headers: corsHeaders
      });
    }
  }
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
  const authResult = await authenticateDeviceWithDetails(req, "id, org_id, name, merge_capable, status, memory_free_gb, active_workers, max_concurrency");
  if (!authResult.device) {
    return new Response(JSON.stringify({
      error: authResult.error
    }), {
      status: 401,
      headers: corsHeaders
    });
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const body = await req.json();
    const { dataset_id } = body;
    if (!dataset_id) {
      throw new Error("dataset_id is required");
    }
    const orgId = authResult.device.org_id;
    const { data: dataset, error: datasetError } = await supabase.from("datasets").select(`
        id,
        name,
        org_id,
        total_size_gb,
        affinity_device_id,
        status,
        merged_at,
        merged_size_gb
      `).eq("id", dataset_id).maybeSingle();
    if (datasetError) throw datasetError;
    if (!dataset) throw new Error("Dataset not found");
    if (dataset.org_id !== orgId) {
      throw new Error("Access denied: dataset belongs to different org");
    }
    if (dataset.status === "merged") {
      return new Response(JSON.stringify({
        ok: true,
        message: "Dataset already merged",
        dataset_id: dataset.id,
        status: dataset.status
      }), {
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders
        }
      });
    }
    const { data: chunks, error: chunksError } = await supabase.from("batch_chunks").select("id, chunk_index, status, merged_in, chunk_size_gb").eq("dataset_id", dataset_id).eq("merged_in", false).order("chunk_index", {
      ascending: true
    });
    if (chunksError) throw chunksError;
    const activeChunks = (chunks || []).filter((c)=>c.status === "completed" && !c.merged_in);
    if (activeChunks.length === 0) {
      return new Response(JSON.stringify({
        ok: false,
        error: "No completed chunks available for merge"
      }), {
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders
        }
      });
    }
    const totalSizeGB = dataset.total_size_gb || activeChunks.reduce((sum, c)=>sum + (c.chunk_size_gb || 0), 0);
    const useTreeMerge = activeChunks.length > MAX_CHUNK_COUNT_THRESHOLD || totalSizeGB > DATASET_SIZE_GB_THRESHOLD;
    let targetDevice = authResult.device;
    if (dataset.affinity_device_id && dataset.affinity_device_id !== targetDevice.id) {
      const { data: affinityDevice, error: affinityError } = await supabase.from("devices").select("id, name, merge_capable, status, memory_free_gb, active_workers, max_concurrency").eq("id", dataset.affinity_device_id).eq("merge_capable", true).eq("status", "online").maybeSingle();
      if (!affinityError && affinityDevice) {
        targetDevice = affinityDevice;
      }
    }
    const VALID_MERGE_STATUSES = [
      "online",
      "available"
    ];
    if (!targetDevice.merge_capable || !VALID_MERGE_STATUSES.includes(targetDevice.status)) {
      const { data: availableDevices, error: devicesError } = await supabase.from("devices").select("id, name, merge_capable, status, memory_free_gb, active_workers, max_concurrency").eq("org_id", orgId).eq("merge_capable", true).eq("status", "online").lt("active_workers", 1).order("memory_free_gb", {
        ascending: false
      }).limit(10);
      if (devicesError) throw devicesError;
      if (!availableDevices || availableDevices.length === 0) {
        return new Response(JSON.stringify({
          ok: false,
          error: "No merge-capable devices available"
        }), {
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders
          }
        });
      }
      targetDevice = availableDevices[0];
    }
    const { data: storageConfig } = await supabase.from("org_storage_configs").select("storage_mode").eq("org_id", orgId).eq("is_default", true).maybeSingle();
    const storageMode = storageConfig?.storage_mode ?? "shared_mount";
    const lockResult = await supabase.rpc("acquire_dataset_merge_lock", {
      p_dataset_id: dataset_id,
      p_agent_id: targetDevice.id,
      p_device_id: targetDevice.id,
      p_org_id: orgId,
      p_duration_minutes: 60
    });
    if (lockResult.error || !lockResult.data?.success) {
      return new Response(JSON.stringify({
        ok: false,
        error: "Failed to acquire merge lock",
        details: lockResult.error?.message || lockResult.data?.error
      }), {
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders
        }
      });
    }
    const mergeJobPayload = {
      job_type: "merge_dataset",
      dataset_id: dataset_id,
      affinity_device_id: targetDevice.id,
      storage_mode: storageMode,
      use_tree_merge: useTreeMerge,
      chunks: activeChunks.map((c, idx)=>({
          chunk_id: c.id,
          chunk_index: c.chunk_index,
          size_bytes: Math.floor((c.chunk_size_gb || 0) * 1024 * 1024 * 1024)
        })),
      delete_chunks_after: false,
      checksum_required: true
    };
    const { data: job, error: jobError } = await supabase.from("agent_jobs").insert({
      agent_id: targetDevice.id,
      org_id: orgId,
      job_type: "merge_dataset",
      payload: mergeJobPayload,
      status: "pending"
    }).select().single();
    if (jobError) {
      await supabase.rpc("release_dataset_merge_lock", {
        p_lock_id: lockResult.data.lock_id,
        p_org_id: orgId
      });
      throw jobError;
    }
    await supabase.from("datasets").update({
      status: "merging",
      merge_started_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }).eq("id", dataset_id).eq("org_id", orgId);
    console.log(`📦 Merge job scheduled for dataset ${dataset_id} on device ${targetDevice.name}`);
    return new Response(JSON.stringify({
      ok: true,
      dataset_id: dataset_id,
      job_id: job.id,
      target_device: {
        id: targetDevice.id,
        name: targetDevice.name
      },
      merge_strategy: useTreeMerge ? "tree" : "sequential",
      chunk_count: activeChunks.length,
      total_size_gb: totalSizeGB
    }), {
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders
      }
    });
  } catch (err) {
    console.error("[schedule_merge_job] ❌", err.message);
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      headers: corsHeaders,
      status: 500
    });
  }
});
