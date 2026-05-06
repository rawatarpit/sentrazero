import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
function normalize(arr) {
  const norm = Math.sqrt(arr.reduce((a, v)=>a + v * v, 0));
  return arr.map((v)=>+(v / (norm || 1)).toFixed(6));
}
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
  const authResult = await authenticateDeviceWithDetails(req, "id, org_id");
  if (!authResult.device) {
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
    const { latency_ms, throughput, model_used } = body;
    if (!latency_ms || !throughput) {
      throw new Error("Missing required fields: latency_ms or throughput");
    }
    const { error: benchErr } = await supabase.from("device_benchmarks").insert([
      {
        device_id: authResult.device.id,
        org_id: authResult.device.org_id,
        latency_ms,
        throughput,
        model_used: model_used || "unspecified",
        created_at: new Date().toISOString()
      }
    ]);
    if (benchErr) throw benchErr;
    const { data: history, error: histErr } = await supabase.from("device_benchmarks").select("throughput, latency_ms").eq("device_id", authResult.device.id).order("created_at", {
      ascending: false
    }).limit(10);
    if (histErr) throw histErr;
    const avgThroughput = history.reduce((s, b)=>s + (b.throughput || 0), 0) / (history.length || 1);
    const avgLatency = history.reduce((s, b)=>s + (b.latency_ms || 0), 0) / (history.length || 1);
    const efficiency = Math.max(1, avgThroughput / (avgLatency || 1));
    const newChunkSize = Math.min(8, Math.max(0.5, efficiency * 10));
    const { data: existing } = await supabase.from("devices").select("device_vector").eq("id", authResult.device.id).maybeSingle();
    const oldVec = existing?.device_vector || [];
    const baseVec = normalize([
      throughput / 1000,
      1 / (latency_ms || 1),
      efficiency / 10,
      throughput / 1000 / (avgThroughput / 1000 || 1),
      avgThroughput / 1000,
      avgLatency / 1000,
      Math.sqrt(efficiency),
      throughput * 0.001,
      latency_ms * 0.0001,
      efficiency * 0.1,
      avgThroughput * 0.001,
      avgLatency * 0.0001,
      throughput / (latency_ms || 1) * 0.001,
      Math.log(efficiency + 1),
      avgThroughput / (avgLatency || 1) * 0.001,
      1
    ]);
    const blended = oldVec && oldVec.length === 16 ? oldVec.map((v, i)=>+(v * 0.8 + baseVec[i] * 0.2).toFixed(6)) : baseVec;
    const { error: updateErr } = await supabase.from("devices").update({
      preferred_chunk_size_gb: newChunkSize,
      device_vector: blended,
      updated_at: new Date().toISOString()
    }).eq("id", authResult.device.id);
    if (updateErr) throw updateErr;
    return new Response(JSON.stringify({
      ok: true,
      message: "Benchmark recorded, chunk size optimized, and profile vector updated",
      preferred_chunk_size_gb: newChunkSize,
      updated_vector_sample: blended.slice(0, 4)
    }), {
      headers: corsHeaders,
      status: 200
    });
  } catch (err) {
    console.error("[record_benchmark] ❌", err.message);
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      headers: corsHeaders,
      status: 400
    });
  }
});
