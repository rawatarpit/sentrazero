import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
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
  const authResult = await authenticateDeviceWithDetails(req, "id, org_id, name");
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
  const device = authResult.device;
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const { data: benchmarks, error } = await supabase.from("device_benchmarks").select("throughput, latency_ms").eq("device_id", device.id).eq("org_id", device.org_id);
    if (error) throw error;
    if (!benchmarks || benchmarks.length === 0) {
      throw new Error("No benchmarks found for device");
    }
    const avgThroughput = benchmarks.reduce((s, b)=>s + (b.throughput ?? 0), 0) / benchmarks.length;
    const avgLatency = benchmarks.reduce((s, b)=>s + (b.latency_ms ?? 1), 0) / benchmarks.length;
    const efficiency = Math.max(1, avgThroughput / avgLatency);
    const preferred_chunk_size_gb = Math.min(8, Math.max(0.5, efficiency * 10));
    const { error: updateErr } = await supabase.from("devices").update({
      preferred_chunk_size_gb
    }).eq("id", device.id).eq("org_id", device.org_id);
    if (updateErr) throw updateErr;
    return new Response(JSON.stringify({
      ok: true,
      preferred_chunk_size_gb
    }), {
      headers: corsHeaders
    });
  } catch (err) {
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      status: 400,
      headers: corsHeaders
    });
  }
});
