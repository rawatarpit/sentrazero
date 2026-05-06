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
  const sb = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const body = await req.json();
    const { chunk_id, dataset_id, batch_id, rows, checksum, duration_ms } = body;
    if (!dataset_id) throw new Error("Missing dataset_id");
    if (!chunk_id && !batch_id) throw new Error("Missing chunk_id or batch_id");
    const { data: chunk, error: chunkErr } = await sb.from("batch_chunks").select("id, org_id").eq("id", chunk_id || batch_id).single();
    if (chunkErr) throw new Error("Chunk not found");
    if (chunk.org_id !== authResult.device.org_id) {
      throw new Error("Access denied: chunk belongs to different org");
    }
    const { error: updateErr } = await sb.from("batch_chunks").update({
      status: "processed",
      processed_at: new Date().toISOString(),
      metadata: {
        rows,
        checksum,
        duration_ms
      }
    }).eq("id", chunk_id || batch_id).eq("org_id", authResult.device.org_id);
    if (updateErr) throw updateErr;
    const { count: remaining, error: countErr } = await sb.from("batch_chunks").select("*", {
      count: "exact",
      head: true
    }).eq("dataset_id", dataset_id).neq("status", "processed");
    if (countErr) throw countErr;
    if (!remaining || remaining === 0) {
      await sb.from("system_logs").insert([
        {
          event_type: "chunks_processed",
          message: `All chunks processed for dataset ${dataset_id}`,
          created_at: new Date().toISOString()
        }
      ]);
    }
    return new Response(JSON.stringify({
      ok: true,
      message: "Chunk processed successfully and merge check complete"
    }), {
      headers: corsHeaders,
      status: 200
    });
  } catch (err) {
    console.error("[upload_complete] ❌", err.message);
    try {
      await sb.rpc("log_agent_error", {
        _device_id: authResult.device.id,
        _job_id: null,
        _error_message: `upload_complete failed: ${err.message}`
      });
    } catch (logErr) {
      console.error("[upload_complete] ⚠ Failed to log error:", logErr.message);
    }
    return new Response(JSON.stringify({
      ok: false,
      error: err.message
    }), {
      headers: corsHeaders,
      status: 400
    });
  }
});
