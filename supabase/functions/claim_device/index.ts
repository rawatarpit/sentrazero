import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { hashToken, validateOrigin } from "../_shared/security.ts";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return jsonResponse({
      ok: false,
      error: "Forbidden"
    }, 403, corsHeaders);
  }
  try {
    const body = await req.json();
    const { claim_code, sysinfo } = body;
    if (!claim_code || typeof claim_code !== 'string' || claim_code.trim() === '') {
      return jsonResponse({
        ok: false,
        error: "Missing or invalid claim_code field"
      }, 400, corsHeaders);
    }
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const { data: orgId, error: validationError } = await supabase.rpc("validate_claim_secret", {
      p_claim: claim_code
    });
    if (validationError) {
      console.error("[claim_device] RPC validation error:", validationError.message, validationError.code);
      return jsonResponse({
        ok: false,
        error: "Claim validation failed: " + validationError.message
      }, 500, corsHeaders);
    }
    if (!orgId) {
      return jsonResponse({
        ok: false,
        error: "Invalid or expired claim code"
      }, 401, corsHeaders);
    }
    const { data: org, error: orgError } = await supabase.from("orgs").select("id, name, plan").eq("id", orgId).maybeSingle();
    if (orgError) {
      console.error("[claim_device] Org query error:", orgError.message, orgError.code);
      return jsonResponse({
        ok: false,
        error: "Failed to fetch organization"
      }, 500, corsHeaders);
    }
    if (!org) {
      return jsonResponse({
        ok: false,
        error: "Organization not found"
      }, 404, corsHeaders);
    }
    const token = crypto.randomUUID().replace(/-/g, "");
    const tokenHash = await hashToken(token);
    console.log("[claim_device] Generated token hash length:", tokenHash.length);
    const devicePayload = {
      org_id: org.id,
      name: sysinfo?.hostname || "Unnamed Device",
      type: sysinfo?.type || "agent",
      status: "online",
      access_token_hash: tokenHash,
      specs: sysinfo || {},
      environment_type: sysinfo?.environment || "local",
      storage_type: sysinfo?.storage || "local",
      network_zone: sysinfo?.network_zone || "default",
      total_cpu_cores: sysinfo?.cpu_cores || 8,
      total_memory_gb: sysinfo?.memory_gb || 32,
      benchmark_score: sysinfo?.benchmark_score || 0,
      merge_capable: sysinfo?.merge_capable ?? true,
      last_seen: new Date().toISOString(),
      last_heartbeat: new Date().toISOString()
    };
    const { data: device, error: insertErr } = await supabase.from("devices").upsert(devicePayload, {
      onConflict: "org_id,name"
    }).select("id, org_id, name, status").maybeSingle();
    if (insertErr) {
      console.error("[claim_device] Upsert Error:", insertErr.message, insertErr.code);
      return jsonResponse({
        ok: false,
        error: "Failed to upsert device: " + insertErr.message
      }, 500, corsHeaders);
    }
    if (!device) {
      console.error("[claim_device] Upsert succeeded but no device returned");
      return jsonResponse({
        ok: false,
        error: "Device upsert failed - no device returned"
      }, 500, corsHeaders);
    }
    if (!device.status) {
      console.error("[claim_device] CRITICAL: Inserted device has no status:", device.id);
      return jsonResponse({
        ok: false,
        error: "Device inserted without status"
      }, 500, corsHeaders);
    }
    console.log("[claim_device] Device registered:", device.id, "status:", device.status);
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY");
    return jsonResponse({
      ok: true,
      device_id: device.id,
      agent_token: token,
      org_id: org.id,
      backend_url: supabaseUrl,
      anon_key: anonKey,
      message: `Device registered under org '${org.name}'`
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[claim_device] Unexpected Error:", err.message, err.stack);
    return jsonResponse({
      ok: false,
      error: err.message
    }, 500, corsHeaders);
  }
});
