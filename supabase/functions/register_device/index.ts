import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { hashToken, validateOrigin } from "../_shared/security.ts";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";

function randomVector(dim = 16) {
  const v = Array(dim)
    .fill(0)
    .map(() => Math.random() - 0.5);
  const norm = Math.sqrt(v.reduce((a, b) => a + b * b, 0));
  return v.map((x) => x / (norm || 1));
}

serve(async (req) => {
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  };

  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }

  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return jsonResponse({ ok: false, error: "Forbidden" }, 403, corsHeaders);
  }

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const authHeader = req.headers.get("Authorization");
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return jsonResponse({ ok: false, error: "Missing Authorization header" }, 401, corsHeaders);
    }
    const claimCode = authHeader.split(" ")[1]?.trim();

    if (!claimCode) {
      return jsonResponse({ ok: false, error: "Invalid claim code" }, 401, corsHeaders);
    }

    const { data: org, error: orgError } = await supabase
      .from("orgs")
      .select("id, name, claim_secret")
      .eq("claim_secret", claimCode)
      .maybeSingle();

    if (orgError) {
      console.error("[register_device] Org query error:", orgError.message, orgError.code)
      return jsonResponse({ ok: false, error: "Organization lookup failed" }, 500, corsHeaders);
    }

    if (!org) {
      return jsonResponse({ ok: false, error: "Invalid or expired claim code" }, 401, corsHeaders);
    }

    const body = await req.json().catch(() => ({}));
    const {
      org_id,
      name,
      environment_type,
      storage_type,
      capabilities,
      specs,
      benchmark_score,
      force_reclaim,
    } = body;

    if (!name) {
      return jsonResponse({ ok: false, error: "Missing device name" }, 400, corsHeaders);
    }

    const { data: existingDevice, error: existingError } = await supabase
      .from("devices")
      .select("id, name, status, last_seen")
      .eq("org_id", org.id)
      .eq("name", name)
      .maybeSingle();

    if (existingError) {
      console.error("[register_device] Existing device query error:", existingError.message)
    }

    if (existingDevice && !force_reclaim) {
      const lastSeen = new Date(existingDevice.last_seen || 0);
      const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);
      
      if (lastSeen > twoHoursAgo) {
        return jsonResponse({
          ok: false,
          error: "Device with this name already exists and is active. Use force_reclaim=true to override.",
          existing_device_id: existingDevice.id,
        }, 409, corsHeaders);
      }
      
      console.log(`[register_device] Reclaiming inactive device ${existingDevice.id}`);
      
      const token = crypto.randomUUID().replace(/-/g, "").slice(0, 32);
      const device_vector = randomVector();
      const tokenHash = await hashToken(token);
      
      const { data: device, error: updateError } = await supabase
        .from("devices")
        .update({
          access_token_hash: tokenHash,
          status: "online",
          last_seen: new Date().toISOString(),
          specs: specs ?? {},
          environment_type: environment_type ?? "local",
          storage_type: storage_type ?? "disk",
          benchmark_score: benchmark_score ?? 0,
          capabilities: capabilities ?? [],
          device_vector,
        })
        .eq("id", existingDevice.id)
        .select("id, org_id, name, status")
        .maybeSingle();

      if (updateError) {
        console.error("[register_device] Update error:", updateError.message, updateError.code)
        return jsonResponse({ ok: false, error: "Failed to reclaim device: " + updateError.message }, 500, corsHeaders);
      }

      if (!device) {
        return jsonResponse({ ok: false, error: "Device reclaim failed - no device returned" }, 500, corsHeaders);
      }

      console.log("[register_device] Device reclaimed:", device.id);

      return jsonResponse({
        ok: true,
        device_id: device.id,
        access_token: token,
        message: `Device '${name}' reclaimed successfully`,
        reclaimed: true,
      }, 200, corsHeaders);
    }

    const token = crypto.randomUUID().replace(/-/g, "").slice(0, 32);
    const device_vector = randomVector();
    const tokenHash = await hashToken(token);

    const { data: device, error: insertError } = await supabase
      .from("devices")
      .insert({
        org_id: org.id,
        name,
        type: environment_type ?? "agent",
        status: "online",
        access_token_hash: tokenHash,
        last_seen: new Date().toISOString(),
        specs: specs ?? {},
        environment_type: environment_type ?? "local",
        storage_type: storage_type ?? "disk",
        benchmark_score: benchmark_score ?? 0,
        capabilities: capabilities ?? [],
        device_vector,
      })
      .select("id, org_id, name, status")
      .maybeSingle();

    if (insertError) {
      console.error("[register_device] Insert error:", insertError.message, insertError.code)
      return jsonResponse({ ok: false, error: "Failed to insert device: " + insertError.message }, 500, corsHeaders);
    }

    if (!device) {
      return jsonResponse({ ok: false, error: "Device insert failed - no device returned" }, 500, corsHeaders);
    }

    if (!device.status) {
      console.error("[register_device] CRITICAL: Inserted device has no status:", device.id)
      return jsonResponse({ ok: false, error: "Device inserted without status" }, 500, corsHeaders);
    }

    console.log("[register_device] Device registered:", device.id, "status:", device.status);

    return jsonResponse({
      ok: true,
      device_id: device.id,
      access_token: token,
      message: `Device registered successfully under org '${org.name}'`,
    }, 200, corsHeaders);
  } catch (err) {
    console.error("[register_device] Unexpected error:", err.message, err.stack);
    return jsonResponse({ ok: false, error: err.message }, 500, corsHeaders);
  }
});
