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

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!SUPABASE_URL || !SERVICE_KEY) {
    return jsonResponse({ ok: false, error: "server misconfigured" }, 500, corsHeaders);
  }

  const authResult = await authenticateDeviceWithDetails(req, "id, org_id");

  if (!authResult.device) {
    return jsonResponse({ ok: false, error: authResult.error }, 401, corsHeaders);
  }

  const deviceId = authResult.device.id;
  const orgId = authResult.device.org_id;

  const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
    global: { headers: { "x-client-info": "reconcile_agent/1.0.0" } },
  });

  const { data: staleJobs, error: staleErr } = await supabase
    .from("agent_jobs")
    .update({ status: "pending", agent_id: null, lease_expires_at: null })
    .eq("agent_id", deviceId)
    .eq("org_id", orgId)
    .in("status", ["assigned", "running"])
    .lt("lease_expires_at", new Date().toISOString())
    .select("id");

  if (staleErr) {
    console.error("[reconcile_agent] Failed to restore stale jobs:", staleErr.message);
    return jsonResponse({ ok: false, error: "Failed to restore stale jobs: " + staleErr.message }, 500, corsHeaders);
  }

  const restoredCount = staleJobs?.length ?? 0;

  await supabase
    .from("devices")
    .update({ last_refresh: new Date().toISOString() })
    .eq("id", deviceId)
    .eq("org_id", orgId);

  return jsonResponse({
    ok: true,
    restored_jobs: restoredCount,
    device_id: deviceId,
    org_id: orgId,
  }, 200, corsHeaders);
});
