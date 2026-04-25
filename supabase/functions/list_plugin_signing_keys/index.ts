import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);

  const origin = req.headers.get("origin");
  if (origin) {
    const allowedOrigins = Deno.env.get("ALLOWED_ORIGINS")?.split(",") ?? [];
    if (
      allowedOrigins.length > 0 &&
      !allowedOrigins.some((o) => origin.trim() === o.trim())
    ) {
      return new Response("Forbidden", { status: 403, headers: corsHeaders });
    }
  }

  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const response = (data: unknown, status = 200) =>
    new Response(JSON.stringify(data), {
      status,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    if (!supabaseUrl || !supabaseServiceKey) {
      return response({ ok: false, error: "MISSING_SUPABASE_CONFIG" }, 500);
    }

    console.log("[list_plugin_signing_keys] stage: auth");

    const authResult = await authenticateDeviceWithDetails(req);
    if (!authResult.device) {
      return response(
        { ok: false, error: authResult.error || "UNAUTHORIZED" },
        401
      );
    }

    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    const orgId = authResult.device.org_id;

    console.log("[list_plugin_signing_keys] stage: fetch", { orgId });

    const { data: signingKeys, error: fetchError } = await supabase
      .from("plugin_signing_keys")
      .select("id, public_key, algorithm, created_at, expires_at, revoked_at")
      .eq("org_id", orgId)
      .is("revoked_at", null)
      .order("created_at", { ascending: false });

    if (fetchError) {
      console.error("[list_plugin_signing_keys] stage: fetch failed", {
        error: fetchError.message,
      });
      return response({
        ok: false,
        error: "FETCH_SIGNING_KEYS_FAILED",
        details: fetchError.message,
      });
    }

    const activeKeys = (signingKeys || []).filter(
      (key) => !key.expires_at || new Date(key.expires_at) > new Date()
    );

    console.log("[list_plugin_signing_keys] stage: complete", {
      total: (signingKeys || []).length,
      active: activeKeys.length,
    });

    return response({
      ok: true,
      data: activeKeys.map((key) => ({
        key_id: key.id,
        public_key: key.public_key,
        algorithm: key.algorithm || "ed25519",
        created_at: key.created_at,
        expires_at: key.expires_at,
      })),
    });
  } catch (error) {
    console.error("[list_plugin_signing_keys] stage: crash", { error: String(error) });
    return response({
      ok: false,
      error: "LIST_PLUGIN_SIGNING_KEYS_FAILED",
      details: String(error),
    });
  }
});