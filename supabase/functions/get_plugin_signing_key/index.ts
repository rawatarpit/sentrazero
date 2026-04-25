import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);
  
  const origin = req.headers.get("origin");
  if (origin) {
    const allowedOrigins = Deno.env.get("ALLOWED_ORIGINS")?.split(",") ?? [];
    if (allowedOrigins.length > 0 && !allowedOrigins.some(o => origin.trim() === o.trim())) {
      return new Response("Forbidden", { status: 403, headers: corsHeaders });
    }
  }

  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const authResult = await authenticateDeviceWithDetails(req, "id, org_id");
  if (!authResult.device) {
    return new Response(
      JSON.stringify({ error: authResult.error }),
      { status: 401, headers: corsHeaders }
    );
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    const url = new URL(req.url);
    const keyId = url.searchParams.get("key_id");

    if (!keyId) {
      return new Response(JSON.stringify({ error: "key_id is required" }), {
        status: 400,
        headers: corsHeaders
      });
    }

    const { data: signingKey, error } = await supabase
      .from("plugin_signing_keys")
      .select("id, org_id, public_key, algorithm, created_at, expires_at, revoked_at")
      .eq("id", keyId)
      .eq("org_id", authResult.device.org_id)
      .eq("revoked_at", null)
      .maybeSingle();

    if (error) throw error;
    if (!signingKey) {
      return new Response(JSON.stringify({ error: "Signing key not found or access denied" }), {
        status: 404,
        headers: corsHeaders
      });
    }

    if (signingKey.expires_at && new Date(signingKey.expires_at) < new Date()) {
      return new Response(JSON.stringify({ error: "Signing key expired" }), {
        status: 410,
        headers: corsHeaders
      });
    }

    return new Response(JSON.stringify({
      key_id: signingKey.id,
      public_key: signingKey.public_key,
      algorithm: signingKey.algorithm || "ed25519"
    }), { headers: corsHeaders });
  } catch (err) {
    console.error("[get_plugin_signing_key] ❌", err.message);
    return new Response(JSON.stringify({ ok: false, error: err.message }), {
      headers: corsHeaders,
      status: 500
    });
  }
});
