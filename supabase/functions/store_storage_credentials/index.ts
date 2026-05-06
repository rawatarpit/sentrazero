import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
// -------- ENV --------
const supabaseUrl = Deno.env.get("SUPABASE_URL");
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
if (!supabaseUrl || !supabaseKey) {
  throw new Error("Missing SUPABASE env vars");
}
const supabase = createClient(supabaseUrl, supabaseKey);
// -------- CORS --------
const allowedOrigins = [
  "http://localhost:3000",
  "https://www.sentrazero.com"
];
function getCorsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  return {
    "Access-Control-Allow-Origin": allowedOrigins.includes(origin) ? origin : "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type"
  };
}
function jsonResponse(req, body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...getCorsHeaders(req),
      "Content-Type": "application/json"
    }
  });
}
// -------- HANDLER --------
Deno.serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
  try {
    let payload;
    try {
      payload = await req.json();
    } catch  {
      return jsonResponse(req, {
        success: false,
        error: "Invalid JSON body"
      }, 400);
    }
    const { org_id, access_key_id, secret_access_key, provider, bucket_name, region, endpoint, name, is_default = true } = payload;
    // -------- VALIDATION --------
    if (!org_id || !access_key_id || !secret_access_key || !provider) {
      return jsonResponse(req, {
        success: false,
        error: "Missing required fields"
      }, 400);
    }
    // -------- STEP 1: VAULT --------
    const { data, error } = await supabase.rpc("store_s3_credentials_to_vault", {
      p_org_id: org_id,
      p_access_key_id: access_key_id,
      p_secret_access_key: secret_access_key,
      p_provider: provider
    });
    console.log("Vault RPC result:", {
      data,
      error
    });
    if (error) {
      return jsonResponse(req, {
        success: false,
        error: error.message
      }, 500);
    }
    if (!data?.secret_name) {
      return jsonResponse(req, {
        success: false,
        error: "Vault did not return secret_name"
      }, 500);
    }
    const secret_name = data.secret_name;
    // -------- STEP 2: UPSERT CONFIG --------
    const { error: upsertError } = await supabase.from("org_storage_configs").upsert({
      org_id,
      storage_mode: "object_storage",
      provider,
      bucket_name: bucket_name ?? null,
      region: region ?? null,
      endpoint: endpoint ?? null,
      vault_secret_name: secret_name,
      name: name ?? `${provider} storage`,
      is_default
    }, {
      onConflict: "org_id,provider"
    });
    if (upsertError) {
      console.error("Upsert failed:", upsertError);
      return jsonResponse(req, {
        success: false,
        error: upsertError.message
      }, 500);
    }
    // -------- SUCCESS --------
    return jsonResponse(req, {
      success: true,
      secret_name,
      provider
    });
  } catch (err) {
    console.error("Unhandled error:", err);
    return jsonResponse(req, {
      success: false,
      error: err?.message || "Unknown error"
    }, 500);
  }
});
