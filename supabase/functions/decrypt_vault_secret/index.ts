import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type"
};
serve(async (req)=>{
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: corsHeaders
    });
  }
  const response = (data, status = 200)=>new Response(JSON.stringify(data), {
      status,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !supabaseServiceKey) {
      return response({
        ok: false,
        error: "MISSING_SUPABASE_CONFIG"
      }, 500);
    }
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    const body = await req.json();
    const { secret_name, org_id } = body;
    console.log("[decrypt_vault_secret] stage: received", {
      secret_name,
      org_id
    });
    if (!secret_name) {
      return response({
        ok: false,
        error: "MISSING_SECRET_NAME"
      }, 400);
    }
    console.log("[decrypt_vault_secret] stage: fetch", {
      secret_name
    });
    const { data: encryptedData, error: rpcError } = await supabase.rpc("decrypt_vault_secret", {
      secret_name: secret_name
    });
    if (rpcError) {
      console.error("[decrypt_vault_secret] stage: fetch failed", {
        error: rpcError.message
      });
      return response({
        ok: false,
        error: "VAULT_FETCH_FAILED",
        details: rpcError.message
      });
    }
    console.log("[decrypt_vault_secret] stage: fetch success", {
      hasData: !!encryptedData,
      type: typeof encryptedData
    });
    if (!encryptedData) {
      return response({
        ok: false,
        error: "SECRET_NOT_FOUND"
      }, 404);
    }
    let decrypted = encryptedData;
    console.log("[decrypt_vault_secret] stage: parse", {
      dataType: typeof encryptedData
    });
    if (typeof encryptedData === "string") {
      try {
        decrypted = JSON.parse(encryptedData);
        console.log("[decrypt_vault_secret] stage: parse success");
      } catch (parseErr) {
        console.error("[decrypt_vault_secret] stage: parse failed", {
          error: String(parseErr)
        });
        return response({
          ok: false,
          error: "INVALID_SECRET_FORMAT",
          details: "Failed to parse secret as JSON"
        });
      }
    }
    console.log("[decrypt_vault_secret] stage: validate", {
      hasKeys: decrypted !== null && typeof decrypted === "object"
    });
    const secret = decrypted;
    if (!secret.access_key_id || !secret.secret_access_key) {
      console.error("[decrypt_vault_secret] stage: validation failed", {
        hasAccessKeyId: !!secret.access_key_id,
        hasSecretAccessKey: !!secret.secret_access_key
      });
      return response({
        ok: false,
        error: "INVALID_SECRET_STRUCTURE",
        details: "Missing access_key_id or secret_access_key"
      });
    }
    if (secret.provider !== "s3") {
      console.error("[decrypt_vault_secret] stage: validation failed", {
        provider: secret.provider
      });
      return response({
        ok: false,
        error: "UNSUPPORTED_PROVIDER",
        details: "Only s3 provider supported"
      });
    }
    console.log("[decrypt_vault_secret] stage: complete", {
      provider: secret.provider
    });
    return response({
      ok: true,
      data: {
        provider: secret.provider,
        created_at: secret.created_at,
        access_key_id: secret.access_key_id,
        secret_access_key: secret.secret_access_key,
        session_token: secret.session_token || ""
      }
    });
  } catch (error) {
    console.error("[decrypt_vault_secret] stage: crash", {
      error: String(error)
    });
    return response({
      ok: false,
      error: "DECRYPT_VAULT_SECRET_FAILED",
      details: String(error)
    });
  }
});
