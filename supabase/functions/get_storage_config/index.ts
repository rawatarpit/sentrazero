import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response("ok", {
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
    const { org_id, storage_config_id } = body;
    console.log("[get_storage_config] received", {
      org_id,
      storage_config_id
    });
    if (!org_id) {
      return response({
        ok: false,
        error: "MISSING_ORG_ID"
      }, 400);
    }
    console.log("[get_storage_config] stage: fetch_config", {
      org_id,
      storage_config_id
    });
    let query = supabase.from("org_storage_configs").select("*").eq("org_id", org_id);
    if (storage_config_id) {
      query = query.eq("id", storage_config_id);
    } else {
      query = query.eq("is_default", true);
    }
    const { data: config, error: configError } = await query.single();
    if (configError) {
      console.error("[get_storage_config] stage: fetch_config failed", {
        error: configError.message
      });
      return response({
        ok: false,
        error: "STORAGE_CONFIG_NOT_FOUND",
        details: configError.message
      });
    }
    if (!config) {
      return response({
        ok: false,
        error: "STORAGE_CONFIG_NOT_FOUND"
      }, 404);
    }
    console.log("[get_storage_config] stage: fetch_config success", {
      id: config.id,
      hasVaultSecret: !!config.vault_secret_name
    });
    let credentials = null;
    let vaultError = null;
    if (config.vault_secret_name) {
      console.log("[get_storage_config] stage: decrypt_vault", {
        secret_name: config.vault_secret_name
      });
      const { data: decrypted, error: decryptError } = await supabase.rpc("decrypt_vault_secret", {
        secret_name: config.vault_secret_name
      });
      if (decryptError) {
        console.error("[get_storage_config] stage: decrypt_vault failed", {
          error: decryptError.message
        });
        vaultError = decryptError.message;
      } else if (decrypted) {
        try {
          credentials = typeof decrypted === "string" ? JSON.parse(decrypted) : decrypted;
          console.log("[get_storage_config] stage: decrypt_vault success");
        } catch (parseErr) {
          console.error("[get_storage_config] stage: decrypt_vault parse failed", {
            error: String(parseErr)
          });
          vaultError = "Failed to parse credentials";
        }
      }
    }
    if (!credentials && config.storage_mode === "object_storage") {
      console.error("[get_storage_config] stage: validation failed", {
        vaultConfigured: !!config.vault_secret_name,
        vaultError,
        hasCredentials: !!credentials
      });
      return response({
        ok: false,
        error: "STORAGE_CONFIG_UNAVAILABLE",
        details: vaultError || "Vault decryption failed or credentials not found"
      });
    }
    const storageMode = config.storage_mode || config.provider;
    const normalizedMode = storageMode === "aws_s3" ? "s3" : storageMode;
    console.log("[get_storage_config] stage: complete", {
      storageMode: normalizedMode,
      hasCredentials: !!credentials
    });
    return response({
      ok: true,
      id: config.id,
      org_id: config.org_id,
      name: config.name,
      storage_mode: config.storage_mode,
      provider: config.provider,
      bucket_name: config.bucket_name,
      region: config.region,
      endpoint: config.endpoint,
      mount_base_path: config.mount_base_path,
      is_default: config.is_default,
      storageMode: normalizedMode,
      credentials,
      vault_secret_name: config.vault_secret_name
    });
  } catch (error) {
    console.error("[get_storage_config] stage: crash", {
      error: String(error)
    });
    return response({
      ok: false,
      error: "GET_STORAGE_CONFIG_FAILED",
      details: String(error)
    });
  }
});
