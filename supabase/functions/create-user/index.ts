import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 5;
const CLAIM_CODE_LENGTH = 6;
const CLAIM_CODE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
function generateClaimCode() {
  let result = "";
  for(let i = 0; i < CLAIM_CODE_LENGTH; i++){
    result += CLAIM_CODE_CHARS.charAt(Math.floor(Math.random() * CLAIM_CODE_CHARS.length));
  }
  return result;
}
function corsHeaders(req) {
  const origin = req.headers.get("origin") ?? "*";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-forwarded-for",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
    "Content-Type": "application/json"
  };
}
async function checkRateLimit(supabase, clientIP) {
  const now = Date.now();
  const windowStart = new Date(now - RATE_LIMIT_WINDOW_MS).toISOString();
  const { count, error: countErr } = await supabase.from("bootstrap_rate_limits").select("*", {
    count: "exact",
    head: true
  }).eq("client_key", clientIP).gte("created_at", windowStart);
  if (countErr) {
    console.error("[create-user] Rate limit check error:", countErr);
    return true;
  }
  if (count !== null && count >= RATE_LIMIT_MAX_REQUESTS) {
    return false;
  }
  const { error: insertErr } = await supabase.from("bootstrap_rate_limits").insert({
    client_key: clientIP
  });
  if (insertErr) {
    console.error("[create-user] Rate limit insert error:", insertErr);
  }
  return true;
}
serve(async (req)=>{
  const headers = corsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers
    });
  }
  if (req.method !== "POST") {
    return new Response(JSON.stringify({
      error: "Method not allowed"
    }), {
      status: 405,
      headers
    });
  }
  const clientIP = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || "unknown";
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  if (!await checkRateLimit(supabase, clientIP)) {
    return new Response(JSON.stringify({
      ok: false,
      error: "Rate limit exceeded"
    }), {
      status: 429,
      headers
    });
  }
  try {
    const body = await req.json();
    const email = body.email;
    const password = body.password;
    const org_name = body.org_name;
    const team_size = body.team_size;
    const member_name = body.member_name;
    if (!email || !password || !org_name) {
      return new Response(JSON.stringify({
        error: "Missing required fields"
      }), {
        status: 400,
        headers
      });
    }
    /* -------------------------------
     * 1️⃣ Create user
     * ------------------------------- */ const userRes = await supabase.auth.admin.createUser({
      email: email,
      password: password,
      email_confirm: true
    });
    if (userRes.error || !userRes.data.user) {
      throw userRes.error || new Error("User creation failed");
    }
    const userId = userRes.data.user.id;
    try {
      /* -------------------------------
       * 2️⃣ Create org (with claim code)
       * ------------------------------- */ let orgId = null;
      let attempts = 0;
      while(!orgId && attempts < 5){
        const claimCode = generateClaimCode();
        const orgRes = await supabase.from("orgs").insert({
          name: org_name,
          plan: "free",
          team_size: team_size ?? null,
          auth_id: userId,
          claim_secret: claimCode,
          claim_secret_expires_at: new Date(Date.now() + 1000 * 60 * 60 * 24 // 24h expiry
          ).toISOString()
        }).select("id").single();
        if (!orgRes.error && orgRes.data) {
          orgId = orgRes.data.id;
          break;
        }
        // retry only on unique violation
        if (orgRes.error && orgRes.error.code === "23505") {
          attempts++;
          continue;
        }
        throw orgRes.error || new Error("Org creation failed");
      }
      if (!orgId) {
        throw new Error("Failed to generate unique claim code");
      }
      /* -------------------------------
       * 3️⃣ Create org member
       * ------------------------------- */ const memberRes = await supabase.from("org_members").insert({
        org_id: orgId,
        user_id: userId,
        role: "admin",
        member_name: member_name ?? null,
        member_email: email
      });
      if (memberRes.error) {
        throw memberRes.error;
      }
      /* -------------------------------
        * 4️⃣ Register signing key
        * ------------------------------- */ const platformPubKey = Deno.env.get("PLATFORM_SIGNING_PUBLIC_KEY_B64");
      const platformPrivKey = Deno.env.get("PLATFORM_SIGNING_PRIVATE_KEY_B64");
      if (!platformPubKey || !platformPrivKey) {
        throw new Error("PLATFORM_SIGNING_PUBLIC_KEY_B64 or PLATFORM_SIGNING_PRIVATE_KEY_B64 is not set.");
      }
      // Store private key in Vault
      const privKeySecretName = `org_${orgId}_ed25519_priv_${Date.now()}`;
      console.log("[create-user] Storing private key in Vault with name:", privKeySecretName);
      const { data: vaultResult, error: vaultError } = await supabase.rpc("store_plugin_signing_key_to_vault", {
        p_org_id: orgId,
        p_private_key_b64: platformPrivKey,
        p_secret_name: privKeySecretName
      });
      if (vaultError) {
        console.error("[create-user] Vault storage failed:", vaultError);
        throw new Error("[create-user] Failed to store private key in Vault: " + vaultError.message);
      }
      console.log("[create-user] Vault storage result:", JSON.stringify(vaultResult));
      console.log("[create-user] About to insert signing key with vault_secret_name:", privKeySecretName);
      // Insert signing key with vault reference
      console.log("[create-user] Inserting signing key with vault_secret_name:", privKeySecretName);
      const keyRes = await supabase.from("plugin_signing_keys").insert({
        org_id: orgId,
        public_key: platformPubKey,
        algorithm: "ed25519",
        vault_secret_name: privKeySecretName
      });
      if (keyRes.error) {
        console.error("[create-user] Signing key insert error:", keyRes.error);
        throw new Error("[create-user] Failed to insert signing key: " + keyRes.error.message);
      }
      console.log("[create-user] Signing key inserted successfully");
      console.log("[create-user] org + claim code created " + orgId);
      return new Response(JSON.stringify({
        user_id: userId,
        org_id: orgId
      }), {
        status: 200,
        headers
      });
    } catch (err) {
      await supabase.auth.admin.deleteUser(userId);
      throw err;
    }
  } catch (err) {
    return new Response(JSON.stringify({
      error: err instanceof Error ? err.message : "Unexpected error"
    }), {
      status: 500,
      headers
    });
  }
});
