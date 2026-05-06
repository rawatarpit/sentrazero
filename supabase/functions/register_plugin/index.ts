import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";

async function signManifest(pluginBinaryBytes, privKeyB64) {
  if (!privKeyB64) {
    console.error("signManifest: No private key provided");
    return null;
  }
  console.log("signManifest: Signing with key length:", privKeyB64.length);
  const hashBuffer = await crypto.subtle.digest("SHA-256", pluginBinaryBytes);
  const privKeyBytes = Uint8Array.from(atob(privKeyB64), (c)=>c.charCodeAt(0));
  const privKey = await crypto.subtle.importKey("pkcs8", privKeyBytes, {
    name: "Ed25519"
  }, false, ["sign"]);
  const sigBuffer = await crypto.subtle.sign({
    name: "Ed25519"
  }, privKey, hashBuffer);
  return btoa(String.fromCharCode(...new Uint8Array(sigBuffer)));
}

serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  try {
    const adminClient = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));

    // Get first org for testing
    const { data: orgData } = await adminClient.from("orgs").select("id").limit(1).single();
    if (!orgData) throw new Error("No org found");
    const org_id = orgData.id;

    const contentType = req.headers.get("content-type") || "";
    let body;
    if (contentType.includes("application/json")) {
      body = await req.json();
    } else {
      const formData = await req.formData();
      body = {
        name: formData.get("name"),
        version: formData.get("version"),
        language: formData.get("language"),
        plugin_type: formData.get("plugin_type"),
        checksum: formData.get("checksum"),
        binary: formData.get("binary"),
      };
    }

    const { name, version, language, plugin_type, checksum, binary } = body;
    if (!name || !version || !language || !plugin_type || !checksum || !binary) {
      return new Response(JSON.stringify({ error: "Missing fields" }), {
        status: 400, headers: corsHeaders
      });
    }

    const pluginId = crypto.randomUUID();
    const storagePath = `plugins/org/${org_id}/${pluginId}/${binary.name}`;

    // Upload binary
    const { error: uploadError } = await adminClient.storage.from("plugins").upload(storagePath, binary);
    if (uploadError) throw new Error(uploadError.message);

    // Get private key from Vault
    const { data: keyData, error: keyError } = await adminClient.from("plugin_signing_keys")
      .select("id, vault_secret_name, org_id")
      .eq("org_id", org_id)
      .is("revoked_at", null)
      .order("created_at", { ascending: false })
      .limit(1)
      .single();

    console.log("Key data query result:", JSON.stringify(keyData), "error:", keyError);

    let privateKeyB64 = null;
    if (keyData?.vault_secret_name) {
      console.log("Fetching Vault secret:", keyData.vault_secret_name);
      const { data: vaultData, error: vaultError } = await adminClient
        .rpc("get_vault_secret", { secret_name: keyData.vault_secret_name });

      if (vaultError) {
        console.error("Failed to get vault secret:", JSON.stringify(vaultError));
      } else if (vaultData) {
        console.log("Vault data type:", typeof vaultData, "raw:", JSON.stringify(vaultData)?.substring(0, 100));
        privateKeyB64 = typeof vaultData === 'string' ? vaultData : (vaultData?.decrypted_secret || vaultData);
        console.log("Private key from Vault:", privateKeyB64 ? "YES (length: " + privateKeyB64.length + ")" : "NO");
        if (privateKeyB64) {
          console.log("Key preview:", privateKeyB64.substring(0, 50) + "...");
        }
      } else {
        console.log("No Vault data returned for secret:", keyData.vault_secret_name);
      }
    } else {
      console.log("No vault_secret_name found in keyData");
    }

    // Sign plugin
    const pluginBytes = new Uint8Array(await binary.arrayBuffer());
    console.log("About to sign - privateKeyB64 exists:", !!privateKeyB64, "length:", privateKeyB64?.length);
    const signature = await signManifest(pluginBytes, privateKeyB64);
    console.log("Signature result:", signature ? "SUCCESS (length: " + signature.length + ")" : "FAILED");

    // Insert plugin
    const { error: insertError } = await adminClient.from("plugins").insert({
      id: pluginId,
      name, version, language, plugin_type,
      storage_path: storagePath,
      checksum,
      signature: signature ? new Uint8Array(atob(signature).split("").map((c)=>c.charCodeAt(0))) : null,
      signature_key_id: keyData?.id,
      signature_verified: !!signature,
      trusted: !!signature,
      created_by: null
    });

    if (insertError) throw new Error(insertError.message);

    return new Response(JSON.stringify({
      ok: true,
      plugin_id: pluginId,
      signature: !!signature,
      debug: {
        hasKey: !!privateKeyB64,
        keyLength: privateKeyB64?.length,
        vaultSecretName: keyData?.vault_secret_name,
        keyDataExists: !!keyData
      }
    }), {
      status: 201, headers: corsHeaders
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: corsHeaders
    });
  }
});
