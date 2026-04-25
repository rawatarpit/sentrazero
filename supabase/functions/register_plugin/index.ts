import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";

// ---------------------------------------------------------------------------
// Platform signing helpers (unchanged)
// ---------------------------------------------------------------------------

async function signManifest(
  name: string,
  version: string,
  filename: string,
  checksum: string
): Promise<string | null> {
  const privKeyB64 = Deno.env.get("PLATFORM_SIGNING_PRIVATE_KEY_B64");
  if (!privKeyB64) return null;

  const signingData = `${name}|${version}|${filename}|${checksum}`;
  const msgBytes = new TextEncoder().encode(signingData);
  const hashBuffer = await crypto.subtle.digest("SHA-256", msgBytes);

  const privKeyBytes = Uint8Array.from(atob(privKeyB64), (c) =>
    c.charCodeAt(0)
  );

  const privKey = await crypto.subtle.importKey(
    "pkcs8",
    privKeyBytes,
    { name: "Ed25519" },
    false,
    ["sign"]
  );

  const sigBuffer = await crypto.subtle.sign(
    { name: "Ed25519" },
    privKey,
    hashBuffer
  );

  return btoa(String.fromCharCode(...new Uint8Array(sigBuffer)));
}

async function getOrgSigningKeyId(
  supabase: ReturnType<typeof createClient>,
  orgId: string
): Promise<string | null> {
  const { data } = await supabase
    .from("plugin_signing_keys")
    .select("id")
    .eq("org_id", orgId)
    .is("revoked_at", null)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  return data?.id ?? null;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);

  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    // -----------------------------------------------------------------------
    // 1. Validate JWT (Dashboard User)
    // -----------------------------------------------------------------------

    const supabaseUserClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_ANON_KEY")!,
      {
        global: {
          headers: {
            Authorization: req.headers.get("Authorization")!,
          },
        },
      }
    );

    const {
      data: { user },
      error: userError,
    } = await supabaseUserClient.auth.getUser();

    if (userError || !user) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 401,
        headers: corsHeaders,
      });
    }

    // -----------------------------------------------------------------------
    // 2. Resolve org + role (CRITICAL)
    // -----------------------------------------------------------------------

    const adminClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const { data: orgMember, error: orgError } = await adminClient
      .from("org_members")
      .select("org_id, role")
      .eq("user_id", user.id)
      .single();

    if (orgError || !orgMember) {
      return new Response(JSON.stringify({ error: "No org access" }), {
        status: 403,
        headers: corsHeaders,
      });
    }

    if (orgMember.role !== "admin") {
      return new Response(JSON.stringify({ error: "Admin only" }), {
        status: 403,
        headers: corsHeaders,
      });
    }

    const org_id = orgMember.org_id;

    // -----------------------------------------------------------------------
    // 3. Parse body
    // -----------------------------------------------------------------------

    const contentType = req.headers.get("content-type") || "";
    let body: any;

    if (contentType.includes("application/json")) {
      body = await req.json();
    } else {
      const formData = await req.formData();
      body = {
        name: formData.get("name"),
        version: formData.get("version"),
        language: formData.get("language"),
        plugin_type: formData.get("plugin_type"),
        description: formData.get("description"),
        config_schema: formData.get("config_schema"),
        binary: formData.get("binary"),
        checksum: formData.get("checksum"),
        network: formData.get("network"),
        resources: formData.get("resources"),
        runtime_dependencies: formData.get("runtime_dependencies"),
        signature_key_id: formData.get("signature_key_id"),
      };
    }

    const {
      name,
      version,
      language,
      plugin_type,
      description,
      config_schema,
      binary,
      checksum,
      network,
      resources,
      runtime_dependencies,
      signature_key_id,
    } = body;

    if (!name || !version || !language || !plugin_type) {
      return new Response(
        JSON.stringify({
          error: "Missing required fields",
        }),
        { status: 400, headers: corsHeaders }
      );
    }

    if (!checksum) {
      return new Response(
        JSON.stringify({ error: "checksum is required" }),
        { status: 400, headers: corsHeaders }
      );
    }

    // -----------------------------------------------------------------------
    // 4. Upload binary
    // -----------------------------------------------------------------------

    const pluginId = crypto.randomUUID();
    const filename = (binary as File)?.name ?? `${name}-${version}`;
    const storagePath = `plugins/org/${org_id}/${pluginId}/${filename}`;

    if (binary) {
      const { error: uploadError } = await adminClient.storage
        .from("plugins")
        .upload(storagePath, binary, {
          contentType: "application/octet-stream",
          upsert: true,
        });

      if (uploadError) {
        throw new Error(uploadError.message);
      }
    }

    // -----------------------------------------------------------------------
    // 5. Signed URL (used in signature)
    // -----------------------------------------------------------------------

    const { data: signedUrlData } = await adminClient.storage
      .from("plugins")
      .createSignedUrl(storagePath, 60 * 60 * 24 * 365);

    const pluginUrl = signedUrlData?.signedUrl ?? storagePath;

    // -----------------------------------------------------------------------
    // 6. Sign plugin
    // -----------------------------------------------------------------------

    const [signature, orgSigningKeyId] = await Promise.all([
      signManifest(name, version, filename, checksum),
      getOrgSigningKeyId(adminClient, org_id),
    ]);

    const finalSignatureKeyId = signature_key_id || orgSigningKeyId;

    // -----------------------------------------------------------------------
    // 7. Parse resources
    // -----------------------------------------------------------------------

    let parsedResources: Record<string, unknown> = {};
    if (resources) {
      try {
        parsedResources =
          typeof resources === "string" ? JSON.parse(resources) : resources;
      } catch {}
    }

    // -----------------------------------------------------------------------
    // 8. Parse runtime dependencies
    // -----------------------------------------------------------------------

    let parsedRuntimeDeps: string[] = [];
    if (runtime_dependencies) {
      try {
        parsedRuntimeDeps =
          typeof runtime_dependencies === "string"
            ? JSON.parse(runtime_dependencies)
            : runtime_dependencies;
      } catch {}
    }

    // -----------------------------------------------------------------------
    // 9. Insert plugin
    // -----------------------------------------------------------------------

    const { error: insertError } = await adminClient.from("plugins").insert({
      id: pluginId,
      name,
      version,
      language,
      plugin_type,
      description: description ?? null,
      config_schema: config_schema
        ? JSON.parse(config_schema as string)
        : null,
      storage_path: storagePath,
      checksum,
      signature: signature
        ? new Uint8Array(
            atob(signature)
              .split("")
              .map((c) => c.charCodeAt(0))
          )
        : null,
      signature_key_id: finalSignatureKeyId,
      signature_verified: !!signature,
      trusted: !!signature,
      network: network === "true" || network === true,
      resources: parsedResources,
      runtime_dependencies: parsedRuntimeDeps,
      created_by: user.id,
    });

    if (insertError) {
      throw new Error(insertError.message);
    }

    // -----------------------------------------------------------------------
    // 9. Enable plugin for org
    // -----------------------------------------------------------------------

    await adminClient.from("org_plugins").insert({
      org_id,
      plugin_id: pluginId,
      enabled: true,
      rollout_percentage: 100,
    });

    // -----------------------------------------------------------------------
    // 10. Response
    // -----------------------------------------------------------------------

    return new Response(
      JSON.stringify({
        ok: true,
        plugin_id: pluginId,
        storage_path: storagePath,
        signature_verified: !!signature,
        trusted: !!signature,
      }),
      { status: 201, headers: corsHeaders }
    );
  } catch (err: any) {
    console.error("[register_plugin] ❌", err.message);
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: corsHeaders,
    });
  }
});