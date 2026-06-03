import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
  try {
    const adminClient = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const { data: orgData } = await adminClient.from("orgs").select("id").limit(1).single();
    if (!orgData) throw new Error("No org found");
    const org_id = orgData.id;
    const contentType = req.headers.get("content-type") || "";
    let name, version, language, plugin_type, checksum, resources, network, os, arch, plugin_group, runtime_type, runtime_dependencies, execution_modes, plugin_entrypoint, category, config_schema, input_schema, output_schema, description;
    let binaryBytes = null;
    let binaryName = "plugin.py";
    if (contentType.includes("application/json")) {
      const body = await req.json();
      ({ name, version, language, plugin_type, checksum, resources, network, os, arch, plugin_group, runtime_type, runtime_dependencies, execution_modes, plugin_entrypoint, category, config_schema, input_schema, output_schema, description } = body);
      if (body.binary && typeof body.binary === "object" && body.binary.data) {
        const base64Data = body.binary.data;
        binaryName = body.binary.name || "plugin.py";
        binaryBytes = Uint8Array.from(atob(base64Data), (c)=>c.charCodeAt(0));
      } else if (body.binary && typeof body.binary === "object" && body.binary.arrayBuffer) {
        binaryName = body.binary.name || "plugin.py";
        binaryBytes = new Uint8Array(await body.binary.arrayBuffer());
      }
    } else {
      const formData = await req.formData();
      name = formData.get("name");
      version = formData.get("version");
      language = formData.get("language");
      plugin_type = formData.get("plugin_type");
      checksum = formData.get("checksum");
      resources = formData.get("resources");
      network = formData.get("network");
      os = formData.get("os");
      arch = formData.get("arch");
      plugin_group = formData.get("plugin_group");
      runtime_type = formData.get("runtime_type");
      runtime_dependencies = formData.get("runtime_dependencies");
      execution_modes = formData.get("execution_modes");
      plugin_entrypoint = formData.get("plugin_entrypoint");
      category = formData.get("category");
      config_schema = formData.get("config_schema");
      input_schema = formData.get("input_schema");
      output_schema = formData.get("output_schema");
      description = formData.get("description");
      const bin = formData.get("binary");
      if (bin) {
        binaryName = bin.name || "plugin.py";
        binaryBytes = new Uint8Array(await bin.arrayBuffer());
      }
    }
    if (!name || !version || !language || !plugin_type || !checksum || !binaryBytes) {
      return new Response(JSON.stringify({
        error: "Missing required fields"
      }), {
        status: 400,
        headers: corsHeaders
      });
    }
    if (typeof network === "string") network = network === "true" || network === "1";
    if (typeof os === "string") os = os === "" ? null : os;
    if (typeof arch === "string") arch = arch === "" ? null : arch;
    const pluginId = crypto.randomUUID();
    const storagePath = `plugins/org/${org_id}/${pluginId}/${binaryName}`;
    const { error: uploadError } = await adminClient.storage.from("plugins").upload(storagePath, binaryBytes, {
      contentType: "application/octet-stream"
    });
    if (uploadError) {
      console.error("Storage upload error:", uploadError.message);
      throw new Error("Storage upload failed: " + uploadError.message);
    }
    const { error: insertError } = await adminClient.from("plugins").insert({
      id: pluginId,
      name,
      version,
      language,
      plugin_type,
      storage_path: storagePath,
      checksum,
      signature: null,
      signature_key_id: null,
      signature_verified: false,
      trusted: true,
      created_by: null,
      resources: typeof resources === "string" ? JSON.parse(resources) : resources,
      network: network ?? false,
      os: os || "any",
      arch: arch || "any",
      plugin_group: plugin_group ?? null,
      runtime_type: runtime_type ?? null,
      runtime_dependencies: typeof runtime_dependencies === "string" ? JSON.parse(runtime_dependencies) : runtime_dependencies,
      execution_modes: typeof execution_modes === "string" ? JSON.parse(execution_modes) : execution_modes,
      plugin_entrypoint: plugin_entrypoint ?? null,
      category: category || "general",
      config_schema: typeof config_schema === "string" ? JSON.parse(config_schema) : config_schema,
      input_schema: typeof input_schema === "string" ? JSON.parse(input_schema) : input_schema,
      output_schema: typeof output_schema === "string" ? JSON.parse(output_schema) : output_schema,
      description: description ?? null
    });
    if (insertError) throw new Error("Insert failed: " + insertError.message);
    const { error: orgPluginError } = await adminClient.from("org_plugins").insert({
      org_id,
      plugin_id: pluginId,
      enabled: true,
      rollout_percentage: 100
    });
    if (orgPluginError) throw new Error("org_plugins insert failed: " + orgPluginError.message);
    return new Response(JSON.stringify({
      ok: true,
      plugin_id: pluginId
    }), {
      status: 201,
      headers: corsHeaders
    });
  } catch (err) {
    console.error("register_plugin error:", err);
    return new Response(JSON.stringify({
      error: err.message
    }), {
      status: 500,
      headers: corsHeaders
    });
  }
});
