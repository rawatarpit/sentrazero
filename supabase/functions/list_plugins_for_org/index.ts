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
    let org_id = null;
    if (req.method === "GET") {
      const url = new URL(req.url);
      org_id = url.searchParams.get("org_id");
    } else {
      try {
        const body = await req.json();
        org_id = body?.org_id;
      } catch  {
        org_id = null;
      }
    }
    console.log("[list_plugins_for_org] received", {
      org_id
    });
    if (!org_id) {
      return response({
        ok: false,
        error: "MISSING_ORG_ID"
      }, 400);
    }
    console.log("[list_plugins_for_org] stage: fetch_builtin");
    const { data: builtInPlugins, error: builtInError } = await supabase.from("plugins").select("*").eq("plugin_group", "builtin").eq("trusted", true);
    if (builtInError) {
      console.error("[list_plugins_for_org] stage: fetch_builtin failed", {
        error: builtInError.message
      });
      return response({
        ok: false,
        error: "FETCH_BUILTIN_PLUGINS_FAILED",
        details: builtInError.message
      });
    }
    console.log("[list_plugins_for_org] stage: fetch_org_plugins", {
      org_id
    });
    const { data: orgPluginData, error: orgPluginError } = await supabase.from("org_plugins").select("plugin_id, enabled, rollout_percentage").eq("org_id", org_id).eq("enabled", true);
    if (orgPluginError) {
      console.error("[list_plugins_for_org] stage: fetch_org_plugins failed", {
        error: orgPluginError.message
      });
    }
    let orgPluginMap = {};
    let orgPluginIds = [];
    if (orgPluginData && orgPluginData.length > 0) {
      for (const op of orgPluginData){
        orgPluginIds.push(op.plugin_id);
        orgPluginMap[op.plugin_id] = {
          enabled: op.enabled,
          rollout_percentage: op.rollout_percentage
        };
      }
    }
    let orgPlugins = [];
    if (orgPluginIds.length > 0) {
      console.log("[list_plugins_for_org] stage: fetch_org_plugin_details", {
        count: orgPluginIds.length
      });
      const { data: pluginData, error: pluginError } = await supabase.from("plugins").select("*").in("id", orgPluginIds).eq("trusted", true);
      if (pluginError) {
        console.error("[list_plugins_for_org] stage: fetch_org_plugin_details failed", {
          error: pluginError.message
        });
      } else if (pluginData) {
        orgPlugins = pluginData;
      }
    }
    const allPlugins = [
      ...builtInPlugins || [],
      ...orgPlugins
    ];
    console.log("[list_plugins_for_org] stage: process_plugins", {
      total: allPlugins.length
    });
    if (!allPlugins || allPlugins.length === 0) {
      return response({
        ok: true,
        data: []
      });
    }
    const results = [];
    for (const p of allPlugins){
      try {
        const orgPlugin = orgPluginMap[p.id];
        const rollout = orgPlugin?.rollout_percentage ?? 100;
        if (!p.storage_path) {
          console.warn("[list_plugins_for_org] skipping (no storage_path):", p.name);
          continue;
        }
        console.log("[list_plugins_for_org] stage: sign_url", {
          plugin: p.name
        });
        let signedUrl = null;
        try {
          const { data: signed, error: signError } = await supabase.storage.from("plugins").createSignedUrl(p.storage_path, 60 * 10);
          if (!signError && signed?.signedUrl) {
            signedUrl = signed.signedUrl;
          } else if (signError) {
            console.warn("[list_plugins_for_org] sign_url error:", p.name, signError.message);
          }
        } catch (storageError) {
          console.warn("[list_plugins_for_org] storage error:", p.name, storageError);
        }
        if (!signedUrl) {
          console.warn("[list_plugins_for_org] skipping (no signed URL):", p.name);
          continue;
        }
        results.push({
          id: p.id,
          name: p.name,
          version: p.version,
          language: p.language,
          plugin_type: p.plugin_type,
          storage_path: p.storage_path,
          checksum: p.checksum,
          signature: p.signature ? btoa(String.fromCharCode(...new Uint8Array(p.signature))) : null,
          signature_key_id: p.signature_key_id,
          resources: p.resources,
          trusted: p.trusted,
          rollout_percentage: rollout,
          signed_url: signedUrl,
          os: p.os,
          arch: p.arch,
          plugin_group: p.plugin_group,
          network: p.network
        });
      } catch (pluginError) {
        console.error("[list_plugins_for_org] plugin error:", p.name, pluginError);
        continue;
      }
    }
    console.log("[list_plugins_for_org] stage: complete", {
      count: results.length
    });
    return response({
      ok: true,
      data: results
    });
  } catch (error) {
    console.error("[list_plugins_for_org] stage: crash", {
      error: String(error)
    });
    return response({
      ok: false,
      error: "LIST_PLUGINS_FOR_ORG_FAILED",
      details: String(error)
    });
  }
});
