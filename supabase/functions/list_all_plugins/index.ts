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
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    // Get user's org from auth header
    const authHeader = req.headers.get("Authorization") || "";
    let orgId = null;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      const userClient = createClient(supabaseUrl, Deno.env.get("SUPABASE_ANON_KEY"), {
        global: {
          headers: {
            Authorization: authHeader
          }
        }
      });
      const { data: { user } } = await userClient.auth.getUser();
      if (user) {
        const { data: orgMember } = await supabase.from("org_members").select("org_id").eq("user_id", user.id).maybeSingle();
        orgId = orgMember?.org_id ?? null;
      }
    }
    // Get built-in plugins (created_by is null - inserted by superadmin)
    const { data: builtInPlugins, error: builtInError } = await supabase.from("plugins").select("*").is("created_by", null).order("name", {
      ascending: true
    });
    if (builtInError) {
      return new Response(JSON.stringify({
        error: "Failed to fetch built-in plugins",
        details: builtInError.message
      }), {
        status: 500,
        headers: {
          ...corsHeaders,
          "Content-Type": "application/json"
        }
      });
    }
    let orgPlugins = [];
    // Get org-specific plugins if orgId found
    if (orgId) {
      const { data: orgPluginRows, error: orgError } = await supabase.from("org_plugins").select("plugin_id, enabled, rollout_percentage").eq("org_id", orgId);
      if (!orgError && orgPluginRows && orgPluginRows.length > 0) {
        const pluginIds = orgPluginRows.map((op)=>op.plugin_id);
        const { data: pluginRows, error: pluginError } = await supabase.from("plugins").select("*").in("id", pluginIds);
        if (!pluginError && pluginRows) {
          orgPlugins = pluginRows.map((p)=>({
              ...p,
              enabled: orgPluginRows.find((op)=>op.plugin_id === p.id)?.enabled ?? true,
              rollout_percentage: orgPluginRows.find((op)=>op.plugin_id === p.id)?.rollout_percentage ?? 100
            }));
        }
      }
    }
    // Mark built-in vs org
    const builtInMarked = (builtInPlugins || []).map((p)=>({
        ...p,
        plugin_source: "built-in",
        enabled: true,
        rollout_percentage: 100,
        org_plugin_id: null
      }));
    const orgMarked = orgPlugins.map((p)=>({
        ...p,
        plugin_source: "org",
        org_plugin_id: p.id
      }));
    const allPlugins = [
      ...builtInMarked,
      ...orgMarked
    ];
    return new Response(JSON.stringify({
      plugins: allPlugins
    }), {
      status: 200,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  } catch (error) {
    console.error("Error listing all plugins:", error);
    return new Response(JSON.stringify({
      error: "Internal server error",
      details: String(error)
    }), {
      status: 500,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
});
