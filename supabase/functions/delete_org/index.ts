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
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    const { org_id } = await req.json();
    if (!org_id) {
      return new Response(JSON.stringify({
        error: "Missing org_id",
        success: false
      }), {
        status: 400,
        headers: {
          ...corsHeaders,
          "Content-Type": "application/json"
        }
      });
    }
    await supabase.from("datasets").delete().eq("org_id", org_id);
    await supabase.from("org_storage_configs").delete().eq("org_id", org_id);
    await supabase.from("device_benchmarks").delete().eq("org_id", org_id);
    await supabase.from("device_events").delete().eq("org_id", org_id);
    await supabase.from("device_job_performance").delete().eq("org_id", org_id);
    await supabase.from("device_job_type_stats").delete().eq("org_id", org_id);
    await supabase.from("device_policies").delete().eq("org_id", org_id);
    await supabase.from("device_routing_rules").delete().eq("org_id", org_id);
    await supabase.from("device_vectors").delete().eq("org_id", org_id);
    await supabase.from("execution_policies").delete().eq("org_id", org_id);
    await supabase.from("executions").delete().eq("org_id", org_id);
    await supabase.from("org_members").delete().eq("org_id", org_id);
    await supabase.from("org_plugins").delete().eq("org_id", org_id);
    await supabase.from("org_quotas").delete().eq("org_id", org_id);
    await supabase.from("org_usage").delete().eq("org_id", org_id);
    await supabase.from("orgs").delete().eq("id", org_id);
    return new Response(JSON.stringify({
      success: true,
      org_id,
      message: "Organization deleted successfully"
    }), {
      status: 200,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  } catch (error) {
    console.error("Error deleting org:", error);
    return new Response(JSON.stringify({
      error: "Internal server error",
      details: String(error),
      success: false
    }), {
      status: 500,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
});
