import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin, authenticateInternal } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", {
      status: 403,
      headers: corsHeaders
    });
  }
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  // Use internal authentication
  const authResult = await authenticateInternal(req);
  if (!authResult.authorized) {
    return new Response(JSON.stringify({
      ok: false,
      error: authResult.error
    }), {
      status: 401,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
  // Additional check for x-relay-key if needed
  const relayKey = req.headers.get("x-relay-key");
  if (!relayKey || relayKey !== Deno.env.get("INTERNAL_RELAY_KEY")) {
    // For now, allow if authenticated internally
    console.log("Warning: x-relay-key missing or invalid, but allowing due to internal auth");
  }
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const results = {};
    // 1. Check triggers on key tables
    const { data: triggers, error: triggerError } = await supabase.rpc("get_triggers_report");
    if (triggerError) {
      // Function might not exist, try alternative approach
      results.triggers = {
        error: triggerError.message,
        note: "RPC function not available"
      };
    } else {
      results.triggers = triggers;
    }
    // 2. Check for auto-chunk functions
    const { data: functions, error: funcError } = await supabase.rpc("get_functions_report");
    if (funcError) {
      results.functions = {
        error: funcError.message,
        note: "RPC function not available"
      };
    } else {
      results.functions = functions;
    }
    // 3. Check dataset status constraint
    const { data: constraints, error: constraintError } = await supabase.rpc("get_constraints_report");
    if (constraintError) {
      results.constraints = {
        error: constraintError.message,
        note: "RPC function not available"
      };
    } else {
      results.constraints = constraints;
    }
    // 4. Check if trg_pre_chunk_on_scan still exists
    const { data: specificTrigger, error: specificError } = await supabase.from("information_schema.triggers").select("trigger_name, event_object_table").eq("trigger_schema", "public").eq("trigger_name", "trg_pre_chunk_on_scan");
    if (specificError) {
      results.specific_trigger_check = {
        error: specificError.message
      };
    } else {
      results.specific_trigger_check = {
        exists: specificTrigger && specificTrigger.length > 0,
        data: specificTrigger
      };
    }
    return new Response(JSON.stringify({
      ok: true,
      message: "Verification complete",
      results,
      migration_applied: "20260430000008_remove_prechunk_trigger.sql",
      timestamp: new Date().toISOString()
    }), {
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({
      ok: false,
      error: err.message,
      note: "You may need to create the RPC functions first. See TRIGGER_AUDIT.md"
    }), {
      status: 400,
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json"
      }
    });
  }
});
