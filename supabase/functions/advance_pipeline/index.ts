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
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const { execution_id } = await req.json();
    if (!execution_id) {
      throw new Error("Missing execution_id");
    }
    const { data: execution, error: execErr } = await supabase.from("executions").select("id, org_id, dataset_id, current_step_index, total_steps, status").eq("id", execution_id).eq("org_id", authResult.orgId).single();
    if (execErr || !execution) {
      throw new Error("Execution not found or access denied");
    }
    if (execution.status !== "running") {
      return ok({
        message: "Execution is not running",
        execution_id
      }, corsHeaders);
    }
    const stepIndex = execution.current_step_index;
    // FIX 1: Use .filter() with the correct PostgREST JSONB arrow syntax.
    // .eq("payload->>field", value) is NOT parsed correctly by the JS client —
    // it treats the whole string as a column name. .filter() passes it as-is
    // to the PostgREST query engine which correctly interprets the ->> operator.
    const { data: unfinishedJobs, error: jobsErr } = await supabase.from("agent_jobs").select("id").eq("execution_id", execution_id) // FIX 2: execution_id is a real column — use .eq() directly
    .filter("payload->>step_index", "eq", String(stepIndex)) // step_index lives in payload JSONB
    .neq("status", "completed");
    if (jobsErr) throw jobsErr;
    // FIX 3: unfinishedJobs can be null if the query returns no rows;
    // guard against calling .length on null.
    if ((unfinishedJobs ?? []).length > 0) {
      return ok({
        message: "Current step still in progress",
        execution_id,
        step_index: stepIndex
      }, corsHeaders);
    }
    // Mark the current step completed
    await supabase.from("execution_steps").update({
      status: "completed",
      completed_at: new Date().toISOString()
    }).eq("execution_id", execution_id).eq("step_index", stepIndex);
    const nextStepIndex = stepIndex + 1;
    // All steps done — mark the whole execution complete
    if (nextStepIndex >= execution.total_steps) {
      await supabase.from("executions").update({
        status: "completed",
        completed_at: new Date().toISOString()
      }).eq("id", execution_id);
      return ok({
        message: "Execution completed",
        execution_id
      }, corsHeaders);
    }
    // Advance the step index
    await supabase.from("executions").update({
      current_step_index: nextStepIndex
    }).eq("id", execution_id);
    // Load all step definitions for the planner
    const { data: stepDefs, error: stepsErr } = await supabase.from("execution_steps").select("step_index, step_type, plugin_id, script_id, config").eq("execution_id", execution_id).order("step_index");
    if (stepsErr) throw stepsErr;
    // FIX 4: plan_dataset_chunks is an idempotent guard — if chunks already exist
    // for this dataset it returns early. Safe to call on advance.
    const plannerRes = await fetch(`${Deno.env.get("SUPABASE_URL")}/functions/v1/plan_dataset_chunks`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-relay-key": Deno.env.get("RELAY_WEBHOOK_SECRET"),
        "x-org-id": authResult.orgId
      },
      body: JSON.stringify({
        dataset_id: execution.dataset_id,
        execution_id,
        step_index: nextStepIndex,
        steps: stepDefs
      })
    });
    if (!plannerRes.ok) {
      const text = await plannerRes.text();
      throw new Error(`Planner failed for next step: ${text}`);
    }
    return ok({
      message: "Advanced to next step",
      execution_id,
      step_index: nextStepIndex
    }, corsHeaders);
  } catch (err) {
    console.error("[advance_pipeline] ❌", err.message);
    return edgeError(err.message, corsHeaders);
  }
});
// deno-lint-ignore no-explicit-any
function ok(payload, corsHeaders) {
  return new Response(JSON.stringify({
    ok: true,
    ...payload
  }), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    }
  });
}
function edgeError(message, corsHeaders) {
  return new Response(JSON.stringify({
    ok: false,
    error: message
  }), {
    status: 400,
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    }
  });
}
