import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
serve(async (req)=>{
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  const results = {};
  const testUuid = "00000000-0000-0000-0000-000000000001";
  // Test 1: start_job - check for overloads
  try {
    const { data, error } = await supabase.rpc("start_job", {
      p_job_id: testUuid,
      p_agent_id: null
    });
    results.start_job = {
      data,
      error: error ? {
        message: error.message,
        code: error.code
      } : null
    };
  } catch (e) {
    results.start_job = {
      catch: String(e)
    };
  }
  // Test 2: get_pipeline_status
  try {
    const { data, error } = await supabase.rpc("get_pipeline_status", {
      p_org_id: testUuid
    });
    results.get_pipeline_status = {
      data,
      error: error ? {
        message: error.message,
        code: error.code
      } : null
    };
  } catch (e) {
    results.get_pipeline_status = {
      catch: String(e)
    };
  }
  // Test 3: pre_chunk_dataset_smart
  try {
    const { data, error } = await supabase.rpc("pre_chunk_dataset_smart", {
      p_dataset_id: testUuid,
      p_chunk_size: 1000
    });
    results.pre_chunk_dataset_smart = {
      data,
      error: error ? {
        message: error.message,
        code: error.code
      } : null
    };
  } catch (e) {
    results.pre_chunk_dataset_smart = {
      catch: String(e)
    };
  }
  // Test 4: rechunk_for_device (4-param)
  try {
    const { data, error } = await supabase.rpc("rechunk_for_device", {
      p_device_id: testUuid,
      p_job_id: testUuid,
      p_chunk_count: 1,
      p_strategy: 'uniform'
    });
    results.rechunk_for_device = {
      data,
      error: error ? {
        message: error.message,
        code: error.code
      } : null
    };
  } catch (e) {
    results.rechunk_for_device = {
      catch: String(e)
    };
  }
  // Test 5: Verify trigger exists
  try {
    const { data, error } = await supabase.from("information_schema").select("routine_name").eq("routine_schema", "public").eq("routine_name", "auto_plan_chunks_after_scan").limit(1);
    results.auto_plan_chunks_trigger_check = {
      data,
      error: error ? {
        message: error.message
      } : null
    };
  } catch (e) {
    results.auto_plan_chunks_trigger_check = {
      catch: String(e)
    };
  }
  // Test 6: Check report_dataset_scan table access
  try {
    const { data, error } = await supabase.from("datasets").update({
      status: 'scanned'
    }).eq("id", testUuid).select();
    results.datasets_update_test = {
      data,
      error: error ? {
        message: error.message,
        code: error.code,
        hint: error.hint
      } : null
    };
  } catch (e) {
    results.datasets_update_test = {
      catch: String(e)
    };
  }
  return new Response(JSON.stringify(results, null, 2), {
    headers: {
      "Content-Type": "application/json"
    }
  });
});
