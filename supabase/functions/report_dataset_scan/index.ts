import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { authenticateDeviceWithDetails } from "../_shared/auth.ts";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-agent-token, x-client-info, apikey, content-type"
};
function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    },
    status
  });
}
serve(async (req)=>{
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: corsHeaders
    });
  }
  try {
    const authResult = await authenticateDeviceWithDetails(req);
    if (!authResult.device) {
      return jsonResponse({
        ok: false,
        error: authResult.error
      }, 401);
    }
    const body = await req.json();
    const { dataset_id, summary, storage_type, org_id } = body;
    if (!dataset_id) {
      return jsonResponse({
        error: "Missing dataset_id"
      }, 400);
    }
    const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
    const scanMetadata = {
      scanned_at: new Date().toISOString(),
      storage_type: storage_type,
      scan_completed: true
    };
    const updateData = {
      scanned_at: new Date().toISOString(),
      status: "scanned",
      scan_completed: true
    };
    if (summary) {
      if (typeof summary.file_count === 'number' && summary.file_count > 0) {
        const fc = Math.floor(Number(summary.file_count));
        if (Number.isInteger(fc)) {
          updateData.file_count = fc;
          scanMetadata.file_count = fc;
        }
      }
      if (typeof summary.total_size_bytes === 'number' && summary.total_size_bytes > 0) {
        const gb = Number(summary.total_size_bytes) / (1024 * 1024 * 1024);
        if (Number.isFinite(gb)) {
          updateData.total_size_gb = parseFloat(gb.toFixed(6));
          scanMetadata.total_size_gb = updateData.total_size_gb;
        }
      }
      if (Array.isArray(summary.columns) && summary.columns.length > 0) {
        updateData.detected_columns = summary.columns.filter((c)=>typeof c === "string");
      }
      if (summary.file_types && typeof summary.file_types === "object") {
        const types = Object.keys(summary.file_types);
        if (types.length > 0) {
          updateData.file_type = types[0].replace(".", "");
        }
      }
      if (summary.total_size_bytes) scanMetadata.total_size_bytes = Number(summary.total_size_bytes);
      if (summary.file_types) scanMetadata.file_types = summary.file_types;
      if (summary.schema) scanMetadata.schema = summary.schema;
      if (summary.sample_row_count) scanMetadata.sample_row_count = Number(summary.sample_row_count);
      if (summary.format) scanMetadata.format = summary.format;
      if (summary.sample_file) scanMetadata.sample_file = summary.sample_file;
    }
    updateData.metadata = scanMetadata;
    console.log("[report_dataset_scan] Updating dataset:", dataset_id, "with data:", JSON.stringify(updateData));
    const { data, error: updateError } = await supabase.from("datasets").update(updateData).eq("id", dataset_id).select();
    if (updateError) {
      console.error("[report_dataset_scan] UPDATE ERROR:", updateError.message, updateError.details, updateError.hint);
      return jsonResponse({
        success: false,
        error: "Database update failed",
        details: updateError.message,
        hint: updateError.hint,
        code: updateError.code
      }, 500);
    }
    if (!data || data.length === 0) {
      return jsonResponse({
        success: false,
        error: "Dataset not found or update failed",
        dataset_id: dataset_id
      }, 404);
    }
    return jsonResponse({
      success: true,
      dataset_id,
      status: "metadata_saved"
    });
  } catch (error) {
    console.error("[report_dataset_scan] FATAL:", error);
    return jsonResponse({
      success: false,
      error: "Internal server error",
      details: String(error)
    }, 500);
  }
});
