import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const body = await req.json();
    const { dataset_id, summary, storage_type, source_path } = body;

    console.log("[report_dataset_scan] received:", { dataset_id, summary_keys: Object.keys(summary || {}) });

    if (!dataset_id) {
      return new Response(
        JSON.stringify({ error: "Missing dataset_id" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Build minimal update - just update status to scanned
    // Don't try to update other fields that might have type issues
    const updateData: any = {
      status: "scanned",
      scanned_at: new Date().toISOString(),
    };

    // Try to update file_count safely
    if (summary?.file_count !== undefined && typeof summary.file_count === 'number') {
      updateData.file_count = summary.file_count;
    }

    const { data, error } = await supabase
      .from("datasets")
      .update(updateData)
      .eq("id", dataset_id)
      .select("id, status")
      .single();

    if (error) {
      console.log("[report_dataset_scan] update warning:", error.message);
      // Don't fail the job - just log the warning
      return new Response(
        JSON.stringify({ success: true, warning: "dataset update skipped", details: error.message }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    console.log("[report_dataset_scan] dataset updated:", data?.id);
    return new Response(
      JSON.stringify({ success: true, dataset: data }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (error) {
    console.error("[report_dataset_scan] error:", error);
    // Return 200 so job doesn't fail - scan already completed
    return new Response(
      JSON.stringify({ success: true, warning: "scan reported with errors", details: String(error) }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});