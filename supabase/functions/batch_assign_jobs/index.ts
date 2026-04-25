import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";

const MAX_JOBS_PER_DEVICE = 10;

serve(async (req) => {
  const corsHeaders = getCorsHeaders(req);
  
  const origin = req.headers.get("origin");
  if (origin) {
    const allowedOrigins = Deno.env.get("ALLOWED_ORIGINS")?.split(",") ?? [];
    if (allowedOrigins.length > 0 && !allowedOrigins.some(o => origin.trim() === o.trim())) {
      return new Response("Forbidden", { status: 403, headers: corsHeaders });
    }
  }

  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const authResult = await authenticateDevice(req);
  if (!authResult.device) {
    return new Response(
      JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: corsHeaders }
    );
  }

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  try {
    const body = await req.json();
    const { job_types, limit = MAX_JOBS_PER_DEVICE } = body;
    const deviceId = authResult.device.id;
    const orgId = authResult.device.org_id;

    const jobTypeFilter = (job_types && job_types.length > 0)
      ? String(job_types[0])
      : null;

    const { data, error } = await supabase.rpc("batch_assign_jobs_atomic", {
      p_device_id: deviceId,
      p_org_id: orgId,
      p_limit: limit,
      p_job_type_filter: jobTypeFilter
    });

    if (error) throw error;

    const { jobs, assigned } = data ?? { jobs: [], assigned: 0 };

    return new Response(JSON.stringify({
      ok: true,
      assigned,
      jobs: jobs.map((j: any) => ({
        id: j.id,
        job_type: j.job_type,
        payload: j.payload,
        created_at: j.created_at
      }))
    }), { headers: corsHeaders });
  } catch (err) {
    console.error("[batch_assign_jobs] ❌", err.message);
    return new Response(
      JSON.stringify({ ok: false, error: err.message }),
      { headers: corsHeaders, status: 500 }
    );
  }
});
