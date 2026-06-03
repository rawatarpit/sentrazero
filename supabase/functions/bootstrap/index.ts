// supabase/functions/bootstrap/index.ts
import { serve } from "https://deno.land/std@0.192.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { validateOrigin } from "../_shared/security.ts";
import { getCorsHeaders } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 10;
async function checkRateLimit(supabase, clientKey) {
  const now = Date.now();
  const { count, error: countErr } = await supabase.from("bootstrap_rate_limits").select("*", {
    count: "exact",
    head: true
  }).eq("client_key", clientKey).gte("created_at", new Date(now - RATE_LIMIT_WINDOW_MS).toISOString());
  if (countErr) {
    console.error("[bootstrap] Rate limit check error:", countErr);
    return true;
  }
  if (count !== null && count >= RATE_LIMIT_MAX_REQUESTS) {
    return false;
  }
  const { error: insertErr } = await supabase.from("bootstrap_rate_limits").insert({
    client_key: clientKey
  });
  if (insertErr) {
    console.error("[bootstrap] Rate limit insert error:", insertErr);
  }
  return true;
}
serve(async (req)=>{
  const corsHeaders = getCorsHeaders(req);
  const origin = req.headers.get("origin");
  if (origin && !validateOrigin(req)) {
    return new Response("Forbidden", {
      status: 403,
      headers: corsHeaders
    });
  }
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: corsHeaders
    });
  }
  if (req.method !== "GET") {
    return new Response(JSON.stringify({
      error: "Method not allowed"
    }), {
      status: 405,
      headers: corsHeaders
    });
  }
  // Authenticate via device token
  const authResult = await authenticateDevice(req);
  if (authResult.error || !authResult.device) {
    return new Response(JSON.stringify({
      error: "Unauthorized"
    }), {
      status: 401,
      headers: corsHeaders
    });
  }
  const clientIP = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || req.headers.get("x-real-ip") || req.headers.get("cf-connecting-ip") || "unknown";
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  if (!await checkRateLimit(supabase, clientIP)) {
    return new Response(JSON.stringify({
      error: "rate limit exceeded"
    }), {
      status: 429,
      headers: corsHeaders
    });
  }
  return new Response(JSON.stringify({
    backend_url: Deno.env.get("SUPABASE_URL"),
    anon_key: Deno.env.get("SUPABASE_ANON_KEY"),
    environment: "prod",
    redis_url: Deno.env.get("Redis_url"),
    redis_token: Deno.env.get("Redis_token")
  }), {
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json",
      "Cache-Control": "private, max-age=60, must-revalidate"
    },
    status: 200
  });
});
