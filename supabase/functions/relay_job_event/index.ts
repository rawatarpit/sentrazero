import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { jsonResponse, optionsResponse } from "../_shared/cors.ts";
import { authenticateDevice } from "../_shared/auth.ts";
import { authenticateInternal } from "../_shared/security.ts";

const CHANNEL_PATTERN = /^agent-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

const SANITIZE_FIELDS = ['input_path', 'output_path', 'processed_path', 'data_location', 'chunk_url', 'mount_path', 'source_path', 'result_path', 'path', 'file', 'uri', 'url'];

function sanitizeValue(value: any): any {
  if (value === null || value === undefined) {
    return value;
  }
  
  if (typeof value === 'string') {
    let sanitized = value;
    sanitized = sanitized.replace(/\/[^\/\s]+\/[^\/\s]+/g, '[PATH]');
    sanitized = sanitized.replace(/[A-Za-z]:\\[^\\]+\\[^\\]+/g, '[PATH]');
    sanitized = sanitized.replace(/s3:\/\/[^\s]+/g, 's3://[BUCKET]');
    sanitized = sanitized.replace(/gs:\/\/[^\s]+/g, 'gs://[BUCKET]');
    sanitized = sanitized.replace(/https:\/\/[^/]+\/[^?\s]+/g, 'https://[STORAGE]/[OBJECT]');
    sanitized = sanitized.replace(/~[^\/\s]+/g, '~[USER]');
    sanitized = sanitized.replace(/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\/[^\s]+/g, '[IP]/[PATH]');
    return sanitized;
  }
  
  if (Array.isArray(value)) {
    return value.map(sanitizeValue);
  }
  
  if (typeof value === 'object') {
    const sanitized: any = {};
    for (const [key, val] of Object.entries(value)) {
      if (SANITIZE_FIELDS.includes(key.toLowerCase())) {
        sanitized[key] = '[REDACTED]';
      } else {
        sanitized[key] = sanitizeValue(val);
      }
    }
    return sanitized;
  }
  
  return value;
}

function sanitizeEventData(data: any): any {
  if (!data) return data;
  
  const sanitized: any = {};
  
  for (const [key, value] of Object.entries(data)) {
    if (key.toLowerCase() === 'type' || key.toLowerCase() === 'job_id' || key.toLowerCase() === 'chunk_id' || key.toLowerCase() === 'dataset_id') {
      sanitized[key] = value;
    } else if (key.toLowerCase() === 'error' || key.toLowerCase() === 'error_message') {
      sanitized[key] = sanitizeValue(value);
    } else if (key.toLowerCase() === 'result' && typeof value === 'object') {
      sanitized[key] = sanitizeValue(value);
    } else if (typeof value === 'object' && value !== null) {
      sanitized[key] = sanitizeEventData(value);
    } else {
      sanitized[key] = value;
    }
  }
  
  return sanitized;
}

function validateChannelFormat(channel: string): { valid: boolean; deviceId: string | null } {
  if (!channel) {
    return { valid: false, deviceId: null };
  }

  if (!CHANNEL_PATTERN.test(channel)) {
    return { valid: false, deviceId: null };
  }

  const deviceId = channel.slice(6);
  return { valid: true, deviceId };
}

const DISPATCH_TYPES = new Set(["process_dataset", "assign_job"]);

serve(async (req) => {
  const corsHeaders = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "authorization, x-agent-token, x-relay-key, x-org-id, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  };

  if (req.method === "OPTIONS") {
    return optionsResponse(corsHeaders);
  }

  try {
    const internalAuth = await authenticateInternal(req);
    let device = null;
    let deviceId: string | null = null;
    let orgId: string | null = null;

    if (!internalAuth.authorized) {
      const authResult = await authenticateDevice(req);
      if (!authResult.device) {
        return jsonResponse({ ok: false, error: authResult.error }, 401, corsHeaders);
      }
      device = authResult.device;
      deviceId = device.id;
      orgId = device.org_id;
    } else {
      orgId = internalAuth.orgId!;
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const body = await req.json();
    const { channel, data } = body ?? {};

    if (!channel || !data) {
      return jsonResponse({ ok: false, error: "missing channel or data" }, 400, corsHeaders);
    }

    const channelValidation = validateChannelFormat(channel);
    if (!channelValidation.valid) {
      return jsonResponse({ ok: false, error: "invalid channel format - must match pattern: agent-<UUID>" }, 400, corsHeaders);
    }

    const agentId = channelValidation.deviceId;
    
    if (device && agentId !== device.id) {
      return jsonResponse({ ok: false, error: "channel device_id does not match authenticated device" }, 403, corsHeaders);
    }

    if (!orgId) {
      return jsonResponse({ ok: false, error: "org_id is required" }, 400, corsHeaders);
    }

    const sanitizedData = sanitizeEventData(data);

    if (DISPATCH_TYPES.has(data.type)) {
      const { error: insertErr } = await supabase.from("agent_jobs").insert({
        agent_id: agentId,
        job_type: data.type ?? "unknown",
        payload: sanitizedData,
        org_id: orgId,
      });
      if (insertErr) throw insertErr;
      console.log(`[relay_job_event] Job queued → agent ${agentId}`);
    } else {
      await supabase.from("system_logs").insert({
        event_type: data.type,
        message: `[relay] agent=${agentId} type=${data.type}`,
        org_id: orgId,
      }).then(() => {});
      console.log(`[relay_job_event] Relayed ${data.type} → agent ${agentId}`);
    }

    return jsonResponse({ ok: true, agent_id: agentId }, 200, corsHeaders);
  } catch (err) {
    console.error("[relay_job_event]", err.message);
    return jsonResponse({ ok: false, error: err.message }, 500, corsHeaders);
  }
});
