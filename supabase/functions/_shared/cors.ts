export function getCorsHeaders(req: Request): Record<string, string> {
  const origin = req.headers.get("origin") || "";
  
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": [
      "authorization",
      "x-agent-token", 
      "x-relay-key", 
      "x-org-id", 
      "x-device-id", 
      "x-client-info", 
      "x-trace-id",
      "apikey", 
      "content-type"
    ].join(", "),
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  };

  if (origin) {
    headers["Access-Control-Allow-Origin"] = origin;
  }

  return headers;
}

export function jsonResponse(
  payload: Record<string, unknown>, 
  status = 200, 
  headers: Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...headers,
    },
    status,
  });
}

export function optionsResponse(corsHeaders: Record<string, string>): Response {
  return new Response(JSON.stringify({ ok: true }), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders,
    },
    status: 200,
  });
}