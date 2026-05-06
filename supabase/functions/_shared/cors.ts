export function getCorsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const headers = {
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
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
  if (origin) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}
export function jsonResponse(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/json",
      ...headers
    },
    status
  });
}
export function optionsResponse(corsHeaders) {
  return new Response(JSON.stringify({
    ok: true
  }), {
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders
    },
    status: 200
  });
}
