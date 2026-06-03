import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
serve(async (req)=>{
  const supabase = createClient(Deno.env.get("SUPABASE_URL"), Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));
  try {
    const body = await req.json();
    const { action, table, data: insertData, select, filters } = body;
    if (action === "insert") {
      const { data, error } = await supabase.from(table).insert(insertData).select();
      return new Response(JSON.stringify({
        data,
        error
      }), {
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    if (action === "select") {
      let q = supabase.from(table).select(select || "*");
      if (filters) {
        for (const [k, v] of Object.entries(filters)){
          q = q.eq(k, v);
        }
      }
      const { data, error } = await q;
      return new Response(JSON.stringify({
        data,
        error
      }), {
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    if (action === "rpc") {
      const { fn, args } = body;
      const { data, error } = await supabase.rpc(fn, args || {});
      return new Response(JSON.stringify({
        data,
        error
      }), {
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    return new Response(JSON.stringify({
      error: "Unknown action"
    }), {
      headers: {
        "Content-Type": "application/json"
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({
      error: err.message
    }), {
      headers: {
        "Content-Type": "application/json"
      }
    });
  }
});
