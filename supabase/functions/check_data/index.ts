import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

serve(async (req)=>{
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL"),
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")
  );
  
  const { data, error } = await supabase
    .from("plugin_signing_keys")
    .select("*")
    .limit(5);
    
  return new Response(JSON.stringify({ data, error }), {
    headers: { "Content-Type": "application/json" }
  });
});
