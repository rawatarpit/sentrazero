-- Create function to retrieve vault secret
CREATE OR REPLACE FUNCTION "public"."get_vault_secret"("secret_name" "text") RETURNS "text"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_secret TEXT;
BEGIN
    SELECT decrypted_secret INTO v_secret
    FROM vault.decrypted_secrets
    WHERE name = secret_name;
    
    RETURN v_secret;
END;
$$;

ALTER FUNCTION "public"."get_vault_secret"("secret_name" "text") OWNER TO "postgres";
