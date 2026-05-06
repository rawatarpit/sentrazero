-- Add vault_secret_name column to plugin_signing_keys table
ALTER TABLE "public"."plugin_signing_keys"
ADD COLUMN IF NOT EXISTS "vault_secret_name" "text";

COMMENT ON COLUMN "public"."plugin_signing_keys"."vault_secret_name" IS 'Supabase Vault secret name for the Ed25519 private key (PKCS8 format, base64)';

-- Create function to store plugin signing key in Vault
CREATE OR REPLACE FUNCTION "public"."store_plugin_signing_key_to_vault"("p_org_id" "uuid", "p_private_key_b64" "text", "p_secret_name" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_secret_name TEXT;
BEGIN
    v_secret_name := COALESCE(
        p_secret_name,
        'org_' || p_org_id::TEXT || '_ed25519_priv_' || FLOOR(EXTRACT(EPOCH FROM NOW()))
    );

    PERFORM vault.create_secret(
        p_private_key_b64,
        v_secret_name,
        'Ed25519 private key for org ' || p_org_id::TEXT
    );

    RETURN jsonb_build_object('secret_name', v_secret_name);
END;
$$;

ALTER FUNCTION "public"."store_plugin_signing_key_to_vault"("p_org_id" "uuid", "p_private_key_b64" "text", "p_secret_name" "text") OWNER TO "postgres";

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
