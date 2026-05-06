-- Update existing plugin_signing_keys records with vault_secret_name
-- This migration fixes records where vault_secret_name wasn't saved properly

UPDATE plugin_signing_keys 
SET vault_secret_name = 'org_' || org_id::TEXT || '_ed25519_priv_' || FLOOR(EXTRACT(EPOCH FROM created_at))::TEXT
WHERE vault_secret_name IS NULL;
