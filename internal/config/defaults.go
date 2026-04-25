package config

var (
	// Default values for enterprise deployment
	// These are pre-configured by Sentra for the client's Supabase instance
	// Can be overridden via environment variables if needed
	DefaultBackendURL = "https://pqcwgkqrblugplpcaxcy.supabase.co"
	DefaultAnonKey   = "REDACTED_ANON_KEY"
)
