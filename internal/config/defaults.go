package config

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	supabaseTempDir = "supabase/.temp"
	projectRefFile  = "project-ref"
)

func ReadProjectRef() string {
	ref := os.Getenv("SUPABASE_PROJECT_REF")
	if ref != "" {
		return ref
	}

	candidates := []string{
		filepath.Join(supabaseTempDir, projectRefFile),
		filepath.Join(".", supabaseTempDir, projectRefFile),
		os.Getenv("SUPABASE_PROJECT_REF_PATH"),
	}

	for _, path := range candidates {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil {
			ref = strings.TrimSpace(string(data))
			if ref != "" {
				return ref
			}
		}
	}

	return ""
}

func BuildBackendURL(projectRef string) string {
	if projectRef == "" {
		return ""
	}
	return "https://" + projectRef + ".supabase.co"
}

func BuildAnonKey(projectRef string) string {
	return os.Getenv("SUPABASE_ANON_KEY")
}
