package plugin

import (
	"context"
	"fmt"
	"log"
)

// InitAndWait = FULL BLOCKING PLUGIN INITIALIZATION.
// Workers MUST NOT start before this completes.
func InitAndWait(ctx context.Context) error {
	log.Println("🧩 Initializing plugin subsystem (blocking)...")

	MustInitKeyCache()

	verified, failed, err := VerifyAllCachedPlugins(ctx)
	if err != nil {
		return err
	}

	log.Printf("🔒 Plugin verification — OK=%d | FAILED=%d", verified, failed)

	if failed > 0 {
		return fmt.Errorf("plugin verification failed: %d plugins invalid", failed)
	}

	log.Println("✅ Plugin subsystem READY.")
	return nil
}

// Init = asynchronous version (non-blocking).
// Use ONLY if caller explicitly allows degraded startup.
func Init(ctx context.Context) {
	log.Println("🧩 Initializing plugin subsystem (async)...")

	MustInitKeyCache()

	verified, failed, err := VerifyAllCachedPlugins(ctx)
	if err != nil {
		log.Printf("⚠️ Plugin verification error: %v", err)
		return
	}

	log.Printf("🔒 Plugin verification — OK=%d | FAILED=%d", verified, failed)

	if failed > 0 {
		log.Printf("⚠️ Plugin subsystem started in DEGRADED state (%d failed)", failed)
		return
	}

	log.Println("✅ Plugin subsystem initialized successfully.")
}
