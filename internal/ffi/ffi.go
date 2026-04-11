package ffi

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>

typedef char* (*plugin_run_t)(const char*);
typedef void (*plugin_free_t)(char*);

static inline char* callPluginRun(plugin_run_t f, const char* arg) {
    return (*f)(arg);
}
static inline void callPluginFree(plugin_free_t f, char* ptr) {
    (*f)(ptr);
}
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"sentra-agent/internal/plugin"
)

// RunStage securely loads and executes a plugin using its manifest checksum.
func RunStage(pluginName, input string) (string, error) {
	pluginDir := os.Getenv("SENTRA_PLUGIN_DIR")
	if pluginDir == "" {
		pluginDir = filepath.Join(os.Getenv("HOME"), ".sentra", "plugins")
	}

	libPath := filepath.Join(pluginDir, pluginName+".so")
	manifestPath := filepath.Join(pluginDir, pluginName+".json")

	var manifest plugin.Manifest
	if data, err := os.ReadFile(manifestPath); err == nil {
		_ = json.Unmarshal(data, &manifest)
	} else {
		log.Printf("[ffi] ⚠️ Manifest not found for %s — skipping checksum validation", pluginName)
	}

	start := time.Now()
	handle, err := DLOpen(libPath, manifest.Checksum)
	if err != nil {
		return "", fmt.Errorf("failed to open plugin %s: %v", pluginName, err)
	}
	defer func() {
		if cerr := DLClose(handle); cerr != nil {
			log.Printf("[ffi] ⚠️ failed to dlclose %s: %v", libPath, cerr)
		}
	}()

	runSym, err := DLSym(handle, "plugin_run")
	if err != nil {
		return "", fmt.Errorf("missing plugin_run: %v", err)
	}
	freeSym, err := DLSym(handle, "plugin_free")
	if err != nil {
		return "", fmt.Errorf("missing plugin_free: %v", err)
	}

	runFunc := *(*func(*C.char) *C.char)(unsafe.Pointer(&runSym))
	freeFunc := *(*func(*C.char))(unsafe.Pointer(&freeSym))

	cInput := C.CString(input)
	defer C.free(unsafe.Pointer(cInput))

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ffi] ⚠️ plugin %s panicked: %v", pluginName, r)
		}
	}()

	outPtr := runFunc(cInput)
	if outPtr == nil {
		return "", errors.New("plugin_run returned NULL")
	}

	outGo := C.GoString(outPtr)
	freeFunc(outPtr)

	log.Printf("[ffi] ✅ %s executed successfully (%.2fs)", pluginName, time.Since(start).Seconds())
	return outGo, nil
}
