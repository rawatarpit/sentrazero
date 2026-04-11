//go:build (linux || darwin) && cgo
// +build linux darwin
// +build cgo

package dispatcher

/*
#include <stdlib.h>
#include <signal.h>

typedef char* (*plugin_run_fn)(char*);
typedef void  (*plugin_free_fn)(char*);

static inline char* call_plugin_run(void* f, char* input) {
	return ((plugin_run_fn)f)(input);
}

static inline void call_plugin_free(void* f, char* ptr) {
	((plugin_free_fn)f)(ptr);
}

static inline int call_plugin_kill(void* handle) {
    if (handle == NULL) return -1;
    // Note: This is a placeholder. In practice, we'd need the PID
    // to send SIGKILL to the plugin process.
    return 0;
}
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"unsafe"

	"sentra-agent/internal/ffi"
	"sentra-agent/internal/obs"
)

// ---------------------------------------------------------------------
// Plugin loading
// ---------------------------------------------------------------------

func openAndGetSymbols(
	pluginPath string,
	expectedSHA string,
) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, error) {

	handle, err := ffi.DLOpen(pluginPath, expectedSHA)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dlopen failed: %w", err)
	}

	runSym, err := ffi.DLSym(handle, "plugin_run")
	if err != nil {
		_ = ffi.DLClose(handle)
		return nil, nil, nil, fmt.Errorf("missing plugin_run: %w", err)
	}

	freeSym, err := ffi.DLSym(handle, "plugin_free")
	if err != nil {
		_ = ffi.DLClose(handle)
		return nil, nil, nil, fmt.Errorf("missing plugin_free: %w", err)
	}

	return handle, runSym, freeSym, nil
}

// ---------------------------------------------------------------------
// Plugin invocation with timeout
// ---------------------------------------------------------------------

func callPlugin(
	ctx context.Context,
	runSym unsafe.Pointer,
	freeSym unsafe.Pointer,
	input string,
) (output string, err error) {

	type result struct {
		output string
		err    error
	}

	resultCh := make(chan result, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				resultCh <- result{err: fmt.Errorf("plugin execution panicked: %v", r)}
			}
		}()

		cInput := C.CString(input)
		defer C.free(unsafe.Pointer(cInput))

		outPtr := C.call_plugin_run(runSym, cInput)
		if outPtr == nil {
			resultCh <- result{err: errors.New("plugin returned NULL output (contract violation)")}
			return
		}

		output := C.GoString(outPtr)
		C.call_plugin_free(freeSym, outPtr)

		resultCh <- result{output: output}
	}()

	select {
	case <-ctx.Done():
		obs.Warn("plugin execution timed out", obs.Field{
			"error": ctx.Err().Error(),
		})
		return "", fmt.Errorf("plugin execution timed out: %w", ctx.Err())
	case res := <-resultCh:
		return res.output, res.err
	}
}

// ---------------------------------------------------------------------
// Plugin cleanup
// ---------------------------------------------------------------------

func closePluginHandle(handle unsafe.Pointer) {
	if err := ffi.DLClose(handle); err != nil {
		log.Printf("[dispatcher] ⚠️ dlclose failed: %v", err)
	}
}
