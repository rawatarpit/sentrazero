//go:build linux || darwin
// +build linux darwin

package ffi

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>

// Reset dlerror before calling dlsym or dlopen to avoid stale messages
void clear_dlerror() { dlerror(); }

void* dlopen_wrapper(const char* path) {
    clear_dlerror();
    return dlopen(path, RTLD_LAZY);
}

void* dlsym_wrapper(void* handle, const char* symbol) {
    clear_dlerror();
    return dlsym(handle, symbol);
}

const char* dlerror_wrapper() {
    return dlerror();
}

int dlclose_wrapper(void* handle) {
    clear_dlerror();
    return dlclose(handle);
}
*/
import "C"

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"
)

// VerifyChecksum compares the local plugin binary’s SHA256 hash
// against an expected value (from the manifest or Supabase metadata).
func VerifyChecksum(path, expectedSHA string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("checksum: failed to open file %s: %w", path, err)
	}
	defer file.Close()

	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return fmt.Errorf("checksum: failed to read file %s: %w", path, err)
	}
	sum := hex.EncodeToString(h.Sum(nil))

	if sum != expectedSHA {
		return fmt.Errorf("checksum mismatch for %s:\n  expected: %s\n  actual:   %s", filepath.Base(path), expectedSHA, sum)
	}
	return nil
}

// DLOpen loads a shared library (.so/.dylib) after verifying its checksum.
func DLOpen(path string, expectedSHA string) (unsafe.Pointer, error) {
	// 🧩 1️⃣ Verify SHA256 integrity before dlopen
	if expectedSHA == "" {
		return nil, fmt.Errorf("checksum is required but missing for %s", path)
	}

	if err := VerifyChecksum(path, expectedSHA); err != nil {
		return nil, err
	}

	// 🧩 2️⃣ Proceed with dlopen
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	handle := C.dlopen_wrapper(cPath)
	if handle == nil {
		errStr := C.GoString(C.dlerror_wrapper())
		return nil, fmt.Errorf("dlopen failed for %s: %s", path, errStr)
	}
	return handle, nil
}

// DLSym retrieves a function pointer from an open shared library.
func DLSym(handle unsafe.Pointer, symbol string) (unsafe.Pointer, error) {
	cSym := C.CString(symbol)
	defer C.free(unsafe.Pointer(cSym))

	ptr := C.dlsym_wrapper(handle, cSym)
	if ptr == nil {
		errStr := C.GoString(C.dlerror_wrapper())
		return nil, fmt.Errorf("dlsym failed for %s: %s", symbol, errStr)
	}
	return ptr, nil
}

// DLClose closes a dynamically loaded shared library.
func DLClose(handle unsafe.Pointer) error {
	if handle == nil {
		return errors.New("dlclose: invalid handle (nil)")
	}
	if C.dlclose_wrapper(handle) != 0 {
		errStr := C.GoString(C.dlerror_wrapper())
		return fmt.Errorf("dlclose failed: %s", errStr)
	}
	return nil
}
