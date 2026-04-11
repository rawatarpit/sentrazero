//go:build windows
// +build windows

package ffi

// VerifyChecksum is a no-op on Windows (plugins not supported)
func VerifyChecksum(path string, expectedSHA string) error {
	return nil
}
