//go:build cgo
// +build cgo

package system

func isCGOEnabled() bool {
	return true
}
