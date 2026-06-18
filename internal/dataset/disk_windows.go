//go:build windows

package dataset

import "errors"

func getAvailableDiskSpace(path string) (int64, error) {
	return 0, errors.New("disk space check not supported on Windows")
}
