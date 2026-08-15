package dataset

import (
	"path/filepath"
	"testing"
)

func TestGetOutputPath_WithDatasetSlug(t *testing.T) {
	path := getOutputPath(nil, "uuid-1234", "my-dataset")
	expected := "my-dataset_merged.csv"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetOutputPath_WithoutDatasetSlug(t *testing.T) {
	path := getOutputPath(nil, "uuid-1234", "")
	expected := "uuid-1234_merged.csv"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetOutputPath_WithDeviceOutputFile(t *testing.T) {
	devOut := &DeviceOutput{
		MountPath: "/mnt/data",
		File:      "explicit.csv",
	}
	path := getOutputPath(devOut, "uuid-1234", "my-dataset")
	expected := filepath.Join("/mnt/data", "explicit.csv")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetOutputPath_WithDeviceOutputMountPath(t *testing.T) {
	devOut := &DeviceOutput{
		MountPath: "/mnt/data",
	}
	path := getOutputPath(devOut, "uuid-1234", "my-dataset")
	expected := filepath.Join("/mnt/data", "my-dataset_merged.csv")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetOutputPath_WithDeviceOutputMountPathNoSlug(t *testing.T) {
	devOut := &DeviceOutput{
		MountPath: "/mnt/data",
	}
	path := getOutputPath(devOut, "uuid-1234", "")
	expected := filepath.Join("/mnt/data", "uuid-1234_merged.csv")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}
