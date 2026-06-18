package storage

import (
	"testing"
)

func TestGetRemotePathWithSlug_Merged(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "my-dataset", 0, "merged")
	expected := "my-dataset.csv"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePathWithSlug_MergedNoSlug(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "", 0, "merged")
	expected := "uuid-1234.csv"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePathWithSlug_Chunk(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "my-dataset", 3, "chunk")
	expected := "my-dataset/chunks/chunk_3.bin"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePathWithSlug_Result(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "my-dataset", 1, "result")
	expected := "my-dataset/chunks/chunk_1.out"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePathWithSlug_UnknownType(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "my-dataset", 0, "unknown")
	expected := ""
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePath_Merged(t *testing.T) {
	path := GetRemotePath("uuid-1234", 0, "merged")
	expected := "uuid-1234.csv"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGetRemotePathWithoutSlug(t *testing.T) {
	path := GetRemotePathWithSlug("uuid-1234", "", 2, "chunk")
	expected := "uuid-1234/chunks/chunk_2.bin"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}
