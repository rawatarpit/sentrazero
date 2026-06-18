//go:build windows

package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
)

func executeScanDataset(ctx context.Context, payload json.RawMessage) error {
	return errors.New("scan_dataset not supported on Windows")
}

func executeMergeDataset(ctx context.Context, payload json.RawMessage) error {
	return errors.New("merge_dataset not supported on Windows")
}

func executeProcessChunk(ctx context.Context, payload json.RawMessage) error {
	return errors.New("process_chunk not supported on Windows")
}

func executeIngestDataset(ctx context.Context, payload json.RawMessage) error {
	return errors.New("ingest_dataset not supported on Windows")
}

func getAgentID() string {
	return "windows-agent"
}
