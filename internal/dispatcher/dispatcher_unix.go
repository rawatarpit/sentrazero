//go:build linux || darwin
// +build linux darwin

package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"sentra-agent/internal/obs"
)

type PluginHandler func(ctx context.Context, payload json.RawMessage) error

type pluginEntry struct {
	handler PluginHandler
	timeout time.Duration
}

var pluginRegistry = make(map[string]pluginEntry)

func getAgentID() string {
	if agentID := os.Getenv("AGENT_ID"); agentID != "" {
		return agentID
	}
	hostname, _ := os.Hostname()
	return hostname
}

func init() {
	registerPlugin("builtin:scan_dataset", executeScanDataset, 10*time.Minute)
	registerPlugin("builtin:merge_dataset", executeMergeDataset, 12*time.Minute)
	registerPlugin("builtin:process", executeProcessChunk, 15*time.Minute)
	registerPlugin("builtin:ingest_dataset", executeIngestDataset, 10*time.Minute)
}

func registerPlugin(pluginID string, handler PluginHandler, timeout time.Duration) {
	pluginRegistry[pluginID] = pluginEntry{
		handler: handler,
		timeout: timeout,
	}
}

func executeJobUnix(
	parent context.Context,
	jobType string,
	payload json.RawMessage,
) error {

	job, _ := parseJobPayload(payload)

	pluginID := job.PluginID
	if pluginID == "" {
		pluginID = "builtin:" + jobType
	}

	entry, exists := pluginRegistry[pluginID]
	if !exists {
		return fmt.Errorf("no plugin registered for: %s", pluginID)
	}

	obs.Info("executing job", obs.Field{
		"job_type":        jobType,
		"plugin_id":       pluginID,
		"timeout_seconds": entry.timeout.Seconds(),
	})

	ctx, cancel := context.WithTimeout(parent, entry.timeout)
	defer cancel()

	return entry.handler(ctx, payload)
}

func parseJobPayload(payload json.RawMessage) (Job, error) {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return job, err
	}
	return job, nil
}
