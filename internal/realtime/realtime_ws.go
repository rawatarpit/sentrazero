package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/obs"
)

type WSPayload struct {
	Ref     string `json:"ref"`
	Topic   string `json:"topic"`
	Event   string `json:"event"`
	Payload any    `json:"payload"`
}

type PostgresChangePayload struct {
	Commit    string `json:"commit"`
	Insert    any    `json:"insert"`
	Update    any    `json:"update"`
	Delete    any    `json:"delete"`
	OldRecord any    `json:"old_record"`
}

type PostgresChangeRecord struct {
	Type   string         `json:"type"`
	Table  string         `json:"table"`
	Schema string         `json:"schema"`
	New    map[string]any `json:"new"`
	Old    map[string]any `json:"old"`
}

func RunRealtimeWS(ctx context.Context, device auth.Device, cfg *config.Config) {
	backoff := 2 * time.Second
	const maxBackoff = 60 * time.Second

	log.Printf("[ws] starting WebSocket realtime client for device %s", device.ID)

	for {
		select {
		case <-ctx.Done():
			log.Println("[ws] shutting down")
			return
		default:
		}

		err := runWSConnection(ctx, device, cfg)
		if err != nil {
			log.Printf("[ws] connection error: %v", err)
		}

		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		reconnectTime := backoff + jitter
		log.Printf("[ws] reconnecting in %s", reconnectTime)

		select {
		case <-ctx.Done():
			log.Println("[ws] shutting down")
			return
		case <-time.After(reconnectTime):
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func runWSConnection(ctx context.Context, device auth.Device, cfg *config.Config) error {
	url := strings.Replace(cfg.BackendURL, "https://", "wss://", 1)
	url = fmt.Sprintf("%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0", url, cfg.BackendAnonKey)

	header := http.Header{}
	header.Set("Authorization", "Bearer "+cfg.BackendAnonKey)

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(url, header)
	if err != nil {
		return fmt.Errorf("WebSocket dial failed: %w", err)
	}
	defer conn.Close()

	log.Println("[ws] connected")

	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}()

	joinRef := fmt.Sprintf("join_%d", time.Now().UnixNano())
	channelRef := fmt.Sprintf("chan_%d", time.Now().UnixNano())

	phxJoin := WSPayload{
		Ref:   joinRef,
		Topic: "phoenix",
		Event: "phx_join",
		Payload: map[string]any{
			"message": map[string]any{},
		},
	}
	if err := conn.WriteJSON(phxJoin); err != nil {
		return fmt.Errorf("phx_join failed: %w", err)
	}

	topic := fmt.Sprintf("realtime:public:agent_jobs:agent_id=eq.%s", device.ID)
	channelJoin := WSPayload{
		Ref:   channelRef,
		Topic: topic,
		Event: "phx_join",
		Payload: map[string]any{
			"message":    map[string]any{},
			"csrf_token": "",
		},
	}
	if err := conn.WriteJSON(channelJoin); err != nil {
		return fmt.Errorf("channel join failed: %w", err)
	}

	log.Printf("[ws] subscribed to %s", topic)

	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				heartbeatRef := fmt.Sprintf("hb_%d", time.Now().UnixNano())
				hb := WSPayload{
					Ref:     heartbeatRef,
					Topic:   "phoenix",
					Event:   "heartbeat",
					Payload: map[string]any{},
				}
				if err := conn.WriteJSON(hb); err != nil {
					log.Printf("[ws] heartbeat failed: %v", err)
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				return fmt.Errorf("read error: %w", err)
			}
			return err
		}

		var msg WSPayload
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		if msg.Event == "phx_error" || msg.Event == "phx_close" {
			return fmt.Errorf("channel error: %s", msg.Event)
		}

		if msg.Event == "postgres_changes" {
			handlePostgresChanges(ctx, msg.Payload, device)
		}
	}
}

func handlePostgresChanges(ctx context.Context, payload any, device auth.Device) {
	payloadMap, ok := payload.(map[string]any)
	if !ok {
		return
	}

	data, ok := payloadMap["data"].(map[string]any)
	if !ok {
		return
	}

	newRecord, ok := data["new"].(map[string]any)
	if !ok {
		return
	}

	agentID, ok := newRecord["agent_id"].(string)
	if !ok || agentID != device.ID {
		return
	}

	status, _ := newRecord["status"].(string)
	if status != "assigned" && status != "running" {
		return
	}

	jobID, _ := newRecord["id"].(string)
	if jobID == "" {
		return
	}

	if pollingClient != nil {
		if _, exists := pollingClient.sentJobs.Load(jobID); exists {
			return
		}
		pollingClient.sentJobs.Store(jobID, time.Now())
	}

	jobType, _ := newRecord["job_type"].(string)
	payloadJSON, _ := json.Marshal(newRecord["payload"])

	log.Printf("[ws] new job received: %s (type: %s)", jobID, jobType)

	obs.Info(
		"job received via websocket",
		obs.Field{
			"job_id":   jobID,
			"job_type": jobType,
			"source":   "websocket",
		},
	)

	traceID := obs.NewTraceID()

	var executionStepID string
	if len(payloadJSON) > 0 {
		var payload struct {
			ExecutionStepID string `json:"execution_step_id"`
		}
		if err := json.Unmarshal(payloadJSON, &payload); err == nil {
			executionStepID = payload.ExecutionStepID
		}
	}

	if err := dispatcher.SubmitJobWithMeta(
		jobType,
		payloadJSON,
		jobID,
		"",
		traceID,
		executionStepID,
	); err != nil {
		log.Printf("[ws] dispatch failed for job %s: %v", jobID, err)
	}
}
