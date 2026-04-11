package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type Server struct {
	port      string
	mux       *http.ServeMux
	server    *http.Server
	ready     atomic.Bool
	deviceID  string
	startTime time.Time
}

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Uptime    string `json:"uptime"`
}

type ReadyResponse struct {
	Status   string `json:"status"`
	DeviceID string `json:"device_id,omitempty"`
}

func New(port string) *Server {
	return &Server{
		port:      port,
		mux:       http.NewServeMux(),
		startTime: time.Now(),
	}
}

func (s *Server) SetDeviceID(id string) {
	s.deviceID = id
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) setupRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/live", s.handleLiveness)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Uptime:    time.Since(s.startTime).String(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	resp := ReadyResponse{
		Status:   "not_ready",
		DeviceID: "",
	}

	if s.ready.Load() && s.deviceID != "" {
		resp.Status = "ready"
		resp.DeviceID = s.deviceID
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

func (s *Server) Start(ctx context.Context) error {
	s.setupRoutes()

	addr := fmt.Sprintf(":%s", s.port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		log.Printf("🏥 Health check server starting on port %s", s.port)
		log.Printf("   • GET /health - Health check")
		log.Printf("   • GET /ready  - Readiness check")
		log.Printf("   • GET /live   - Liveness check")

		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ Health check server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		log.Println("🏥 Shutting down health check server")
		return s.server.Shutdown(ctx)
	}
	return nil
}
