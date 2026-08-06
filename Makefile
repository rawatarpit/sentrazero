# ============================================
# SENTRA AGENT MAKEFILE
# Project: SentraZero (Sentra Agent)
#
# Canonical build entrypoint: scripts/build.sh
# Always builds from ./cmd/ (the REAL agent), NOT ./cmd/sentra/ (CLI).
# ============================================

GO_BINARY := bin/sentra-agent

# --- Default Targets ---
.PHONY: all build release verify clean info

# --- Build Everything (native platform only, fast) ---
all: build

# --- Build native Go Agent (stripped, from ./cmd/) ---
build:
	@echo "Building native Sentra Agent from ./cmd/ ..."
	@mkdir -p bin
	go build -ldflags="-w -s" -o $(GO_BINARY) ./cmd/
	@echo "Build complete: $(GO_BINARY)"
	@echo "Verifying entrypoint (must be sentra-agent/cmd, never cmd/sentra):"
	@go version -m $(GO_BINARY) | grep -E '^\s+path' || (echo "FAILED: not a Go binary" && exit 1)
	@go version -m $(GO_BINARY) | grep -E '^\s+path' | grep -q "cmd/sentra" && (echo "FAILED: wrong entrypoint (CLI, not agent)" && exit 1) || echo "OK: real agent"

# --- Build ALL 5 platforms into dist/ + download/ + SHA256SUMS (canonical) ---
release:
	@scripts/build.sh

# --- Run the Agent (native) ---
run: build
	@echo "Starting Sentra Agent..."
	./$(GO_BINARY)

# --- Verify an existing binary is the real agent ---
verify:
	@echo "Verifying $(GO_BINARY) ..."
	@go version -m $(GO_BINARY) | grep -E '^\s+path' || (echo "FAILED: not a Go binary" && exit 1)
	@go version -m $(GO_BINARY) | grep -E '^\s+path' | grep -q "cmd/sentra" && (echo "FAILED: wrong entrypoint (CLI, not agent)" && exit 1) || echo "OK: real agent"

# --- Clean Build Artifacts (keeps go module cache!) ---
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin dist download
	@echo "Clean complete."

# --- Environment info ---
info:
	@echo "Go: $$(go version)"
	@echo "OS: $$(uname -a)"
	@echo "Entrypoint: ./cmd/ (real agent) — never ./cmd/sentra/ (CLI)"
