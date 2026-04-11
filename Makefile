# ============================================
# SENTRA AGENT MAKEFILE
# Author: Arpit Singh Rawat
# Project: Sentra Agent / Kickin Compute System
# ============================================

GO_BINARY := bin/sentra-agent

# --- Default Targets ---
.PHONY: all build run clean verify info

# --- Build Everything ---
all: build

# --- Build Go Agent ---
build:
	@echo "🧠 Building Go Sentra Agent..."
	@mkdir -p bin
	go clean -cache -modcache -testcache
	go build -o $(GO_BINARY) ./cmd/main.go
	@echo "✅ Build complete: $(GO_BINARY)"

# --- Run the Agent ---
run: build
	@echo "🚀 Starting Sentra Agent..."
	./$(GO_BINARY)

# --- Verify Binary ---
verify:
	@echo "🔍 Verifying binary linkage..."
	@ls -lh $(GO_BINARY)
	@otool -L $(GO_BINARY) 2>/dev/null || ldd $(GO_BINARY) || true

# --- Clean Build Artifacts ---
clean:
	@echo "🧹 Cleaning build artifacts..."
	go clean -cache -modcache -testcache
	rm -rf bin
	@echo "✅ Clean complete."

# --- Environment info ---
info:
	@echo "🧩 Environment info"
	@echo "Go:    $$(go version)"
	@echo "OS:    $$(uname -a)"

# ---------------------------------------------------------
# 🧩 FUTURE PLUGIN COMPILATION (COMMENTED FOR NOW)
# ---------------------------------------------------------
# RUST_ROOT := ./rust_core
# PLUGIN_DIR := /opt/sentra/plugins
#
# build-rust:
# 	@echo "🔧 Building native Rust plugins..."
# 	cargo build --release --manifest-path=$(RUST_ROOT)/Cargo.toml
#
# install-plugins:
# 	@echo "📦 Installing locally compiled plugins..."
# 	sudo mkdir -p $(PLUGIN_DIR)
# 	sudo cp $(RUST_ROOT)/plugin_*/*.so $(PLUGIN_DIR)/ || true
# 	@echo "✅ Plugins installed to $(PLUGIN_DIR)"
#
# test-ffi:
# 	@echo "🧪 Testing Go↔Rust FFI bridge..."
# 	go run tests/ffi_check.go
# 	@echo "✅ FFI bridge validated."
