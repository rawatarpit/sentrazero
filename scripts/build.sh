#!/usr/bin/env bash
# ============================================================
# SentraZero canonical build script
#
# Builds the REAL agent binary for all supported platforms from
# the correct entrypoint (./cmd), verifies each artifact with
# `go version -m`, and generates SHA256SUMS.
#
# Why this exists:
#   The repo has TWO main packages:
#     ./cmd/        -> the real agent (sandbox, heartbeat, realtime)  ~13-14 MB
#     ./cmd/sentra/ -> a small CLI tool for running plugins locally   ~2.5 MB
#
#   Always build from ./cmd/ and ALWAYS verify the embedded build path
#   (must be "sentra-agent/cmd", never "sentra-agent/cmd/sentra").
#
# Usage:
#   scripts/build.sh                  build all 5 platforms into dist/
#   scripts/build.sh --no-sync        build only, skip download/ sync
#   scripts/build.sh --skip-build     only regenerate SHA256SUMS (all must exist)
#
# Outputs:
#   dist/sentra-agent-<os>-<arch>[.exe]
#   dist/SHA256SUMS
#   download/ (synced copy unless --no-sync)
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT/dist"
DOWNLOAD_DIR="$ROOT/download"
ENTRYPOINT="./cmd/"          # <-- MUST be ./cmd/, not ./cmd/sentra/
LDFLAGS='-w -s'
SYNC_DOWNLOAD=1

# --- arg parsing ---
for arg in "$@"; do
  case "$arg" in
    --no-sync) SYNC_DOWNLOAD=0 ;;
    --skip-build) SKIP_BUILD=1 ;;
    --help|-h)
      sed -n '1,40p' "${BASH_SOURCE[0]}" | grep '^#' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown arg: $arg (try --help)" >&2; exit 1 ;;
  esac
done

# --- platform matrix: os/arch/extension ---
declare -a TARGETS=(
  "linux/amd64/"
  "linux/arm64/"
  "darwin/amd64/"
  "darwin/arm64/"
  "windows/amd64/.exe"
)

fail() { echo "ERROR: $*" >&2; exit 1; }

# --- verify a binary is the REAL agent, not the CLI ---
verify_agent() {
  local bin="$1"
  [[ -f "$bin" ]] || fail "missing binary: $bin"

  local path depcount
  path="$(go version -m "$bin" 2>/dev/null | awk '/^\tpath\t/ {print $2}')"
  depcount="$(go version -m "$bin" 2>/dev/null | awk '/^\tdep\t/ {print $2}' | wc -l | tr -d ' ')"

  # The real agent embeds build path "sentra-agent/cmd" (never "cmd/sentra").
  if [[ "$path" == *"cmd/sentra"* ]]; then
    fail "$(basename "$bin"): WRONG ENTRYPOINT (embedded path '$path' is the CLI, not the agent)"
  fi
  if [[ -z "$path" ]]; then
    fail "$(basename "$bin"): no embedded build path - is this a Go binary?"
  fi
  if [[ "$depcount" -lt 10 ]]; then
    fail "$(basename "$bin"): only $depcount deps - this looks like the stripped CLI, expected 30+"
  fi
  echo "  OK  $(basename "$bin"): path=$path deps=$depcount"
}

# --- build one target ---
build_one() {
  local spec="$1"
  local os="${spec%%/*}"
  local rest="${spec#*/}"
  local arch="${rest%%/*}"
  local ext="${rest#*/}"
  [[ "$ext" == "$rest" ]] && ext=""

  local out="$DIST_DIR/sentra-agent-$os-$arch$ext"
  echo ">>> building $os/$arch -> $(basename "$out")"
  mkdir -p "$DIST_DIR"
  GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 \
    go build -ldflags="$LDFLAGS" -o "$out" "$ENTRYPOINT"
  verify_agent "$out"
}

# --- main ---
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  cd "$ROOT"
  command -v go >/dev/null || fail "go not found in PATH"
  echo "Go: $(go version)"
  echo "Entrypoint: $ENTRYPOINT (must be ./cmd/)"
  echo ""

  for t in "${TARGETS[@]}"; do
    build_one "$t"
  done

  # keep the standalone agent name as well (deployment convenience)
  echo ""
  echo ">>> copying linux/amd64 -> $DIST_DIR/sentra-agent"
  cp "$DIST_DIR/sentra-agent-linux-amd64" "$DIST_DIR/sentra-agent"
fi

echo ""
echo ">>> generating SHA256SUMS"
(
  cd "$DIST_DIR"
  shasum -a 256 \
    sentra-agent-linux-amd64 \
    sentra-agent-linux-arm64 \
    sentra-agent-darwin-amd64 \
    sentra-agent-darwin-arm64 \
    sentra-agent-windows-amd64.exe \
    > SHA256SUMS
)

if [[ "$SYNC_DOWNLOAD" == "1" ]]; then
  echo ""
  echo ">>> syncing to download/"
  mkdir -p "$DOWNLOAD_DIR"
  cp "$DIST_DIR"/sentra-agent-* "$DIST_DIR"/SHA256SUMS "$DOWNLOAD_DIR/"
  diff "$DIST_DIR/SHA256SUMS" "$DOWNLOAD_DIR/SHA256SUMS" \
    && echo ">>> checksums MATCH"
fi

echo ""
echo "=== BUILD COMPLETE ==="
echo "dist/SHA256SUMS:"
cat "$DIST_DIR/SHA256SUMS"
echo ""
echo "NOTE: verify BEFORE publishing a release:"
echo "  go version -m dist/sentra-agent-linux-amd64 | grep '^	path'"
echo "  # must print: sentra-agent/cmd"
