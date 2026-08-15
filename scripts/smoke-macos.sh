#!/usr/bin/env bash
# ============================================================
# SentraZero macOS smoke test
#
# Verifies a single macOS agent environment with one command:
#   scripts/smoke-macos.sh
#
# What it does:
#   1. Static gates: `go build ./...` + `go vet ./...`
#   2. Builds the REAL agent binary  -> bin/sentra-agent-darwin
#   3. Runs the darwin sandbox harness (`go run ./cmd/sandbox-test`)
#   4. Asserts on the harness output (seatbelt / net namespaces):
#        simple-echo   -> must be OK
#        write-workdir -> must be OK
#        write-etc     -> must be denied by seatbelt (ERROR/denied)
#        net-blocked   -> must NOT be OK (network blocked when net=false)
#        net-allowed   -> must be OK (network allowed when net=true)
#
# Safe to run on a dev machine: the harness only writes to tmp/its
# workdir and *attempts* a denied /etc write (that denial is the test).
#
# Exit code: 0 = all required checks passed, 1 = any required check failed.
# ============================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN_DIR="$ROOT/bin"
AGENT_BIN="$BIN_DIR/sentra-agent-darwin"

FAILED=0
RESULTS=()

OUT="$(mktemp "${TMPDIR:-/tmp}/sentra-smoke-macos.XXXXXX")"
trap 'rm -f "$OUT"' EXIT

# --- result helpers -------------------------------------------------------
pass() { RESULTS+=("PASS|$1"); echo "  PASS  $1"; }
skip() { RESULTS+=("SKIP|$1"); echo "  SKIP  $1${2:+ - $2}"; }
fail() {
  RESULTS+=("FAIL|$1")
  echo "  FAIL  $1${2:+ - $2}"
  FAILED=1
}

# gate runs a command and records PASS/FAIL by exit code.
gate() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label" "command exited non-zero: $*"
  fi
}

# line_for prints the first harness output line for a test name.
line_for() {
  grep -F "[$1]" "$OUT" | head -n 1
}

# --- 1. static gates ------------------------------------------------------
echo "==> static gates"
gate "static gate: go build ./..." go build ./...
gate "static gate: go vet ./..."    go vet ./...

# --- 2. build agent binary ------------------------------------------------
echo "==> build agent"
mkdir -p "$BIN_DIR"
gate "build agent (bin/sentra-agent-darwin)" go build -o "$AGENT_BIN" ./cmd

# --- 3. run darwin sandbox harness ----------------------------------------
echo "==> run darwin sandbox harness (go run ./cmd/sandbox-test)"
go run ./cmd/sandbox-test >"$OUT" 2>&1
HARNESS_RC=$?
if [[ $HARNESS_RC -eq 0 ]]; then
  pass "harness run (sandbox-test)"
else
  fail "harness run (sandbox-test)" "exit code $HARNESS_RC"
fi

# --- 4. assertions on harness output --------------------------------------
echo "==> harness assertions"

line="$(line_for simple-echo)"
if [[ "$line" == *"OK"* ]]; then
  pass "simple-echo reports OK"
else
  fail "simple-echo reports OK" "got: ${line:-<no output>}"
fi

line="$(line_for write-workdir)"
if [[ "$line" == *"OK"* ]]; then
  pass "write-workdir reports OK"
else
  fail "write-workdir reports OK" "got: ${line:-<no output>}"
fi

line="$(line_for write-etc)"
if grep -qiE 'ERROR|denied|deny' <<<"$line"; then
  pass "write-etc denied by seatbelt"
else
  fail "write-etc denied by seatbelt" "got: ${line:-<no output>}"
fi

line="$(line_for net-blocked)"
if [[ "$line" == *"OK"* ]]; then
  fail "net-blocked (network blocked when net=false)" "reports OK: $line"
else
  pass "net-blocked (network blocked when net=false)"
fi

line="$(line_for net-allowed)"
if [[ "$line" == *"OK"* ]]; then
  pass "net-allowed (network allowed when net=true)"
else
  fail "net-allowed (network allowed when net=true)" "got: ${line:-<no output>}"
fi

# plugin-e2e: a REAL plugin must execute through the production path
# (plugin.Execute -> RunSandboxedPlugin -> sandbox Prepare/Execute/Destroy)
# and return its JSON output. The harness reports
# [plugin-e2e] OK: plugin ran inside sandbox, method=native_sandbox ...
# when the plugin completed inside the seatbelt sandbox with the echoed payload.
line="$(line_for plugin-e2e)"
if [[ "$line" == *"OK"* ]]; then
  pass "plugin-e2e (plugin runs inside sandbox)"
else
  fail "plugin-e2e (plugin runs inside sandbox)" "got: ${line:-<no output>}"
fi

# --- 5. summary ------------------------------------------------------------
if [[ $FAILED -eq 1 ]]; then
  echo ""
  echo "--- harness output (debug) ---"
  sed -n '1,120p' "$OUT"
fi

echo ""
echo "============================================================"
echo " macOS smoke test summary"
echo "============================================================"
printf '  %-44s %s\n' "CHECK" "RESULT"
printf '  %-44s %s\n' "-----" "------"
for r in "${RESULTS[@]}"; do
  status="${r%%|*}"
  name="${r#*|}"
  printf '  %-44s %s\n' "$name" "$status"
done
echo "============================================================"

if [[ $FAILED -eq 0 ]]; then
  echo "RESULT: ALL CHECKS PASSED"
  exit 0
else
  echo "RESULT: SOME CHECKS FAILED"
  exit 1
fi
