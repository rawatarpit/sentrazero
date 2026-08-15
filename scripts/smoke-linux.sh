#!/usr/bin/env bash
# ============================================================
# SentraZero Linux smoke test
#
# Verifies a single Linux agent environment with one command:
#   scripts/smoke-linux.sh
#
# What it does:
#   1. Static gates: `go build ./...` + `go vet ./...`
#   2. Builds:
#        bin/sentra-agent-linux-amd64  (from ./cmd - the REAL agent)
#        bin/sandbox-test              (from ./cmd/sandbox-test)
#        bin/seccomp-probe             (from ./cmd/seccomp-probe, linux-only)
#   3. Runs the Linux sandbox harness and asserts:
#        simple-echo      -> OK
#        write-workdir    -> OK
#        cpu-time-limit   -> ERROR/killed (CPU cap enforced)
#        net-blocked      -> NOT OK (network blocked when net=false)
#        net-allowed      -> OK
#   4. Seccomp end-to-end with the REAL agent binary:
#        agent --seccomp-exec /bin/echo hello          -> exit 0, prints hello
#        agent --seccomp-exec /bin/sh -c 'echo hi'     -> exit 0 (the -c path)
#        agent --seccomp-exec bin/seccomp-probe        -> SIGSYS (exit 159)
#             on x86_64 only; on aarch64 seccomp is not applied -> exit 0
#        SANDBOX_SECCOMP_PROFILE=off agent --seccomp-exec probe -> exit 0
#        agent --no-new-privs-exec sh -c 'grep NoNewPrivs ...'  -> prints 1
#   5. Root-only (optional) cgroup check: runs a CPU-burner through the
#      sandboxer with SANDBOX_DEFAULT_CPU_PERCENT=50 and verifies
#      cpu.max ("50000 100000") was applied (skipped when not root or
#      when the cgroup v2 cpu controller is not available).
#
# Exit code: 0 = all required checks passed, 1 = any required check failed.
# ============================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ERROR: scripts/smoke-linux.sh must run on Linux (got $(uname -s))" >&2
  exit 2
fi

BIN_DIR="${SMOKE_BIN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/sentrazero-smoke.XXXXXX")}"
AGENT_BIN="$BIN_DIR/sentra-agent-linux-amd64"
HARNESS_BIN="$BIN_DIR/sandbox-test"
PROBE_BIN="$BIN_DIR/seccomp-probe"
ARCH="$(uname -m)"

FAILED=0
RESULTS=()

OUT="$(mktemp "${TMPDIR:-/tmp}/sentra-smoke-linux.XXXXXX")"
# $OUT and $OUT.<suffix> (used for per-test captures) are all removed on exit.
# $BIN_DIR is a throwaway root-owned temp dir (world-traversable path) so the
# sandbox's CLONE_NEWUSER re-exec (which re-runs os.Executable()) can exec the
# binaries: inside the user namespace the container-root can only traverse
# paths whose owners are mapped into the namespace. A repo-local bin/ under
# /home/runner/work/... is owned by an unmapped uid and exec fails with
# EACCES. When SMOKE_BIN_DIR is set the caller owns cleanup.
trap 'rm -f "$OUT" "$OUT".*; if [[ -z "${SMOKE_BIN_DIR:-}" && -n "${BIN_DIR:-}" ]]; then rm -rf "$BIN_DIR"; fi' EXIT

# --- result helpers -------------------------------------------------------
pass() { RESULTS+=("PASS|$1"); echo "  PASS  $1"; }
skip() { RESULTS+=("SKIP|$1"); echo "  SKIP  $1${2:+ - $2}"; }
fail() {
  RESULTS+=("FAIL|$1")
  echo "  FAIL  $1${2:+ - $2}"
  FAILED=1
}

gate() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    pass "$label"
  else
    fail "$label" "command exited non-zero: $*"
  fi
}

line_for() {
  grep -F "[$1]" "$OUT" | head -n 1
}

# --- 1. static gates ------------------------------------------------------
echo "==> static gates"
gate "static gate: go build ./..." go build ./...
gate "static gate: go vet ./..."    go vet ./...

# --- 2. build binaries ----------------------------------------------------
echo "==> build binaries"
mkdir -p "$BIN_DIR"
gate "build agent (bin/sentra-agent-linux-amd64)" go build -o "$AGENT_BIN" ./cmd
gate "build harness (bin/sandbox-test)"           go build -o "$HARNESS_BIN" ./cmd/sandbox-test
gate "build probe (bin/seccomp-probe)"            go build -o "$PROBE_BIN" ./cmd/seccomp-probe

# --- 3. run Linux sandbox harness -----------------------------------------
echo "==> run Linux sandbox harness"
"$HARNESS_BIN" >"$OUT" 2>&1
HARNESS_RC=$?
if [[ $HARNESS_RC -eq 0 ]]; then
  pass "harness run (sandbox-test)"
else
  fail "harness run (sandbox-test)" "exit code $HARNESS_RC"
fi

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

# The Linux harness reports [cpu-time-limit] OK: busy loop killed as
# expected: <err> when the CPU cap worked, and
# [cpu-time-limit] ERROR: busy loop was not killed by the CPU limit when
# it did not. PASS = the loop was killed (cap enforced).
line="$(line_for cpu-time-limit)"
if [[ "$line" == *"killed"* && "$line" != *"not killed"* ]]; then
  pass "cpu-time-limit shows ERROR/killed (loop killed)"
elif [[ "$line" == *"ERROR"* || "$line" == *"not killed"* ]]; then
  fail "cpu-time-limit shows ERROR/killed (loop killed)" "CPU limit not enforced: $line"
else
  fail "cpu-time-limit shows ERROR/killed (loop killed)" "got: ${line:-<no output>}"
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

# --- 4. seccomp end-to-end with the REAL agent binary ----------------------
echo "==> seccomp end-to-end (real agent binary)"

# 4a. --seccomp-exec /bin/echo hello  -> exit 0 and output "hello"
"$AGENT_BIN" --seccomp-exec /bin/echo hello >"$OUT.a" 2>&1
rc=$?
out="$(<"$OUT.a")"
if [[ $rc -eq 0 && "$out" == *"hello"* ]]; then
  pass "seccomp-exec /bin/echo hello (exit 0 + hello)"
else
  fail "seccomp-exec /bin/echo hello (exit 0 + hello)" "rc=$rc out=${out:-<empty>}"
fi

# 4b. --seccomp-exec /bin/sh -c 'echo hi' -> exit 0 (the -c arg path regression)
"$AGENT_BIN" --seccomp-exec /bin/sh -c 'echo hi' >"$OUT.b" 2>&1
rc=$?
if [[ $rc -eq 0 ]]; then
  pass "seccomp-exec /bin/sh -c 'echo hi' (exit 0)"
else
  fail "seccomp-exec /bin/sh -c 'echo hi' (exit 0)" "rc=$rc"
fi

# 4c. --seccomp-exec bin/seccomp-probe -> SIGSYS on x86_64 (exit 159);
#     on aarch64 seccomp is NOT applied (GOARCH guard) -> probe exits 0
"$AGENT_BIN" --seccomp-exec "$PROBE_BIN" >"$OUT.c" 2>&1
rc=$?
if [[ "$ARCH" == "x86_64" ]]; then
  if [[ $rc -eq 159 ]]; then
    pass "seccomp-exec probe SIGSYS-killed (exit 159)"
  else
    fail "seccomp-exec probe SIGSYS-killed (exit 159)" "expected 159, got $rc"
  fi
else
  if [[ $rc -eq 0 ]]; then
    pass "seccomp not applied on $ARCH (probe exits 0)"
  else
    fail "seccomp not applied on $ARCH (probe exits 0)" "expected 0, got $rc"
  fi
fi

# 4d. SANDBOX_SECCOMP_PROFILE=off --seccomp-exec probe -> exit 0 (escape hatch)
SANDBOX_SECCOMP_PROFILE=off "$AGENT_BIN" --seccomp-exec "$PROBE_BIN" >"$OUT.d" 2>&1
rc=$?
if [[ $rc -eq 0 ]]; then
  pass "SANDBOX_SECCOMP_PROFILE=off escape hatch (probe exits 0)"
else
  fail "SANDBOX_SECCOMP_PROFILE=off escape hatch (probe exits 0)" "rc=$rc"
fi

# 4e. NO_NEW_PRIVS: --no-new-privs-exec sh -c 'grep NoNewPrivs /proc/self/status'
"$AGENT_BIN" --no-new-privs-exec /bin/sh -c 'grep NoNewPrivs /proc/self/status' >"$OUT.e" 2>&1
rc=$?
out="$(<"$OUT.e")"
if [[ $rc -eq 0 && "$out" =~ NoNewPrivs:[[:space:]]*1 ]]; then
  pass "no-new-privs-exec sets NoNewPrivs=1"
else
  fail "no-new-privs-exec sets NoNewPrivs=1" "rc=$rc out=${out:-<empty>}"
fi

# --- 5. root-only (optional) cgroup cpu.max test ---------------------------
echo "==> cgroup cpu.max test (root-only, optional)"
if [[ $EUID -ne 0 ]]; then
  skip "cgroup cpu.max (50%)" "requires root"
else
  CG_BASE="${SANDBOX_CGROUPS_PATH:-/sys/fs/cgroup}"
  if [[ ! -f "$CG_BASE/cgroup.controllers" ]]; then
    skip "cgroup cpu.max (50%)" "no cgroup v2 at $CG_BASE - manual step: SANDBOX_DEFAULT_CPU_PERCENT=50 $HARNESS_BIN then verify <cg>/job-*/cpu.max == '50000 100000'"
  elif [[ ! -w "$CG_BASE" ]]; then
    skip "cgroup cpu.max (50%)" "cgroup path not writable: $CG_BASE - manual step: SANDBOX_DEFAULT_CPU_PERCENT=50 $HARNESS_BIN then verify cpu.max"
  elif [[ ! -f "$CG_BASE/cgroup.subtree_control" ]] || ! grep -Eq '(^| )cpu( |$)' "$CG_BASE/cgroup.subtree_control"; then
    skip "cgroup cpu.max (50%)" "cpu controller not enabled at $CG_BASE - manual step: SANDBOX_DEFAULT_CPU_PERCENT=50 $HARNESS_BIN then verify cpu.max"
  else
    echo "  running CPU-burner via sandboxer with SANDBOX_DEFAULT_CPU_PERCENT=50 (cgroup base: $CG_BASE)"
    SANDBOX_DEFAULT_CPU_PERCENT=50 SANDBOX_CGROUPS_PATH="$CG_BASE" "$HARNESS_BIN" >"$OUT.cg" 2>&1
    if grep -q 'cgroup cpu.max applied' "$OUT.cg" && grep -q '50000 100000' "$OUT.cg"; then
      pass "cgroup cpu.max (50% -> 50000 100000)"
    else
      fail "cgroup cpu.max (50% -> 50000 100000)" "no 'cgroup cpu.max applied' with quota 50000 in harness output"
    fi
  fi
fi

# --- 6. summary ------------------------------------------------------------
if [[ $FAILED -eq 1 ]]; then
  echo ""
  echo "--- harness output (debug, first 120 lines) ---"
  sed -n '1,120p' "$OUT"
fi

echo ""
echo "============================================================"
echo " Linux smoke test summary ($ARCH)"
echo "============================================================"
printf '  %-46s %s\n' "CHECK" "RESULT"
printf '  %-46s %s\n' "-----" "------"
for r in "${RESULTS[@]}"; do
  status="${r%%|*}"
  name="${r#*|}"
  printf '  %-46s %s\n' "$name" "$status"
done
echo "============================================================"

if [[ $FAILED -eq 0 ]]; then
  echo "RESULT: ALL CHECKS PASSED"
  exit 0
else
  echo "RESULT: SOME CHECKS FAILED"
  exit 1
fi
