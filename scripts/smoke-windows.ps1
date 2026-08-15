#!/usr/bin/env powershell
# ============================================================
# SentraZero Windows smoke test
#
# Verifies a single Windows agent environment with one command:
#   powershell -ExecutionPolicy Bypass -File scripts\smoke-windows.ps1
#
# What it does:
#   1. Static gates: `go build ./...` + `go vet ./...`
#   2. Builds:
#        bin\sentra-agent-windows-amd64.exe  (from .\cmd - the REAL agent)
#        bin\sandbox-test.exe                (from .\cmd\sandbox-test)
#   3. Runs the Windows sandbox harness and asserts:
#        simple-echo      -> OK   (THE regression test for the recently
#                                  fixed job-object resume bug - its
#                                  completion is the whole point)
#        write-workdir    -> OK
#        job-memory-cap   -> killed/error (best-effort; process must be
#                            killed by the Job Object memory cap)
#        net-blocked      -> blocked-or-skipped (admin) / best-effort (non-admin)
#
#   If not running as Administrator, firewall-based net-block tests are
#   skipped by design (best-effort) but job-object tests still run.
#
# Compatible with Windows PowerShell 5.1 (no `??`, no ternary, no `&&`).
# Exit code: 0 = all required checks passed, 1 = any required check failed.
# ============================================================

$ErrorActionPreference = 'Continue'

if ([System.Environment]::OSVersion.Platform -ne 'Win32NT') {
  Write-Host ('ERROR: smoke-windows.ps1 must run on Windows (got platform {0})' -f [System.Environment]::OSVersion.Platform)
  exit 2
}

$Root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Set-Location $Root

$BinDir = Join-Path $Root 'bin'
$Agent  = Join-Path $BinDir 'sentra-agent-windows-amd64.exe'
$Harness = Join-Path $BinDir 'sandbox-test.exe'

$script:Failed = $false
$script:Results = @()
$script:HarnessOut = ''

function Record {
  param([string]$Status, [string]$Name, [string]$Detail = '')
  $script:Results += ('{0}|{1}|{2}' -f $Status, $Name, $Detail)
  if ($Detail) {
    Write-Host ('  {0,-6} {1}  ({2})' -f $Status, $Name, $Detail)
  } else {
    Write-Host ('  {0,-6} {1}' -f $Status, $Name)
  }
  if ($Status -eq 'FAIL') { $script:Failed = $true }
}

function Check-ExitCode {
  param([string]$Name)
  if ($LASTEXITCODE -eq 0) { Record 'PASS' $Name } else { Record 'FAIL' $Name ('exit code {0}' -f $LASTEXITCODE) }
}

function Get-HarnessLine {
  param([string]$Name)
  $pattern = '^\s*\[' + [regex]::Escape($Name) + '\]'
  foreach ($l in ($script:HarnessOut -split '\r?\n')) {
    if ($l -match $pattern) { return $l }
  }
  return $null
}

# --- 1. static gates ------------------------------------------------------
Write-Host '==> static gates'
go build ./...
Check-ExitCode 'static gate: go build ./...'
go vet ./...
Check-ExitCode 'static gate: go vet ./...'

# --- 2. build binaries ----------------------------------------------------
Write-Host '==> build binaries'
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
go build -o $Agent .\cmd
Check-ExitCode 'build agent (bin\sentra-agent-windows-amd64.exe)'
go build -o $Harness .\cmd\sandbox-test
Check-ExitCode 'build harness (bin\sandbox-test.exe)'

# --- 3. run Windows sandbox harness ---------------------------------------
Write-Host '==> run Windows sandbox harness'
if (-not (Test-Path $Harness)) {
  Record 'FAIL' 'harness binary exists' ('missing {0}' -f $Harness)
} else {
  $script:HarnessOut = (& $Harness 2>&1 | Out-String)
  $HarnessRC = $LASTEXITCODE
  if ($HarnessRC -eq 0) { Record 'PASS' 'harness run (sandbox-test.exe)' }
  else { Record 'FAIL' 'harness run (sandbox-test.exe)' ('exit code {0}' -f $HarnessRC) }
}

# --- admin detection ------------------------------------------------------
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($isAdmin) {
  Write-Host '  note: running as Administrator - firewall net-block tests are enforceable'
} else {
  Write-Host '  WARN: not running as Administrator - firewall net-block tests are best-effort (skipped by design); job-object tests still run'
}

# --- 4. assertions on harness output --------------------------------------
Write-Host '==> harness assertions'

# simple-echo: THE regression test for the job-object resume bug.
$line = Get-HarnessLine 'simple-echo'
if ([string]::IsNullOrEmpty($line)) {
  Record 'FAIL' 'simple-echo reports OK' 'no harness line'
} elseif ($line -match '\bOK\b') {
  Record 'PASS' 'simple-echo reports OK'
} else {
  Record 'FAIL' 'simple-echo reports OK' ('got: {0}' -f $line)
}

$line = Get-HarnessLine 'write-workdir'
if ([string]::IsNullOrEmpty($line)) {
  Record 'FAIL' 'write-workdir reports OK' 'no harness line'
} elseif ($line -match '\bOK\b') {
  Record 'PASS' 'write-workdir reports OK'
} else {
  Record 'FAIL' 'write-workdir reports OK' ('got: {0}' -f $line)
}

# job-memory-cap: best-effort. The harness reports
# [job-memory-cap] OK: killed by the Job Object memory cap: <err> when the
# cap worked, and ERROR: ... not killed / allocation survived when it did
# not. PASS = the process was killed; FAIL = cap not enforced; absent -> WARN.
$line = Get-HarnessLine 'job-memory-cap'
if ([string]::IsNullOrEmpty($line)) {
  Record 'WARN' 'job-memory-cap killed/error (best-effort)' 'no harness line - skipped'
} elseif ($line -match 'killed' -and $line -notmatch 'not killed') {
  Record 'PASS' 'job-memory-cap killed/error (best-effort)'
} elseif ($line -match 'ERROR|not killed|survived') {
  Record 'FAIL' 'job-memory-cap killed/error (best-effort)' ('memory cap not enforced: {0}' -f $line)
} else {
  Record 'WARN' 'job-memory-cap killed/error (best-effort)' ('unexpected line: {0}' -f $line)
}

# net-blocked: with admin the firewall rule must block; without admin it is
# best-effort by design (job-object tests above still run and are enforced).
$line = Get-HarnessLine 'net-blocked'
if ($isAdmin) {
  if ([string]::IsNullOrEmpty($line)) {
    Record 'WARN' 'net-blocked blocked-or-skipped' 'no harness line'
  } elseif ($line -match '\bOK\b') {
    Record 'FAIL' 'net-blocked blocked-or-skipped' ('network not blocked: {0}' -f $line)
  } else {
    Record 'PASS' 'net-blocked blocked-or-skipped'
  }
} else {
  if (-not [string]::IsNullOrEmpty($line) -and $line -notmatch '\bOK\b') {
    Record 'PASS' 'net-blocked blocked-or-skipped (non-admin)'
  } else {
    Record 'WARN' 'net-blocked blocked-or-skipped (non-admin)' 'firewall block tests skipped without admin - job-object tests still run'
  }
}

# --- 5. summary ------------------------------------------------------------
if ($script:Failed) {
  Write-Host ''
  Write-Host '--- harness output (debug, first 60 lines) ---'
  $script:HarnessOut -split '\r?\n' | Select-Object -First 60 | ForEach-Object { Write-Host $_ }
}

Write-Host ''
Write-Host '============================================================'
Write-Host ' Windows smoke test summary'
Write-Host '============================================================'
Write-Host ('  {0,-46} {1}' -f 'CHECK', 'RESULT')
Write-Host ('  {0,-46} {1}' -f '-----', '------')
foreach ($r in $script:Results) {
  $parts = $r -split '\|', 3
  if ($parts[2]) {
    Write-Host ('  {0,-46} {1}  ({2})' -f $parts[1], $parts[0], $parts[2])
  } else {
    Write-Host ('  {0,-46} {1}' -f $parts[1], $parts[0])
  }
}
Write-Host '============================================================'
if ($script:Failed) {
  Write-Host 'RESULT: SOME CHECKS FAILED'
  exit 1
}
Write-Host 'RESULT: ALL CHECKS PASSED'
exit 0
