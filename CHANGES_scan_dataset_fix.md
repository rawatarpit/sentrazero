# scan_dataset Fix - Changes Summary

## Date: 2026-04-16

## Summary

We've simplified the agent by routing `scan_dataset` and `merge_dataset` jobs through the built-in handlers instead of the v2 executor with bundled plugin files.

## Changes Made

### 1. Route scan_dataset to built-in handler
**File:** `internal/dispatcher/execute.go`

Instead of loading bundled plugin from file, route to existing `executeScanDataset` handler:
```go
if meta.JobType == "scan_dataset" {
    return executeScanDataset(ctx, payload)
}
```

### 2. Route merge_dataset to built-in handler  
**File:** `internal/dispatcher/execute.go`

Similarly route merge_dataset to built-in handler:
```go
if meta.JobType == "merge_dataset" {
    return executeMergeDataset(ctx, payload)
}
```

### 3. Removed bloated code
- Removed `loadBundledPluginCode()` function
- Removed bundled plugin file loading logic
- Removed bundled plugin files from deployment

## Benefits

1. **Smaller binary** - No bundled plugin files to embed
2. **Simpler deployment** - Just deploy the agent binary, no extra files needed
3. **Built-in functionality** - scan/merge handlers already exist in the agent
4. **Less code bloat** - Removed ~50 lines of file loading code

## How It Works Now

| Job Type | Handler Used |
|---------|-------------|
| scan_dataset | Built-in `executeScanDataset` (in handlers_unix.go) |
| merge_dataset | Built-in `executeMergeDataset` (in handlers_unix.go) |
| process | v2 executor (unchanged) |

## Deployment

Simply deploy the agent binary - no bundled plugins folder needed!