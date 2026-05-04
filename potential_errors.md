# Potential Bugs - SentraZero

## Critical Bugs

### 1. **Duplicate Function Definition: `cleanup_stuck_jobs`**
- **Location**: Schema lines 3813-3825 and 3828-3908
- **Issue**: Two functions with the same name but different signatures. PostgreSQL will use the last definition, making the first one (lines 3813-3825) dead code.
- **Impact**: The simple `cleanup_stuck_jobs()` (no params) will never be called. The first function also references non-existent column `assigned_device_id` (should be `agent_id`).
- **Fix**: Remove the first definition (lines 3813-3825) or rename it.

### 2. **Missing Function: `rechunk_for_device`**
- **Location**: Called in `claim_jobs_for_device` (schema line 3398) and `claim_job_with_compatibility` but not defined in schema
- **Issue**: The function `public.rechunk_for_device()` is called but never defined in the provided schema
- **Impact**: Jobs with datasets will fail when claiming
- **Fix**: Implement `rechunk_for_device(p_dataset_id, p_device_id, p_org_id, p_job_type)`

### 3. **Missing Function: `rotate_agent_token`**
- **Location**: Called in `auto_rotate_stale_tokens` (schema line 2854)
- **Issue**: Function `public.rotate_agent_token()` is called but not defined
- **Impact**: Token rotation will fail
- **Fix**: Implement or remove the call

### 4. **Typos in SQL Keywords (Critical Syntax Errors)**
Multiple SQL keywords are misspelled throughout the schema:
- `RAISE` → `RAISE` (lines 2254, 2265, 2269, 2471, 2479, 2490, 2502, 2508, 3943, 3947, 3955, 3959, 3971, 3976, 3996, 4003, 4018, 4028, etc.)
- `RETURNING` → `RETURNING` (lines 2173, 2227, 2696, 3415, 3547, 3596, 3996, 4015, 4021, etc.)
- `CONTINUE` → `CONTINUE` (lines 3223, 3534, 3873, etc.)
- **Impact**: These will cause syntax errors and prevent schema from loading
- **Fix**: Replace all misspellings with correct SQL keywords

## Edge Function Bugs

### 5. **Typo in Supabase Import (Multiple Files)**
- **Location**: `complete_job/index.ts` line 2, `cleanup_stuck_jobs/index.ts` line 2
- **Issue**: `import { createClient } from "https://esm.sh/@supabase/supabase-js@2";`
  - Package name is misspelled as `supabase-js` instead of `@supabase/supabase-js`
- **Impact**: Import will fail, function won't work
- **Fix**: Change to `import { createClient } from "https://esm.sh/@supabase/supabase-js@2";`

### 6. **Typo in `start_job/index.ts` RPC URL**
- **Location**: `start_job/index.ts` line 46
- **Issue**: `` `${supabaseUrl}/rest/v1/rpc/start_job` `` has incorrect backtick usage and extra backslash
- **Impact**: RPC call will fail due to malformed URL
- **Fix**: Use proper template literal: `${supabaseUrl}/rest/v1/rpc/start_job`

### 7. **Field Name Mismatch: `started_at` vs `started_at`**
- **Location**: `complete_job/index.ts` line 60 and schema
- **Issue**: Schema uses `started_at` but code references `started_at` (also schema line 139 has `started_at` definition)
- **Impact**: Job timing tracking will fail
- **Fix**: Use consistent field name `started_at` (as defined in schema line 25)

### 8. **Table Name Typo in `cleanup_leases_on_offline`**
- **Location**: Schema line 3718
- **Issue**: `DELETE FROM public.leases` is missing the 'e' - should be `public.leases`
- **Impact**: Trigger will fail when device goes offline
- **Fix**: Change to `DELETE FROM public.leases`

## Logic Bugs

### 9. **`device_supports_execution_mode` Always Returns True**
- **Location**: Schema lines 4491-4508
- **Issue**: The `ELSE RETURN true;` at line 4505 means any unknown mode returns true
- **Impact**: Invalid execution modes won't be caught
- **Fix**: Return false for unknown modes

### 10. **`cleanup_stuck_jobs` First Definition Uses Wrong Column**
- **Location**: Schema lines 3813-3825
- **Issue**: References `assigned_device_id` which doesn't exist in `agent_jobs` table (column is `agent_id`)
- **Impact**: Function will throw error if ever called
- **Fix**: Change to `agent_id = NULL`

### 11. **Lease Check Has 2-Second Window**
- **Location**: `complete_job_idempotent` schema line 3967
- **Issue**: `lease_expires_at > NOW() - interval '2 seconds'` allows a 2-second grace period
- **Impact**: Jobs might be completed with slightly expired leases
- **Fix**: Remove the grace period or make it configurable

### 12. **`claim_jobs_for_device` Doesn't Verify Device Belongs to Org**
- **Location**: `claim_jobs_for_device` schema lines 3355-3376
- **Issue**: It only checks if job's `org_id` matches, but doesn't verify the device belongs to the same org
- **Impact**: Device from another org could claim jobs
- **Fix**: Add device org verification like other functions do

## Security Issues

### 13. **`cron_secret` Stored in Plain Text**
- **Location**: `cleanup_stuck_jobs/index.ts` line 7
- **Issue**: Cron secret is compared in plain text, should use secure comparison
- **Note**: Code uses `timingSafeEqual` which is good, but the secret is accessed from env

### 14. **RLS Not Enabled on Some Tables**
- **Location**: Schema - tables like `system_config`, `plan_limits`
- **Issue**: These tables don't have `ENABLE ROW LEVEL SECURITY` but may contain sensitive data
- **Impact**: Potential data exposure
- **Fix**: Enable RLS or ensure proper access controls

## Performance Issues

### 15. **Missing Index on `leases.status`**
- **Location**: Schema
- **Issue**: `leases` table has queries filtering by `status` but no index mentioned for this column alone
- **Impact**: Slow queries when checking active leases
- **Fix**: Add index on `leases(status)` or `leases(status, expires_at)`

### 16. **`get_dashboard_stats` Uses Multiple Subqueries**
- **Location**: Schema lines 4894-4916
- **Issue**: Function runs 12 separate COUNT queries instead of a single aggregated query
- **Impact**: Poor performance, especially with large datasets
- **Fix**: Rewrite using CTE or single query with COUNT FILTER

## Data Integrity Issues

### 17. **`agent_jobs.job_type` Check Constraint Inconsistency**
- **Location**: Schema lines 1612 and 1630
- **Issue**: Two different check constraints on `job_type`:
  - Line 1612: `('scan', 'scan_dataset', 'preprocess', 'process', 'process_dataset', 'merge', 'merge_dataset', 'validate', 'export', 'import')`
  - Line 1630: `('process', 'preprocess', 'scan', 'scan_dataset', 'merge', 'merge_dataset', 'unknown')`
- **Impact**: Constraint conflict or confusion about valid job types
- **Fix**: Consolidate to single consistent check constraint

### 18. **Vector Dimension Mismatch Possible**
- **Location**: Schema - `device_vectors.profile_vector` is `vector(16)` but `devices.device_vector` is `vector(64)`
- **Issue**: Code may try to compare or assign between these with different dimensions
- **Impact**: Vector operation errors
- **Fix**: Ensure consistent dimensions or proper casting

## Edge Function Response Issues

### 19. **`claim_jobs_for_device` Response Field Mismatch**
- **Location**: `claim_jobs_for_device/index.ts` lines 48-58
- **Issue**: Normalizes fields like `job_id`, `job_type`, `payload` but the RPC returns different field names (`j_id`, `j_type`, `j_payload`)
- **Impact**: Response mapping may be incorrect
- **Fix**: Verify RPC return column names match the normalization

### 20. **`complete_job` Updates Wrong Table Name**
- **Location**: `complete_job/index.ts` line 132
- **Issue**: Updates `executions` but table name in schema is `executions` (line 596) - this is actually correct
- **Second Issue**: Line 132 uses `executions` (should verify actual table name from schema)
- **Fix**: Verify table name consistency

## Recommendations

1. **Run the schema through a SQL linter** to catch all the `RAISE`/`RETURNING` typos
2. **Add unit tests** for all edge functions
3. **Add integration tests** for the database functions
4. **Set up CI/CD** to catch these issues before deployment
5. **Review all function signatures** to ensure no duplicates with different parameters
