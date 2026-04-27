-- Migration: Fix trigger and add job state transitions
-- Date: 2026-04-27
-- Description: Fix auto_progress_after_scan UUID->bigint cast, add start_job RPC

-- ============================================================================
-- ISSUE 1: auto_progress_after_scan trigger has UUID->bigint cast error
-- This causes the trigger to FAIL silently when dataset status changes to 'scanned'
-- ============================================================================

CREATE OR REPLACE FUNCTION public.auto_progress_after_scan()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  IF NEW.status = 'scanned' AND OLD.status != 'scanned' THEN
    -- FIXED: Use hashtextextended instead of direct UUID->bigint cast
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id::text, 0) % 1000000000, 1);
    
    IF EXISTS (SELECT 1 FROM public.batch_chunks WHERE dataset_id = NEW.id LIMIT 1) THEN
      PERFORM pg_notify('dataset_scanned', json_build_object('dataset_id', NEW.id, 'org_id', NEW.org_id)::text);
      RETURN NEW;
    END IF;
    
    PERFORM pg_notify('dataset_scanned', json_build_object('dataset_id', NEW.id, 'org_id', NEW.org_id)::text);
  END IF;
  RETURN NEW;
END;
$function$;

-- ============================================================================
-- ISSUE 2: Job never transitions from "assigned" to "running"
-- The agent executes the job but never sets started_at or status="running"
-- This means the job appears stuck until complete_job is called
-- ============================================================================

-- Create start_job RPC to transition job to running state
CREATE OR REPLACE FUNCTION public.start_job(
  p_job_id uuid,
  p_agent_id uuid DEFAULT NULL::uuid
)
 RETURNS TABLE(
  success boolean,
  job_id uuid,
  status text,
  started_at timestamptz,
  error text
)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $$
DECLARE
  v_job RECORD;
  v_started_at TIMESTAMPTZ := NOW();
BEGIN
  -- Get current job state
  SELECT id, status, agent_id, execution_id
  INTO v_job
  FROM public.agent_jobs
  WHERE id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT FALSE, p_job_id, NULL, NULL, 'job_not_found'::text;
    RETURN;
  END IF;

  -- Validate agent ownership if provided
  IF p_agent_id IS NOT NULL AND v_job.agent_id != p_agent_id THEN
    RETURN QUERY SELECT FALSE, p_job_id, v_job.status, NULL, 'agent_mismatch'::text;
    RETURN;
  END IF;

  -- Idempotent: don't re-start if already running/completed/failed
  IF v_job.status NOT IN ('assigned', 'pending') THEN
    RETURN QUERY SELECT TRUE, p_job_id, v_job.status, v_job.started_at, 'already_started'::text;
    RETURN;
  END IF;

  -- Transition to running
  UPDATE public.agent_jobs
  SET 
    status = 'running',
    started_at = v_started_at,
    updated_at = v_started_at,
    last_transition_at = v_started_at
  WHERE id = p_job_id;

  -- Also start execution if exists
  IF v_job.execution_id IS NOT NULL THEN
    UPDATE public.executions
    SET 
      status = 'running',
      started_at = v_started_at,
      updated_at = v_started_at
    WHERE id = v_job.execution_id;
  END IF;

  RETURN QUERY SELECT TRUE, p_job_id, 'running'::text, v_started_at, NULL::text;
END;
$$;

-- ============================================================================
-- POST-SCAN FLOW (How it SHOULD work after these fixes):
-- ============================================================================
-- 1. Agent receives scan_dataset job (status = "assigned")
-- 2. Agent calls start_job RPC → status = "running", started_at = NOW()
-- 3. Agent executes scan
-- 4. Agent calls report_dataset_scan → dataset.status = "scanned"
-- 5. Trigger auto_progress_after_scan fires (FIXED UUID→bigint bug)
-- 6. Trigger trg_auto_plan_chunks fires → creates plan_chunks job
-- 7. Agent calls complete_job → job.status = "completed"
-- 8. Trigger trg_update_dataset_on_scan_complete fires (idempotent safety)
-- ============================================================================