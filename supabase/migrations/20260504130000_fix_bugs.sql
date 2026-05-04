-- Fix bugs identified in potential_errors.md

-- 1. Fix public.leases typo in trigger function
DROP FUNCTION IF EXISTS public.cleanup_leases_on_offline() CASCADE;
CREATE OR REPLACE FUNCTION public.cleanup_leases_on_offline()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
    IF NEW.status = 'offline' AND OLD.status != 'offline' THEN
        DELETE FROM public.leases WHERE device_id = NEW.id AND status = 'active';
    END IF;
    RETURN NEW;
END;
$function$
;

-- 2. Remove duplicate cleanup_stuck_jobs function (the one without params)
-- Keep only the function with parameters (p_max_retries, p_org_id)
DROP FUNCTION IF EXISTS public.cleanup_stuck_jobs();

-- 3. Fix device_supports_execution_mode to not always return true for unknown modes
CREATE OR REPLACE FUNCTION public.device_supports_execution_mode(p_device_id uuid, p_mode text)
RETURNS boolean
LANGUAGE plpgsql
STABLE SECURITY DEFINER
AS $function$
DECLARE
    v_docker_available BOOLEAN;
BEGIN
    SELECT docker_available INTO v_docker_available FROM devices WHERE id = p_device_id;
    
    CASE p_mode
        WHEN 'docker' THEN RETURN v_docker_available = true;
        WHEN 'runtime' THEN RETURN true;
        WHEN 'native' THEN RETURN true;
        ELSE RETURN false;  -- Fixed: was RETURN true
    END CASE;
END;
$function$
;

-- 4. Fix cleanup_stuck_jobs - remove reference to non-existent assigned_device_id
CREATE OR REPLACE FUNCTION public.cleanup_stuck_jobs(p_max_retries integer DEFAULT 3, p_org_id uuid DEFAULT NULL::uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_reclaimed_count INTEGER := 0;
  v_dead_letter_count INTEGER := 0;
  v_fixed_count INTEGER := 0;
  v_job RECORD;
  v_lock_key BIGINT;
  v_updated_rows INTEGER;
BEGIN
  -- First, fix any inconsistent states
  UPDATE public.agent_jobs
  SET agent_id = NULL,
      assigned_at = NULL,
      lease_expires_at = NULL,
      last_error = 'fixed: stale agent_id cleared by cleanup_stuck_jobs',
      updated_at = NOW()
  WHERE status = 'pending'
    AND agent_id IS NOT NULL
    AND (p_org_id IS NULL OR org_id = p_org_id)
    AND last_error != 'fixed: stale agent_id cleared by cleanup_stuck_jobs';

  GET DIAGNOSTICS v_fixed_count = ROW_COUNT;

  -- Reclaim expired leases
  FOR v_job IN
    SELECT aj.id, aj.retry_count, aj.max_retries, aj.agent_id, aj.org_id, aj.assigned_at
    FROM public.agent_jobs aj
    WHERE aj.status = 'assigned'
      AND aj.lease_expires_at < NOW() - INTERVAL '2 seconds'
      AND (p_org_id IS NULL OR aj.org_id = p_org_id)
    ORDER BY aj.lease_expires_at ASC
    LIMIT 100
  LOOP
    v_lock_key := hashtextextended(v_job.id::text, 0);
    PERFORM pg_advisory_xact_lock(v_lock_key);

    SELECT status INTO v_job.status
    FROM public.agent_jobs
    WHERE id = v_job.id
    FOR UPDATE;

    IF v_job.status != 'assigned' THEN
      CONTINUE;
    END IF;

    DELETE FROM leases WHERE job_id = v_job.id AND status = 'active';

    IF COALESCE(v_job.retry_count, 0) >= COALESCE(v_job.max_retries, p_max_retries) THEN
      PERFORM public.move_job_to_dead_letter(v_job.id, v_job.org_id);

      UPDATE public.agent_jobs
      SET dead_lettered = true,
          last_error = 'max retries exceeded - auto dead-lettered',
          updated_at = NOW(),
          last_transition_at = NOW()
      WHERE id = v_job.id;

      v_dead_letter_count := v_dead_letter_count + 1;
    ELSE
      UPDATE public.agent_jobs
      SET status = 'pending',
          agent_id = NULL,
          assigned_at = NULL,
          lease_expires_at = NULL,
          retry_count = COALESCE(retry_count, 0) + 1,
          last_error = 'lease expired - auto reclaim',
          updated_at = NOW(),
          last_transition_at = NOW()
      WHERE id = v_job.id
        AND status = 'assigned'
      RETURNING 1 INTO v_updated_rows;

      IF v_updated_rows > 0 THEN
        v_reclaimed_count := v_reclaimed_count + 1;
      END IF;
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'reclaimed', v_reclaimed_count,
    'dead_lettered', v_dead_letter_count,
    'fixed_inconsistent', v_fixed_count,
    'checked_at', NOW()
  );
END;
$function$
;

-- 5. Fix claim_jobs_for_device to verify device org matches job org
CREATE OR REPLACE FUNCTION public.claim_jobs_for_device(p_device_id uuid, p_org_id uuid, p_limit integer DEFAULT 10, p_lease_ttl_seconds integer DEFAULT 1800)
RETURNS TABLE(job_id uuid, job_type text, job_payload jsonb, exec_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    v_interval interval;
    v_exec_id uuid;
    v_job_id_val uuid;
    v_rtype_val text;
    v_rpayload_val jsonb;
    v_row record;
    v_dataset_id uuid;
    v_rechunk_result jsonb;
    v_device_org uuid;
BEGIN
    -- Verify device belongs to the same org
    SELECT org_id INTO v_device_org FROM public.devices WHERE id = p_device_id;
    IF v_device_org IS DISTINCT FROM p_org_id THEN
        RAISE EXCEPTION 'device ownership violation - org_id mismatch';
    END IF;

    v_interval := (p_lease_ttl_seconds || ' seconds')::interval;
    
    FOR v_row IN
        SELECT aj.id AS j_id, aj.job_type AS j_type, aj.payload AS j_payload, 
               aj.execution_id AS j_exec_id, aj.job_dataset_id
        FROM public.agent_jobs aj
        WHERE aj.org_id = p_org_id
            AND aj.status = 'pending'
            AND (
                aj.agent_id IS NULL
                OR aj.agent_id = p_device_id
                OR (aj.agent_id IS NOT NULL AND aj.assigned_at IS NULL)
                OR (aj.agent_id IS NOT NULL AND aj.assigned_at < NOW() - INTERVAL '1 hour')
            )
        ORDER BY
            CASE WHEN aj.agent_id IS NULL THEN 0 ELSE 1 END,
            aj.created_at ASC
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        v_job_id_val := v_row.j_id;
        v_rtype_val := v_row.j_type;
        v_rpayload_val := v_row.j_payload;
        v_exec_id := v_row.j_exec_id;
        
        UPDATE public.agent_jobs
        SET status = 'assigned',
            agent_id = p_device_id,
            assigned_at = NOW(),
            lease_expires_at = NOW() + v_interval,
            updated_at = NOW(),
            last_transition_at = NOW(),
            execution_id = COALESCE(v_exec_id, execution_id)
        WHERE agent_jobs.id = v_job_id_val AND agent_jobs.status = 'pending';
        
        IF FOUND THEN
            INSERT INTO public.job_notification_queue (job_id, agent_id, org_id)
            VALUES (v_job_id_val, p_device_id, p_org_id)
            ON CONFLICT DO NOTHING;
            
            job_id := v_job_id_val;
            job_type := v_rtype_val;
            job_payload := v_rpayload_val;
            exec_id := v_exec_id;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$function$
;

-- 6. Add index on leases.status for better performance
CREATE INDEX IF NOT EXISTS idx_leases_status ON public.leases(status);
CREATE INDEX IF NOT EXISTS idx_leases_status_expires ON public.leases(status, lease_expires_at);

-- 7. Fix agent_jobs job_type check constraint - consolidate to single consistent constraint
ALTER TABLE public.agent_jobs DROP CONSTRAINT IF EXISTS agent_jobs_job_type_check;
ALTER TABLE public.agent_jobs DROP CONSTRAINT IF EXISTS valid_job_type;
ALTER TABLE public.agent_jobs ADD CONSTRAINT agent_jobs_job_type_check 
  CHECK ((job_type = ANY (ARRAY['scan'::text, 'scan_dataset'::text, 'preprocess'::text, 'process'::text, 'process_dataset'::text, 'merge'::text, 'merge_dataset'::text, 'validate'::text, 'export'::text, 'import'::text]));

