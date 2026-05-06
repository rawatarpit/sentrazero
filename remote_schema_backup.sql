


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "_internal";


ALTER SCHEMA "_internal" OWNER TO "postgres";


CREATE EXTENSION IF NOT EXISTS "pg_cron" WITH SCHEMA "pg_catalog";






CREATE EXTENSION IF NOT EXISTS "pg_net" WITH SCHEMA "extensions";






COMMENT ON SCHEMA "public" IS 'Audited 2026-04-30: Removed trg_pre_chunk_on_scan trigger. Checking remaining functions.';



CREATE EXTENSION IF NOT EXISTS "btree_gist" WITH SCHEMA "public";






CREATE EXTENSION IF NOT EXISTS "http" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public";






CREATE TYPE "public"."device_status_enum" AS ENUM (
    'online',
    'offline',
    'available',
    'busy',
    'error',
    'draining'
);


ALTER TYPE "public"."device_status_enum" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "_internal"."list_public_functions"() RETURNS TABLE("function_name" "text", "arguments" "text", "return_type" "text", "language" "text", "function_code" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    p.proname::text AS function_name,
    pg_get_function_identity_arguments(p.oid)::text AS arguments,
    pg_get_function_result(p.oid)::text       AS return_type,
    l.lanname::text                           AS language,
    pg_get_functiondef(p.oid)::text           AS function_code
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_language  l ON l.oid = p.prolang
  WHERE n.nspname = 'public'
  ORDER BY p.proname;
END;
$$;


ALTER FUNCTION "_internal"."list_public_functions"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "_internal"."sentra_rls_table_status"() RETURNS TABLE("schema" "text", "table_name" "text", "rls_enabled" boolean, "rls_forced" boolean)
    LANGUAGE "sql" SECURITY DEFINER
    AS $$
  SELECT
    n.nspname,
    c.relname,
    c.relrowsecurity,
    c.relforcerowsecurity
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND n.nspname IN ('public', 'storage', 'realtime')
  ORDER BY n.nspname, c.relname;
$$;


ALTER FUNCTION "_internal"."sentra_rls_table_status"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer DEFAULT 30) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_lock_id UUID;
  v_expires_at TIMESTAMPTZ;
  v_dev_org uuid;
  v_updated_rows INTEGER;
BEGIN
  v_expires_at := now() + (p_duration_minutes || ' minutes')::INTERVAL;

  SELECT org_id INTO v_dev_org
  FROM public.devices
  WHERE id = p_device_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'device not found';
  END IF;

  IF v_dev_org != p_org_id THEN
    RAISE EXCEPTION 'device ownership violation - org_id mismatch';
  END IF;

  -- First attempt: Try to insert new lock
  INSERT INTO dataset_merge_locks (
    dataset_id,
    agent_id,
    device_id,
    expires_at,
    status
  )
  VALUES (
    p_dataset_id,
    p_agent_id,
    p_device_id,
    v_expires_at,
    'active'
  )
  ON CONFLICT DO NOTHING
  RETURNING id INTO v_lock_id;

  IF v_lock_id IS NOT NULL THEN
    RETURN jsonb_build_object(
      'success', true,
      'lock_id', v_lock_id,
      'expires_at', v_expires_at
    );
  END IF;

  -- Second attempt: Clean up expired locks and retry
  UPDATE dataset_merge_locks
  SET status = 'expired',
      updated_at = now()
  WHERE dataset_id = p_dataset_id
    AND status = 'active'
    AND expires_at < now()
  RETURNING 1 INTO v_updated_rows;

  -- Try to acquire existing expired lock
  UPDATE dataset_merge_locks
  SET agent_id = p_agent_id,
      device_id = p_device_id,
      expires_at = v_expires_at,
      status = 'active',
      updated_at = now()
  WHERE dataset_id = p_dataset_id
    AND status = 'expired'
  RETURNING id INTO v_lock_id;

  IF v_lock_id IS NOT NULL THEN
    RETURN jsonb_build_object(
      'success', true,
      'lock_id', v_lock_id,
      'expires_at', v_expires_at
    );
  END IF;

  -- Third attempt: Insert new lock
  INSERT INTO dataset_merge_locks (
    dataset_id,
    agent_id,
    device_id,
    expires_at,
    status
  )
  VALUES (
    p_dataset_id,
    p_agent_id,
    p_device_id,
    v_expires_at,
    'active'
  )
  ON CONFLICT DO NOTHING
  RETURNING id INTO v_lock_id;

  IF v_lock_id IS NOT NULL THEN
    RETURN jsonb_build_object(
      'success', true,
      'lock_id', v_lock_id,
      'expires_at', v_expires_at
    );
  END IF;

  RAISE EXCEPTION 'merge lock acquisition failed - lock held by another device';

END;
$$;


ALTER FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer) IS 'Acquire merge lock for a dataset. Requires p_org_id to verify device ownership.
Parameters: p_dataset_id, p_agent_id, p_device_id, p_org_id, p_duration_minutes';



CREATE OR REPLACE FUNCTION "public"."acquire_lease"("p_job_id" "uuid", "p_org_id" "uuid", "p_device_id" "uuid", "p_ttl_seconds" integer DEFAULT 1800) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job RECORD;
  v_expires_at TIMESTAMPTZ;
BEGIN
  v_expires_at := NOW() + (p_ttl_seconds || ' seconds')::INTERVAL;

  SELECT id, status, agent_id, org_id
  INTO v_job
  FROM public.agent_jobs
  WHERE id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN 
    RAISE EXCEPTION 'job not found';
  END IF;

  IF v_job.org_id != p_org_id THEN
    RAISE EXCEPTION 'device ownership violation - org_id mismatch';
  END IF;

  IF v_job.agent_id != p_device_id THEN
    RAISE EXCEPTION 'device ownership violation - job assigned to different device';
  END IF;

  INSERT INTO leases (job_id, device_id, lease_expires_at, status)
  VALUES (p_job_id, p_device_id, v_expires_at, 'active')
  ON CONFLICT (job_id) WHERE status = 'active'
  DO UPDATE SET
    device_id = EXCLUDED.device_id,
    lease_expires_at = EXCLUDED.lease_expires_at,
    status = 'active';

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."acquire_lease"("p_job_id" "uuid", "p_org_id" "uuid", "p_device_id" "uuid", "p_ttl_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."activate_pipeline"("p_pipeline_template_id" "uuid", "p_dataset_id" "uuid", "p_org_id" "uuid", "p_created_by" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_pipeline RECORD;
    v_dataset RECORD;
    v_execution_id uuid;
    v_step jsonb;
    v_step_index INT := 0;
    v_total_steps INT;
    v_job_id uuid;
BEGIN
    -- Get pipeline template
    SELECT * INTO v_pipeline
    FROM public.pipeline_templates
    WHERE id = p_pipeline_template_id AND org_id = p_org_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'Pipeline template not found');
    END IF;
    
    -- Get dataset
    SELECT * INTO v_dataset
    FROM public.datasets
    WHERE id = p_dataset_id AND org_id = p_org_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'Dataset not found');
    END IF;
    
    IF v_dataset.status NOT IN ('scanned', 'chunked') THEN
        RETURN jsonb_build_object('ok', false, 'error', 'Dataset must be scanned and chunked first');
    END IF;
    
    v_total_steps := jsonb_array_length(v_pipeline.steps);
    
    -- Create execution record
    INSERT INTO public.executions (
        org_id, dataset_id, pipeline_template_id,
        status, current_step_index, total_steps,
        created_by
    ) VALUES (
        p_org_id, p_dataset_id, p_pipeline_template_id,
        'running', 0, v_total_steps,
        p_created_by
    )
    RETURNING id INTO v_execution_id;
    
    -- Create execution steps
    FOR v_step_index IN 0..(v_total_steps - 1) LOOP
        v_step := v_pipeline.steps->v_step_index;
        
        INSERT INTO public.execution_steps (
            execution_id, step_index, step_type,
            plugin_id, script_id, config,
            status
        ) VALUES (
            v_execution_id, v_step_index,
            v_step->>'type',
            (v_step->>'plugin_id')::uuid,
            v_step->>'script_id',
            COALESCE(v_step->'config', '{}'::jsonb),
            'pending'
        );
        
        -- Create agent job for this step
        INSERT INTO public.agent_jobs (
            org_id, job_type, status,
            payload, execution_id, execution_step_id,
            created_at, updated_at, last_transition_at
        ) VALUES (
            p_org_id,
            COALESCE(v_step->>'type', 'process'),
            'pending',
            jsonb_build_object(
                'dataset_id', p_dataset_id,
                'step_index', v_step_index,
                'step_type', v_step->>'type',
                'plugin_id', v_step->>'plugin_id',
                'config', COALESCE(v_step->'config', '{}'::jsonb)
            ),
            v_execution_id,
            (SELECT id FROM public.execution_steps 
             WHERE execution_id = v_execution_id AND step_index = v_step_index),
            NOW(), NOW(), NOW()
        )
        RETURNING id INTO v_job_id;
    END LOOP;
    
    -- Update dataset status
    UPDATE public.datasets
    SET status = 'processing',
        updated_at = NOW()
    WHERE id = p_dataset_id;
    
    RETURN jsonb_build_object(
        'ok', TRUE,
        'execution_id', v_execution_id,
        'total_steps', v_total_steps,
        'jobs_created', v_total_steps,
        'message', 'Pipeline activated successfully'
    );
END;
$$;


ALTER FUNCTION "public"."activate_pipeline"("p_pipeline_template_id" "uuid", "p_dataset_id" "uuid", "p_org_id" "uuid", "p_created_by" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."advance_pipeline_on_job_complete"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_output_token TEXT;
    v_structured_output JSONB;
BEGIN
    v_output_token := NEW.output_token;

    IF v_output_token IS NOT NULL AND v_output_token != '' THEN
        BEGIN
            v_structured_output := v_output_token::JSONB;
        EXCEPTION WHEN OTHERS THEN
            v_structured_output := jsonb_build_object("raw_output", v_output_token);
        END;
    END IF;

    IF NEW.status = 'completed' AND OLD.status IN ('pending', 'assigned', 'running') THEN
        INSERT INTO job_notification_queue (org_id, job_id, event_type, payload)
        VALUES (
            NEW.org_id,
            NEW.id,
            'job_completed',
            jsonb_build_object(
                'job_id', NEW.id,
                'job_type', NEW.job_type,
                'status', NEW.status,
                'output', v_structured_output,
                'completed_at', NEW.completed_at
            )
        );
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."advance_pipeline_on_job_complete"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."apply_routing_rules"("p_org_id" "uuid", "p_job_type" "text", "p_candidate_device_ids" "uuid"[]) RETURNS TABLE("device_id" "uuid", "score_modifier" numeric, "rule_action" "text")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
    RETURN QUERY
    SELECT drr.device_id,
        CASE drr.action WHEN 'prefer' THEN 1.5 WHEN 'exclude' THEN 0.0 WHEN 'require' THEN 2.0 ELSE 1.0 END,
        drr.action
    FROM device_routing_rules drr
    WHERE drr.org_id = p_org_id AND drr.job_type = p_job_type AND drr.enabled = true
      AND (drr.device_id IS NULL OR drr.device_id = ANY(p_candidate_device_ids))
    ORDER BY drr.priority DESC;
END;
$$;


ALTER FUNCTION "public"."apply_routing_rules"("p_org_id" "uuid", "p_job_type" "text", "p_candidate_device_ids" "uuid"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."assign_agent_job"("p_org_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN public.assign_best_job_to_best_device(p_org_id);
END;
$$;


ALTER FUNCTION "public"."assign_agent_job"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."assign_agent_job"("p_org_id" "uuid", "p_agent_id" "uuid") RETURNS TABLE("ok" boolean, "job_id" "uuid", "job_type" "text", "payload" "jsonb", "error" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY SELECT * FROM public.claim_next_job_for_device(p_org_id, p_agent_id);
END;
$$;


ALTER FUNCTION "public"."assign_agent_job"("p_org_id" "uuid", "p_agent_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job_uuid uuid;
  v_agent_uuid uuid;
  v_job_org uuid;
  v_agent_org uuid;
BEGIN
  v_job_uuid := p_job_id::uuid;
  v_agent_uuid := p_agent_id::uuid;

  IF p_org_id IS NOT NULL THEN
    SELECT org_id INTO v_job_org
    FROM public.agent_jobs
    WHERE id = v_job_uuid;
    
    IF v_job_org IS DISTINCT FROM p_org_id THEN
      RAISE EXCEPTION 'job org_id mismatch';
    END IF;
    
    SELECT org_id INTO v_agent_org
    FROM public.devices
    WHERE id = v_agent_uuid;
    
    IF v_agent_org IS DISTINCT FROM p_org_id THEN
      RAISE EXCEPTION 'agent org_id mismatch';
    END IF;
    
    RETURN public.force_assign_job(v_job_uuid, v_agent_uuid);
  END IF;

  v_job_org := public.safe_cast_uuid(
    nullif(current_setting('app.current_org', true), '')
  );
  
  IF v_job_org IS NULL THEN
    RAISE EXCEPTION 'org_id required: provide p_org_id parameter or set app.current_org';
  END IF;
  
  SELECT org_id INTO v_job_org
  FROM public.agent_jobs
  WHERE id = v_job_uuid;
  
  SELECT org_id INTO v_agent_org
  FROM public.devices
  WHERE id = v_agent_uuid;
  
  IF v_job_org IS DISTINCT FROM v_agent_org THEN
    RAISE EXCEPTION 'job and agent must belong to the same organization';
  END IF;
  
  IF v_job_org IS DISTINCT FROM public.safe_cast_uuid(
    nullif(current_setting('app.current_org', true), '')
  ) THEN
    RAISE EXCEPTION 'org_id mismatch between job and session context';
  END IF;
  
  RETURN public.force_assign_job(v_job_uuid, v_agent_uuid);
END;
$$;


ALTER FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") IS 'Force-assign a specific job to a device. REQUIRES org_id context to prevent cross-org attacks.';



CREATE OR REPLACE FUNCTION "public"."assign_best_job_to_best_device"("p_org_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  _job_row record;
  _device_record record;
  _result jsonb;
  _execution_id uuid;
BEGIN
  PERFORM 1;

  BEGIN
    SELECT aj.id, aj.job_type, aj.payload, aj.execution_id
    INTO STRICT _job_row
    FROM public.agent_jobs aj
    WHERE aj.org_id = p_org_id
      AND aj.status = 'pending'
    ORDER BY aj.created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error', 'no_pending_jobs');
  END;

  SELECT *
  INTO _device_record
  FROM public.match_best_device(
    p_org_id,
    COALESCE(
      (_job_row.payload -> 'chunk_vector')::vector(16),
      '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'::vector(16)
    ),
    _job_row.job_type
  )
  LIMIT 1;

  IF _device_record IS NULL OR _device_record.device_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'error', 'no_available_device');
  END IF;

  -- Create execution if not exists
  IF _job_row.execution_id IS NULL THEN
    INSERT INTO public.executions (
        org_id,
        dataset_id,
        status,
        current_step_index
    ) VALUES (
        p_org_id,
        (_job_row.payload->>'dataset_id')::uuid,
        'running',
        0
    )
    RETURNING id INTO _execution_id;
  ELSE
    _execution_id := _job_row.execution_id;
  END IF;

  UPDATE public.agent_jobs
  SET agent_id   = _device_record.device_id,
      status     = 'assigned',
      assigned_at = now(),
      updated_at = now(),
      execution_id = COALESCE(_execution_id, execution_id)
  WHERE id = _job_row.id;

  _result := jsonb_build_object(
    'ok',              true,
    'assigned_job_id',  _job_row.id,
    'device_id',       _device_record.device_id,
    'job_type',        _job_row.job_type
  );

  RETURN _result;

EXCEPTION WHEN others THEN
  PERFORM public.log_agent_error(
    NULL::uuid, NULL::uuid,
    'assign_best_job_to_best_device failed: ' || SQLERRM
  );
  RETURN jsonb_build_object(
    'ok', false, 'error', 'assign_failed', 'message', SQLERRM
  );
END;
$$;


ALTER FUNCTION "public"."assign_best_job_to_best_device"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."assign_chunk_job_on_insert"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_device_id    UUID;
  v_job_id       UUID;
  v_raw_vector   vector(16);
  v_safe_vector  vector(16);
  v_lease_expires TIMESTAMPTZ := NOW() + INTERVAL '30 minutes';
  v_is_zero      BOOLEAN;

  DEFAULT_VEC CONSTANT vector(16) :=
    '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]';
BEGIN
  -- NO advisory lock needed - UNIQUE index + ON CONFLICT handles race safety

  v_raw_vector := COALESCE(NEW.chunk_vector, DEFAULT_VEC);

  SELECT (v_raw_vector <#> v_raw_vector) = 0
  INTO   v_is_zero;

  v_safe_vector := CASE WHEN COALESCE(v_is_zero, true)
                        THEN DEFAULT_VEC
                        ELSE v_raw_vector
                   END;

  SELECT public.select_best_device(
    NEW.org_id,
    COALESCE(NEW.job_type, 'preprocess'),
    v_safe_vector
  ) INTO v_device_id;

  IF v_device_id IS NOT NULL THEN
    -- Atomic insert with ON CONFLICT as safety net
    INSERT INTO public.agent_jobs (
      org_id,
      job_type,
      payload,
      status,
      agent_id,
      assigned_at,
      lease_expires_at,
      created_at,
      updated_at,
      last_transition_at
    )
    VALUES (
      NEW.org_id,
      COALESCE(NEW.job_type, 'preprocess'),
      jsonb_build_object(
        'chunk_id',    NEW.id,
        'chunk_index', NEW.chunk_index,
        'dataset_id',  NEW.dataset_id
      ),
      'assigned',
      v_device_id,
      NOW(),
      v_lease_expires,
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT (job_chunk_id) 
    WHERE job_type IN ('preprocess', 'process') 
      AND status NOT IN ('completed', 'failed', 'dead')
    DO NOTHING
    RETURNING id INTO v_job_id;

    IF v_job_id IS NOT NULL THEN
      PERFORM public.notify_new_job(v_job_id, v_device_id);
      INSERT INTO public.job_notification_queue (job_id, agent_id)
      VALUES (v_job_id, v_device_id);
    END IF;

  ELSE
    -- No device available - insert as pending
    INSERT INTO public.agent_jobs (
      org_id,
      job_type,
      payload,
      status,
      created_at,
      updated_at,
      last_transition_at
    )
    VALUES (
      NEW.org_id,
      COALESCE(NEW.job_type, 'preprocess'),
      jsonb_build_object(
        'chunk_id',    NEW.id,
        'chunk_index', NEW.chunk_index,
        'dataset_id',  NEW.dataset_id
      ),
      'pending',
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT (job_chunk_id) 
    WHERE job_type IN ('preprocess', 'process') 
      AND status NOT IN ('completed', 'failed', 'dead')
    DO NOTHING;
  END IF;

  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."assign_chunk_job_on_insert"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."auto_assign_merge_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  INSERT INTO public.http_queue (url, body, headers, idempotency_key)
  VALUES (
    '/functions/v1/auto_assign_best_device',
    jsonb_build_object('org_id', NEW.org_id, 'dataset_id', NEW.dataset_id, 'job_type', 'merge'),
    jsonb_build_object('Content-Type', 'application/json'),
    gen_random_uuid()
  )
  ON CONFLICT (idempotency_key) DO NOTHING;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."auto_assign_merge_job"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."auto_create_agent_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_chunk_count INTEGER;
  v_merge_threshold INTEGER := 100;
  v_lock_key BIGINT;
BEGIN
  IF NEW.status != 'processed' OR OLD.status = 'processed' THEN
    RETURN NEW;
  END IF;

  v_lock_key := hashtextextended(NEW.dataset_id::text, 0);
  PERFORM pg_advisory_xact_lock(v_lock_key);

  SELECT COUNT(*) INTO v_chunk_count
  FROM public.batch_chunks
  WHERE dataset_id = NEW.dataset_id
    AND org_id = NEW.org_id
    AND status = 'pending';

  IF v_chunk_count > 0 AND v_chunk_count >= v_merge_threshold THEN
    IF NOT EXISTS (
      SELECT 1 FROM public.agent_jobs
      WHERE job_dataset_id = NEW.dataset_id
        AND job_type = 'merge'
        AND status IN ('pending', 'assigned', 'running')
      LIMIT 1
      FOR UPDATE
    ) THEN
      INSERT INTO public.agent_jobs (
        org_id, job_type, status, payload,
        created_at, updated_at, last_transition_at
      )
      VALUES (
        NEW.org_id,
        'merge',
        'pending',
        jsonb_build_object('dataset_id', NEW.dataset_id, 'merge_level', 1),
        NOW(),
        NOW(),
        NOW()
      )
      ON CONFLICT (job_dataset_id)
      WHERE job_type = 'merge'
        AND status IN ('pending', 'assigned', 'running')
      DO NOTHING;
    END IF;
  END IF;

  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."auto_create_agent_job"() OWNER TO "postgres";


COMMENT ON FUNCTION "public"."auto_create_agent_job"() IS 'Trigger to create a single merge agent_job when batch_chunks complete for a dataset.';



CREATE OR REPLACE FUNCTION "public"."auto_progress_after_scan"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
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
$$;


ALTER FUNCTION "public"."auto_progress_after_scan"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."auto_rotate_stale_tokens"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  rec RECORD;
  v_rotated INTEGER := 0;
  v_failed INTEGER := 0;
BEGIN
  FOR rec IN
    SELECT d.id, d.org_id, d.name, COALESCE(d.token_rotate_fail_count, 0) as fail_count
    FROM public.devices d
    WHERE d.last_refresh < NOW() - INTERVAL '7 days'
      AND COALESCE(d.token_rotate_fail_count, 0) < 5
  LOOP
    BEGIN
      PERFORM public.rotate_agent_token(rec.id, rec.org_id);

      UPDATE public.devices
      SET token_rotate_fail_count = 0
      WHERE id = rec.id;

      v_rotated := v_rotated + 1;

    EXCEPTION WHEN OTHERS THEN
      v_failed := v_failed + 1;

      UPDATE public.devices
      SET token_rotate_fail_count = rec.fail_count + 1
      WHERE id = rec.id;

      IF rec.fail_count + 1 >= 5 THEN
        INSERT INTO public.system_logs (event_type, message)
        VALUES (
          'token_rotate_threshold_exceeded',
          format('Device %s (%s) has exceeded token rotation failure threshold', rec.id, rec.name)
        );
      ELSE
        INSERT INTO public.system_logs (event_type, message)
        VALUES (
          'token_rotate_error',
          format('Failed to rotate token for device %s (%s): %s', rec.id, rec.name, SQLERRM)
        );
      END IF;
    END;
  END LOOP;

  INSERT INTO public.system_logs (event_type, message)
  VALUES (
    'token_maintenance',
    format('Token rotation: %s rotated, %s failed', v_rotated, v_failed)
  );
END;
$$;


ALTER FUNCTION "public"."auto_rotate_stale_tokens"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."batch_assign_jobs_atomic"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer DEFAULT 10, "p_job_type_filter" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job RECORD;
  v_claimed INTEGER := 0;
  v_lease_expires TIMESTAMPTZ := NOW() + INTERVAL '30 minutes';
  v_jobs jsonb := '[]'::jsonb;
BEGIN
  FOR v_job IN
    SELECT aj.id, aj.job_type, aj.payload, aj.created_at, aj.execution_id
    FROM public.agent_jobs aj
    WHERE aj.status = 'pending'
      AND aj.org_id = p_org_id
      AND (p_job_type_filter IS NULL OR aj.job_type = p_job_type_filter)
    ORDER BY aj.created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  LOOP
    UPDATE public.agent_jobs
    SET status = 'assigned',
        agent_id = p_device_id,
        assigned_at = NOW(),
        updated_at = NOW(),
        lease_expires_at = v_lease_expires
    WHERE id = v_job.id;

    v_jobs := v_jobs || jsonb_build_object(
      'id', v_job.id,
      'job_type', v_job.job_type,
      'payload', v_job.payload,
      'created_at', v_job.created_at,
      'execution_id', v_job.execution_id
    );

    v_claimed := v_claimed + 1;
  END LOOP;

  RETURN jsonb_build_object(
    'jobs', v_jobs,
    'assigned', v_claimed
  );
END;
$$;


ALTER FUNCTION "public"."batch_assign_jobs_atomic"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_job_type_filter" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."calculate_dependency_hash"("p_runtime_type" "text", "p_runtime_dependencies" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN encode(digest(
        COALESCE(p_runtime_type, 'native') || '|' || 
        COALESCE(jsonb_pretty(p_runtime_dependencies), ''),
        'sha256'
    ), 'hex');
END;
$$;


ALTER FUNCTION "public"."calculate_dependency_hash"("p_runtime_type" "text", "p_runtime_dependencies" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."calculate_retry_backoff"("p_retry_count" integer, "p_base_delay_seconds" integer DEFAULT 5, "p_max_delay_seconds" integer DEFAULT 300, "p_multiplier" numeric DEFAULT 2.0) RETURNS interval
    LANGUAGE "plpgsql" STABLE
    AS $$
DECLARE
    v_delay NUMERIC;
BEGIN
    v_delay := p_base_delay_seconds * POWER(p_multiplier, p_retry_count);
    v_delay := LEAST(v_delay, p_max_delay_seconds::NUMERIC);
    RETURN (v_delay || ' seconds')::INTERVAL;
END;
$$;


ALTER FUNCTION "public"."calculate_retry_backoff"("p_retry_count" integer, "p_base_delay_seconds" integer, "p_max_delay_seconds" integer, "p_multiplier" numeric) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_result jsonb;
BEGIN
  UPDATE devices
  SET last_policy_update = now()
  WHERE id = p_device_id
    AND (last_policy_update IS NULL OR last_policy_update < now() - interval '5 seconds')
  RETURNING jsonb_build_object(
    'cooldown_active', false,
    'device_id', id
  ) INTO v_result;

  IF v_result IS NULL then
    RETURN jsonb_build_object(
      'cooldown_active', true,
      'device_id', p_device_id
    );
  END IF;

  RETURN v_result;
END;
$$;


ALTER FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") IS 'Atomic cooldown check for agent health policy updates';



CREATE OR REPLACE FUNCTION "public"."check_http_queue_depth"() RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_count INT;
    v_threshold INT := 50;
    v_old_items INT;
BEGIN
    SELECT COUNT(*), COUNT(*) FILTER (WHERE created_at < NOW() - INTERVAL '5 minutes')
    INTO v_count, v_old_items
    FROM http_queue
    WHERE processed = false;

    IF v_old_items > v_threshold THEN
        INSERT INTO alert_history (org_id, condition_type, actual_value, threshold_value, triggered_at)
        VALUES (NULL, 'http_queue_depth', v_old_items, v_threshold, NOW());
    END IF;

    RETURN false;
END;
$$;


ALTER FUNCTION "public"."check_http_queue_depth"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."check_org_quota"("p_org_id" "uuid", "p_quota_type" "text", "p_value" integer) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_limit INTEGER;
    v_current INTEGER;
BEGIN
    SELECT CASE p_quota_type 
        WHEN 'devices' THEN max_devices
        WHEN 'jobs' THEN max_concurrent_jobs
        WHEN 'environments' THEN max_environments
        ELSE NULL
    END INTO v_limit
    FROM public.org_quotas
    WHERE org_id = p_org_id;

    IF v_limit IS NULL THEN
        RETURN TRUE;
    END IF;

    CASE p_quota_type
        WHEN 'devices' THEN 
            SELECT COUNT(*) INTO v_current FROM public.devices WHERE org_id = p_org_id;
        WHEN 'jobs' THEN 
            SELECT COUNT(*) INTO v_current FROM public.agent_jobs WHERE org_id = p_org_id AND status IN ('assigned', 'running');
        WHEN 'environments' THEN 
            SELECT COUNT(*) INTO v_current FROM public.runtime_environments WHERE org_id = p_org_id AND invalidated_at IS NULL;
    END CASE;

    RETURN v_current + p_value <= v_limit;
END;
$$;


ALTER FUNCTION "public"."check_org_quota"("p_org_id" "uuid", "p_quota_type" "text", "p_value" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."check_plan_limit"("p_org_id" "uuid", "p_limit_type" "text", "p_increment" integer DEFAULT 1) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_plan text;
    v_max_devices integer;
    v_max_jobs integer;
    v_current integer;
BEGIN
    SELECT o.plan INTO v_plan FROM orgs o WHERE o.id = p_org_id;
    IF v_plan IS NULL THEN v_plan := 'free'; END IF;

    SELECT max_devices, max_concurrent_jobs INTO v_max_devices, v_max_jobs FROM plan_limits WHERE plan_name = v_plan;

    IF p_limit_type = 'device' AND v_max_devices IS NOT NULL THEN
        SELECT COUNT(*) INTO v_current FROM devices WHERE org_id = p_org_id;
        IF v_current + p_increment > v_max_devices THEN RETURN false; END IF;
    ELSIF p_limit_type = 'job' AND v_max_jobs IS NOT NULL THEN
        SELECT COUNT(*) INTO v_current FROM agent_jobs WHERE org_id = p_org_id AND status IN ('assigned', 'running');
        IF v_current + p_increment > v_max_jobs THEN RETURN false; END IF;
    END IF;

    RETURN true;
END;
$$;


ALTER FUNCTION "public"."check_plan_limit"("p_org_id" "uuid", "p_limit_type" "text", "p_increment" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."check_platform_signing_configured"() RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    RETURN EXISTS (SELECT 1 FROM plugin_signing_keys WHERE revoked_at IS NULL LIMIT 1);
END;
$$;


ALTER FUNCTION "public"."check_platform_signing_configured"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_job_with_compatibility"("p_org_id" "uuid", "p_device_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("ok" boolean, "job_id" "uuid", "job_type" "text", "payload" "jsonb", "runtime_type" "text", "runtime_dependencies" "jsonb", "entrypoint" "text", "execution_mode" "text", "dependency_hash" "text", "run_id" "uuid", "attempt_number" integer, "environment_id" "uuid", "actual_execution_mode" "text", "execution_policy" "jsonb", "error" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_job RECORD;
    v_device RECORD;
    v_plugin RECORD;
    v_policy RECORD;
    v_selected_job_id UUID;
    v_run_id UUID;
    v_attempt_number INTEGER;
    v_environment_id UUID;
    v_actual_mode TEXT;
    v_fallback_reason TEXT;
    v_compatible BOOLEAN;
    v_runtime_check BOOLEAN;
    v_mode_check BOOLEAN;
    v_capability_check BOOLEAN;
    v_has_warm_env BOOLEAN;
    v_mode_priority JSONB;
    v_score INTEGER := 0;
    v_best_score INTEGER := -1;
    v_best_job_id UUID;
BEGIN
    -- Get device info
    IF p_device_id IS NULL THEN
        SELECT d.id, d.org_id, d.runtime_supported, d.docker_available, d.status, 
               d.active_jobs, d.max_concurrency, d.python_version, d.node_version,
               d.required_arch, d.required_os, d.runtime_cache, d.region
        INTO v_device
        FROM public.devices d
        WHERE d.org_id = p_org_id
            AND d.status IN ('online', 'available', 'busy')
            AND (d.active_jobs IS NULL OR d.active_jobs < COALESCE(d.max_concurrency, 1))
        ORDER BY d.active_jobs ASC NULLS FIRST
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    ELSE
        SELECT d.id, d.org_id, d.runtime_supported, d.docker_available, d.status,
               d.active_jobs, d.max_concurrency, d.python_version, d.node_version,
               d.required_arch, d.required_os, d.runtime_cache, d.region
        INTO v_device
        FROM public.devices
        WHERE id = p_device_id AND org_id = p_org_id 
            AND status IN ('online', 'available', 'busy')
        FOR UPDATE SKIP LOCKED;
    END IF;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'no_available_device'::TEXT;
        RETURN;
    END IF;

    -- Find compatible job with runtime-aware scoring
    FOR v_job IN
        SELECT aj.id, aj.job_type, aj.payload, aj.runtime_type, aj.runtime_dependencies, 
               aj.entrypoint, aj.execution_mode, aj.plugin_id, aj.run_id, aj.attempt_number,
               aj.dependency_hash, aj.environment_id, aj.actual_execution_mode,
               aj.execution_policy_id, aj.dependency_lock_hash, aj.created_at
        FROM public.agent_jobs aj
        WHERE aj.status = 'pending'
            AND aj.org_id = p_org_id
        ORDER BY aj.created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 20
    LOOP
        v_score := 0;
        v_job.runtime_type := COALESCE(v_job.runtime_type, 'native');
        v_job.execution_mode := COALESCE(v_job.execution_mode, 'native');
        
        -- Get plugin info
        IF v_job.plugin_id IS NOT NULL THEN
            SELECT * INTO v_plugin
            FROM public.plugins
            WHERE id::text = v_job.plugin_id;
            
            IF FOUND THEN
                v_job.runtime_type := COALESCE(v_job.runtime_type, v_plugin.runtime_type, 'native');
                v_job.runtime_dependencies := COALESCE(v_job.runtime_dependencies, v_plugin.runtime_dependencies, '{}'::jsonb);
            END IF;
        END IF;

        -- Get execution policy
        v_mode_priority := '[{"mode": "runtime", "fallback": ["docker", "native"]}]'::jsonb;
        
        IF v_job.execution_policy_id IS NOT NULL THEN
            SELECT * INTO v_policy FROM public.execution_policies WHERE id = v_job.execution_policy_id;
            IF FOUND AND v_policy.enabled THEN
                v_mode_priority := v_policy.mode_priority;
            END IF;
        END IF;

        -- Determine execution mode and check compatibility
        v_compatible := false;
        v_actual_mode := NULL;
        
        FOR i IN 0..jsonb_array_length(v_mode_priority) - 1 LOOP
            v_actual_mode := v_mode_priority->i->>'mode';
            
            v_runtime_check := public.device_supports_runtime(v_device.id, v_job.runtime_type);
            v_mode_check := public.device_supports_execution_mode(v_device.id, v_actual_mode);
            
            v_capability_check := true;
            IF v_job.plugin_id IS NOT NULL THEN
                v_capability_check := public.device_matches_requirements(
                    v_device.id,
                    v_job.runtime_type,
                    COALESCE(v_plugin.min_python_version, v_plugin.min_node_version),
                    COALESCE(v_plugin.required_arch, v_device.required_arch),
                    COALESCE(v_plugin.required_os, v_device.required_os)
                );
            END IF;

            IF v_runtime_check AND v_mode_check AND v_capability_check THEN
                v_compatible := true;
                EXIT;
            END IF;
        END LOOP;

        IF NOT v_compatible THEN
            CONTINUE;
        END IF;

        -- Runtime-aware scoring: boost score if device has warm environment
        IF v_job.dependency_lock_hash IS NOT NULL THEN
            v_has_warm_env := public.device_has_warm_environment(
                v_device.id,
                v_job.runtime_type,
                COALESCE(v_device.python_version, v_device.node_version, 'latest'),
                v_job.dependency_lock_hash
            );
            IF v_has_warm_env THEN
                v_score := v_score + 100; -- Major boost for warm environment
            END IF;
        END IF;

        -- Base score from created_at (older jobs get slight priority)
        v_score := v_score + EXTRACT(EPOCH FROM (NOW() - v_job.created_at))::INTEGER;

        IF v_score > v_best_score THEN
            v_best_score := v_score;
            v_best_job_id := v_job.id;
        END IF;
    END LOOP;

    IF v_best_job_id IS NULL THEN
        RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'no_compatible_job'::TEXT;
        RETURN;
    END IF;

    -- Get the best job
    SELECT * INTO v_job
    FROM public.agent_jobs
    WHERE id = v_best_job_id;

    -- Calculate dependency hash for environment
    v_run_id := COALESCE(v_job.run_id, gen_random_uuid());
    v_attempt_number := COALESCE(v_job.attempt_number, 1);
    
    IF v_job.runtime_type != 'native' AND v_job.runtime_dependencies IS NOT NULL THEN
        v_job.dependency_lock_hash := COALESCE(
            v_job.dependency_lock_hash,
            public.calculate_dependency_hash(v_job.runtime_type, v_job.runtime_dependencies)
        );
        
        SELECT public.get_or_create_runtime_environment(
            p_org_id,
            v_job.runtime_type,
            COALESCE(v_device.python_version, v_device.node_version, 'latest'),
            v_job.dependency_lock_hash,
            v_device.id
        ) INTO v_environment_id;
    END IF;

    -- Update device runtime cache
    IF v_environment_id IS NOT NULL THEN
        UPDATE public.devices
        SET runtime_cache = COALESCE(runtime_cache, '[]'::jsonb) || 
            jsonb_build_array(
                jsonb_build_object(
                    'runtime_type', v_job.runtime_type,
                    'runtime_version', COALESCE(v_device.python_version, v_device.node_version, 'latest'),
                    'dependency_lock_hash', v_job.dependency_lock_hash,
                    'environment_id', v_environment_id,
                    'last_used_at', NOW()::TEXT
                )
            ),
            updated_at = NOW()
        WHERE id = v_device.id;
    END IF;

    -- Atomic update
    UPDATE public.agent_jobs
    SET status = 'assigned',
        agent_id = v_device.id,
        assigned_at = NOW(),
        updated_at = NOW(),
        run_id = v_run_id,
        attempt_number = v_attempt_number,
        environment_id = v_environment_id,
        actual_execution_mode = v_actual_mode,
        dependency_hash = COALESCE(dependency_hash, v_job.dependency_lock_hash)
    WHERE id = v_best_job_id
    RETURNING run_id, attempt_number, environment_id, actual_execution_mode, execution_policy_id
    INTO v_run_id, v_attempt_number, v_environment_id, v_actual_mode, v_job.execution_policy_id;

    -- Update device
    UPDATE public.devices
    SET active_jobs = COALESCE(active_jobs, 0) + 1,
        current_job_id = v_best_job_id,
        updated_at = NOW()
    WHERE id = v_device.id;

    -- Get policy
    IF v_job.execution_policy_id IS NOT NULL THEN
        SELECT * INTO v_policy FROM public.execution_policies WHERE id = v_job.execution_policy_id;
    END IF;

    RETURN QUERY SELECT 
        TRUE, 
        v_best_job_id, 
        v_job.job_type, 
        v_job.payload,
        v_job.runtime_type,
        v_job.runtime_dependencies,
        v_job.entrypoint,
        v_job.execution_mode,
        v_job.dependency_lock_hash,
        v_run_id,
        v_attempt_number,
        v_environment_id,
        v_actual_mode,
        CASE WHEN v_policy IS NOT NULL THEN 
            jsonb_build_object(
                'max_retries', v_policy.max_retries,
                'retry_backoff_seconds', v_policy.retry_backoff_seconds,
                'default_timeout_seconds', v_policy.default_timeout_seconds,
                'hard_timeout_seconds', v_policy.hard_timeout_seconds,
                'retryable_errors', v_policy.retryable_errors,
                'fatal_errors', v_policy.fatal_errors
            )
        ELSE NULL END,
        NULL::TEXT;
END;
$$;


ALTER FUNCTION "public"."claim_job_with_compatibility"("p_org_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_jobs_for_device"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer DEFAULT 10, "p_lease_ttl_seconds" integer DEFAULT 1800) RETURNS TABLE("job_id" "uuid", "job_type" "text", "job_payload" "jsonb", "exec_id" "uuid")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_interval interval;
    v_exec_id uuid;
    v_job_id_val uuid;
    v_rtype_val text;
    v_rpayload_val jsonb;
    v_row record;
    v_dataset_id uuid;
    v_rechunk_result jsonb;
BEGIN
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
        
        -- Extract dataset_id from job_dataset_id or from payload
        v_dataset_id := v_row.job_dataset_id;
        IF v_dataset_id IS NULL AND v_rpayload_val IS NOT NULL THEN
            v_dataset_id := (v_rpayload_val->>'dataset_id')::uuid;
        END IF;
        
        -- Re-chunk for this device if dataset exists
        IF v_dataset_id IS NOT NULL AND v_rtype_val IN ('process', 'preprocess') THEN
            SELECT public.rechunk_for_device(v_dataset_id, p_device_id, p_org_id, v_rtype_val)
            INTO v_rechunk_result;
            
            IF (v_rechunk_result->>'ok')::boolean = TRUE THEN
                RAISE NOTICE 'Re-chunked dataset % for device %: %', 
                    v_dataset_id, p_device_id, v_rechunk_result->>'new_chunks';
            END IF;
        END IF;
        
        IF v_exec_id IS NULL THEN
            INSERT INTO public.executions (org_id, dataset_id, status, current_step_index)
            VALUES (p_org_id, v_dataset_id, 'running', 0)
            RETURNING id INTO v_exec_id;
        END IF;
        
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
$$;


ALTER FUNCTION "public"."claim_jobs_for_device"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_lease_ttl_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_next_job_for_device"("p_org_id" "uuid", "p_device_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("ok" boolean, "job_id" "uuid", "job_type" "text", "payload" "jsonb", "runtime_type" "text", "runtime_dependencies" "jsonb", "entrypoint" "text", "execution_mode" "text", "error" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_job RECORD;
    v_device RECORD;
    v_selected_job_id UUID;
    v_policies RECORD;
    v_runtime_check BOOLEAN;
    v_mode_check BOOLEAN;
BEGIN
    IF p_device_id IS NULL THEN
        SELECT d.id, d.org_id, d.runtime_supported, d.docker_available, d.status, d.active_jobs, d.max_concurrency
        INTO v_device
        FROM public.devices d
        WHERE d.org_id = p_org_id
            AND d.status IN ('online', 'available', 'busy')
            AND (d.active_jobs IS NULL OR d.active_jobs < COALESCE(d.max_concurrency, 1))
        ORDER BY d.active_jobs ASC NULLS FIRST
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        IF NOT FOUND THEN
            RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'no_available_device'::text;
            RETURN;
        END IF;
    ELSE
        SELECT id, org_id, runtime_supported, docker_available, status, active_jobs, max_concurrency
        INTO v_device
        FROM public.devices
        WHERE id = p_device_id
            AND org_id = p_org_id
            AND status IN ('online', 'available', 'busy')
        FOR UPDATE SKIP LOCKED;

        IF NOT FOUND THEN
            IF EXISTS (SELECT 1 FROM public.devices WHERE id = p_device_id AND org_id = p_org_id AND status = 'offline') THEN
                RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device_offline_cannot_claim_jobs'::text;
            ELSE
                RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device_not_available'::text;
            END IF;
            RETURN;
        END IF;
    END IF;

    -- Check device policies
    SELECT * INTO v_policies
    FROM public.device_policies
    WHERE (device_id = v_device.id OR device_id IS NULL)
        AND org_id = p_org_id
        AND enabled = true
    ORDER BY device_id DESC NULLS LAST
    LIMIT 1;

    IF v_policies.max_concurrency IS NOT NULL THEN
        IF v_device.active_jobs >= v_policies.max_concurrency THEN
            RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device_at_concurrency_limit'::text;
            RETURN;
        END IF;
    END IF;

    -- Find pending job and verify runtime compatibility
    FOR v_job IN
        SELECT aj.id, aj.job_type, aj.payload, aj.runtime_type, aj.runtime_dependencies,
               aj.entrypoint, aj.execution_mode, aj.agent_id, aj.assigned_at, aj.created_at
        FROM public.agent_jobs aj
        WHERE aj.status = 'pending'
            AND aj.org_id = p_org_id
            AND (
                aj.agent_id IS NULL
                OR aj.agent_id = p_device_id
                OR (aj.agent_id IS NOT NULL AND aj.assigned_at IS NULL)
                OR (aj.agent_id IS NOT NULL AND aj.assigned_at < NOW() - INTERVAL '1 hour')
            )
        ORDER BY
            CASE WHEN aj.agent_id IS NULL THEN 0 ELSE 1 END,
            aj.created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 10
    LOOP
        -- Check runtime compatibility
        v_runtime_check := public.device_supports_runtime(
            v_device.id,
            COALESCE(v_job.runtime_type, 'native')
        );

        -- Check execution mode compatibility
        v_mode_check := public.device_supports_execution_mode(
            v_device.id,
            COALESCE(v_job.execution_mode, 'native')
        );

        -- Skip incompatible jobs
        IF NOT v_runtime_check OR NOT v_mode_check THEN
            CONTINUE;
        END IF;

        v_selected_job_id := v_job.id;

        UPDATE public.agent_jobs
        SET status = 'assigned',
            agent_id = v_device.id,
            assigned_at = NOW(),
            updated_at = NOW(),
            last_transition_at = NOW(),
            lease_expires_at = NOW() + INTERVAL '30 minutes'
        WHERE id = v_selected_job_id;

        IF FOUND THEN
            UPDATE public.devices
            SET active_jobs = COALESCE(active_jobs, 0) + 1,
                current_job_id = v_selected_job_id,
                updated_at = NOW()
            WHERE id = v_device.id;

            INSERT INTO public.agent_worker_activity (job_id, device_id, job_type, status, started_at)
            VALUES (v_selected_job_id, v_device.id, v_job.job_type, 'assigned', NOW());

            RETURN QUERY SELECT
                TRUE,
                v_selected_job_id,
                v_job.job_type,
                v_job.payload,
                v_job.runtime_type,
                v_job.runtime_dependencies,
                v_job.entrypoint,
                v_job.execution_mode,
                NULL::text;
            RETURN;
        END IF;
    END LOOP;

    -- Fallback: try to claim any compatible job
    UPDATE public.agent_jobs
    SET status = 'assigned',
        agent_id = v_device.id,
        assigned_at = NOW(),
        updated_at = NOW(),
        last_transition_at = NOW(),
        lease_expires_at = NOW() + INTERVAL '30 minutes'
    WHERE id = (
        SELECT id
        FROM public.agent_jobs
        WHERE status = 'pending'
            AND org_id = p_org_id
            AND (
                agent_id IS NULL
                OR agent_id = p_device_id
                OR (agent_id IS NOT NULL AND assigned_at IS NULL)
                OR (agent_id IS NOT NULL AND assigned_at < NOW() - INTERVAL '1 hour')
            )
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, job_type, payload, runtime_type, runtime_dependencies, entrypoint, execution_mode
    INTO v_selected_job_id, v_job.job_type, v_job.payload, v_job.runtime_type, v_job.runtime_dependencies, v_job.entrypoint, v_job.execution_mode;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'no_pending_jobs'::text;
        RETURN;
    END IF;

    RETURN QUERY SELECT
        TRUE,
        v_selected_job_id,
        v_job.job_type,
        v_job.payload,
        v_job.runtime_type,
        v_job.runtime_dependencies,
        v_job.entrypoint,
        v_job.execution_mode,
        NULL::text;
END;
$$;


ALTER FUNCTION "public"."claim_next_job_for_device"("p_org_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_agent_worker_activity"("p_days_old" integer DEFAULT 30) RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE v_deleted integer;
BEGIN
  INSERT INTO public.system_logs (event_type, message)
  SELECT 'worker_activity_archived',
         format('Worker %s completed %s job %s', worker_id, job_type, job_id)
  FROM public.agent_worker_activity
  WHERE finished_at IS NOT NULL AND finished_at < NOW() - (p_days_old || ' days')::interval
  ON CONFLICT DO NOTHING;

  WITH deleted AS (
    DELETE FROM public.agent_worker_activity
    WHERE finished_at IS NOT NULL AND finished_at < NOW() - (p_days_old || ' days')::interval
    RETURNING id
  )
  SELECT COUNT(*) INTO v_deleted FROM deleted;
  INSERT INTO public.system_logs (event_type, message)
  VALUES ('worker_activity_cleanup', format('agent_worker_activity cleanup: %s rows deleted', v_deleted));
  RETURN v_deleted;
END;
$$;


ALTER FUNCTION "public"."cleanup_agent_worker_activity"("p_days_old" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_duplicate_cron_jobs"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'cron', 'pg_catalog'
    AS $$
BEGIN
    -- Use direct DELETE with proper permission (run as superuser)
    -- Remove duplicate prune_old_system_logs (keep job 20, remove job 7)
    DELETE FROM cron.job WHERE jobid = 7;
    
    -- Remove duplicate prune_old_agent_metrics (keep job 21, remove job 10)  
    DELETE FROM cron.job WHERE jobid = 10;
END;
$$;


ALTER FUNCTION "public"."cleanup_duplicate_cron_jobs"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_expired_merge_locks"("p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS integer
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_count INT;
BEGIN
  UPDATE public.dataset_merge_locks l
  SET status = 'expired',
      updated_at = now()
  WHERE l.status = 'active'
    AND l.expires_at < now()
    AND (
      p_org_id IS NULL
      OR EXISTS (
        SELECT 1
        FROM public.devices d
        WHERE d.id = l.device_id
          AND d.org_id = p_org_id
      )
    );

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;


ALTER FUNCTION "public"."cleanup_expired_merge_locks"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_job_notification_queue"("p_days_old" integer DEFAULT 7) RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE v_deleted integer;
BEGIN
  WITH deleted AS (
    DELETE FROM public.job_notification_queue
    WHERE processed = true AND created_at < NOW() - (p_days_old || ' days')::interval
    RETURNING id
  )
  SELECT COUNT(*) INTO v_deleted FROM deleted;
  INSERT INTO public.system_logs (event_type, message)
  VALUES ('queue_cleanup', format('job_notification_queue cleanup: %s rows deleted', v_deleted));
  RETURN v_deleted;
END;
$$;


ALTER FUNCTION "public"."cleanup_job_notification_queue"("p_days_old" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_leases_on_offline"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.status = 'offline' AND OLD.status != 'offline' THEN
        DELETE FROM public.leases WHERE device_id = NEW.id AND status = 'active';
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."cleanup_leases_on_offline"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_offline_device_leases"("p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_cleaned_leases integer := 0;
    v_reclaimed_jobs integer := 0;
    v_offline_device RECORD;
BEGIN
    FOR v_offline_device IN
        SELECT d.id, d.org_id, d.name
        FROM public.devices d
        WHERE d.status = 'offline'
          AND (p_org_id IS NULL OR d.org_id = p_org_id)
          AND EXISTS (
              SELECT 1 FROM leases l 
              WHERE l.device_id = d.id AND l.status = 'active'
          )
    LOOP
        UPDATE leases
        SET status = 'cancelled'
        WHERE device_id = v_offline_device.id AND status = 'active';

        GET DIAGNOSTICS v_cleaned_leases = ROW_COUNT;

        UPDATE agent_jobs
        SET status = 'pending',
            agent_id = NULL,
            assigned_at = NULL,
            lease_expires_at = NULL,
            retry_count = COALESCE(retry_count, 0) + 1,
            last_error = 'device went offline - job reclaimed',
            updated_at = NOW(),
            last_transition_at = NOW()
        WHERE agent_id = v_offline_device.id
          AND status IN ('assigned', 'running')
        RETURNING 1 INTO v_reclaimed_jobs;

        INSERT INTO system_logs (event_type, message)
        VALUES (
            'offline_device_lease_cleanup',
            format('Device %s (%s): %s leases released, %s jobs reclaimed',
                v_offline_device.id, v_offline_device.name, v_cleaned_leases, v_reclaimed_jobs)
        );
    END LOOP;

    RETURN jsonb_build_object(
        'ok', true,
        'devices_processed', (
            SELECT COUNT(DISTINCT device_id) 
            FROM leases 
            WHERE status = 'cancelled' 
              AND device_id IN (SELECT id FROM devices WHERE status = 'offline')
        ),
        'cleaned_at', NOW()
    );
END;
$$;


ALTER FUNCTION "public"."cleanup_offline_device_leases"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_old_agent_jobs"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    DELETE FROM public.agent_jobs
    WHERE status IN ('completed', 'failed', 'dead_letter')
    AND updated_at < NOW() - INTERVAL '30 days';
END;
$$;


ALTER FUNCTION "public"."cleanup_old_agent_jobs"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_old_benchmarks"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    DELETE FROM device_benchmarks
    WHERE created_at < NOW() - INTERVAL '30 days';
    
    RAISE NOTICE 'Cleaned benchmarks older than 30 days';
END;
$$;


ALTER FUNCTION "public"."cleanup_old_benchmarks"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_stuck_jobs"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    UPDATE public.agent_jobs SET 
        status = 'pending', assigned_device_id = NULL,
        started_at = NULL, updated_at = NOW()
    WHERE status = 'running' AND started_at < NOW() - INTERVAL '2 hours';
END;
$$;


ALTER FUNCTION "public"."cleanup_stuck_jobs"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cleanup_stuck_jobs"("p_max_retries" integer DEFAULT 3, "p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
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
$$;


ALTER FUNCTION "public"."cleanup_stuck_jobs"("p_max_retries" integer, "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint DEFAULT NULL::bigint, "p_result" "jsonb" DEFAULT NULL::"jsonb", "p_device_id" "uuid" DEFAULT NULL::"uuid", "p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_updated_rows INTEGER;
  v_job_org_id uuid;
  v_job_agent uuid;
  v_job_status text;
  v_lease_valid BOOLEAN;
BEGIN
  SELECT org_id, agent_id, status INTO v_job_org_id, v_job_agent, v_job_status
  FROM public.agent_jobs
  WHERE id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'job not found';
  END IF;

  IF v_job_status = 'completed' THEN
    RAISE EXCEPTION 'job already in terminal state completed - immutable';
  END IF;

  IF v_job_status = 'dead' THEN
    RAISE EXCEPTION 'job already in terminal state dead - immutable';
  END IF;

  IF p_device_id IS NULL OR p_org_id IS NULL THEN
    RAISE EXCEPTION 'device_id and org_id are required';
  END IF;

  IF v_job_org_id != p_org_id THEN
    RAISE EXCEPTION 'device ownership violation - org_id mismatch';
  END IF;

  IF v_job_agent != p_device_id THEN
    RAISE EXCEPTION 'device ownership violation - job assigned to different device';
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM leases l
    WHERE l.job_id = p_job_id
      AND l.device_id = p_device_id
      AND l.status = 'active'
      AND l.lease_expires_at > NOW() - interval '2 seconds'
  ) INTO v_lease_valid;

  IF NOT v_lease_valid THEN
    RAISE EXCEPTION 'no valid active lease - job may have been re-claimed';
  END IF;

  IF p_status = 'completed' THEN
    IF v_job_status NOT IN ('pending', 'assigned', 'running') THEN
      RAISE EXCEPTION 'state transition failed - invalid transition from % to completed', v_job_status;
    END IF;

    UPDATE public.agent_jobs
    SET status = 'completed',
        completed = true,
        finished_at = NOW(),
        duration_ms = COALESCE(duration_ms, p_duration_ms),
        updated_at = NOW(),
        last_transition_at = NOW()
    WHERE id = p_job_id
      AND status IN ('pending', 'assigned', 'running')
    RETURNING 1 INTO v_updated_rows;

    IF v_updated_rows IS NULL OR v_updated_rows = 0 THEN
      RAISE EXCEPTION 'state transition failed - concurrent modification detected';
    END IF;

    DELETE FROM leases
    WHERE job_id = p_job_id
      AND device_id = p_device_id
      AND status = 'active';

    RETURN TRUE;

  ELSIF p_status = 'failed' THEN
    IF v_job_status NOT IN ('pending', 'assigned', 'running') THEN
      RAISE EXCEPTION 'state transition failed - invalid transition from % to failed', v_job_status;
    END IF;

    UPDATE public.agent_jobs
    SET status = 'failed',
        completed = false,
        finished_at = NOW(),
        error = p_result::text,
        updated_at = NOW(),
        last_transition_at = NOW()
    WHERE id = p_job_id
      AND status IN ('pending', 'assigned', 'running')
    RETURNING 1 INTO v_updated_rows;

    IF v_updated_rows IS NULL OR v_updated_rows = 0 THEN
      RAISE EXCEPTION 'state transition failed - concurrent modification detected';
    END IF;

    DELETE FROM leases
    WHERE job_id = p_job_id
      AND device_id = p_device_id
      AND status = 'active';

    RETURN TRUE;
  ELSE
    RAISE EXCEPTION 'invalid status: %', p_status;
  END IF;
END;
$$;


ALTER FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint, "p_result" "jsonb", "p_device_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint, "p_result" "jsonb", "p_device_id" "uuid", "p_org_id" "uuid") IS 'Idempotent job completion with lease validation';



CREATE OR REPLACE FUNCTION "public"."compute_agent_job_hashes"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    -- Only compute if runtime_type changed
    IF NEW.runtime_type IS NOT NULL AND OLD.runtime_type IS DISTINCT FROM NEW.runtime_type THEN
        NEW.dependency_lock_hash := encode(digest(
            COALESCE(NEW.runtime_type, 'native') || '|' || COALESCE(NEW.runtime_dependencies::text, ''),
            'sha256'
        ), 'hex');
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."compute_agent_job_hashes"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."compute_dependency_lock_hash"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_lock_hash TEXT;
    v_deps JSONB;
BEGIN
    -- Compute dependency lock hash from runtime_dependencies
    -- This creates a deterministic hash based on actual dependencies
    v_deps := COALESCE(NEW.runtime_dependencies, '{}'::jsonb);
    
    -- Create deterministic hash from sorted dependencies
    v_lock_hash := encode(
        digest(
            COALESCE(NEW.runtime_type, 'native') || '|' ||
            COALESCE(NEW.runtime_version, '') || '|' ||
            COALESCE(v_deps::text, ''),
            'sha256'
        ),
        'hex'
    );
    
    NEW.dependency_lock_hash := v_lock_hash;
    
    -- Generate idempotency key if not provided
    IF NEW.idempotency_key IS NULL THEN
        NEW.idempotency_key := gen_random_uuid()::text;
    END IF;
    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."compute_dependency_lock_hash"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."compute_job_lock_hash"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_lock_hash TEXT;
    v_deps JSONB;
BEGIN
    v_deps := COALESCE(NEW.runtime_dependencies, '{}'::jsonb);
    
    v_lock_hash := encode(digest(
        COALESCE(NEW.runtime_type, 'native') || '|' ||
        COALESCE(NEW.runtime_version, '') || '|' ||
        COALESCE(v_deps::text, ''),
        'sha256'
    ), 'hex');
    
    NEW.dependency_lock_hash := v_lock_hash;
    
    IF NEW.idempotency_key IS NULL THEN
        NEW.idempotency_key := gen_random_uuid()::text;
    END IF;
    
    IF NEW.run_id IS NULL THEN
        NEW.run_id := gen_random_uuid();
    END IF;
    
    IF NEW.attempt_number IS NULL THEN
        NEW.attempt_number := 1;
    END IF;
    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."compute_job_lock_hash"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."compute_plugin_checksum"("p_storage_path" "text") RETURNS "text"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_checksum TEXT;
BEGIN
    -- This requires enabling http extension and storage
    -- For now, create function that can be called after upload
    -- The actual implementation will be in the edge function
    RETURN NULL;
END;
$$;


ALTER FUNCTION "public"."compute_plugin_checksum"("p_storage_path" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."consolidated_dispatch"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_edge_url TEXT;
  v_cron_secret TEXT;
BEGIN
  v_edge_url := public.get_edge_url();
  IF v_edge_url IS NULL THEN RETURN; END IF;

  SELECT value INTO v_cron_secret
  FROM public.system_config WHERE key = 'cron_secret';

  PERFORM net.http_post(
    url     := v_edge_url || '/functions/v1/dispatch_http_jobs',
    headers := jsonb_build_object(
      'Content-Type',  'application/json',
      'x-cron-secret', COALESCE(v_cron_secret, '')
    ),
    body := jsonb_build_object('source', 'cron_consolidated_dispatch')
  );

  INSERT INTO public.system_logs(event_type, message, created_at)
  VALUES('cron_dispatch', 'Consolidated dispatch completed', now());
END;
$$;


ALTER FUNCTION "public"."consolidated_dispatch"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_chunks_from_plan_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_dataset_id uuid;
  v_dataset record;
  v_chunk_size_gb integer := 2;
  v_num_chunks integer;
  v_chunk_vector vector(16);
  v_i integer;
  v_planned_chunks integer;
BEGIN
  -- Only trigger when plan_chunks job completes successfully
  IF NEW.status = 'completed' AND OLD.status IS DISTINCT FROM 'completed' AND NEW.job_type = 'plan_chunks' THEN

    -- Get dataset_id from payload
    v_dataset_id := (NEW.payload ->> 'dataset_id')::uuid;

    IF v_dataset_id IS NULL THEN
      RAISE WARNING 'plan_chunks job % has no dataset_id in payload', NEW.id;
      RETURN NEW;
    END IF;

    -- Get dataset info
    SELECT * INTO v_dataset FROM public.datasets WHERE id = v_dataset_id;

    IF NOT FOUND THEN
      RAISE WARNING 'Dataset % not found for plan_chunks job %', v_dataset_id, NEW.id;
      RETURN NEW;
    END IF;

    -- Check if chunks already exist
    SELECT COUNT(*) INTO v_planned_chunks FROM public.batch_chunks WHERE dataset_id = v_dataset_id;

    IF v_planned_chunks > 0 THEN
      RAISE NOTICE 'Dataset % already has % chunks, skipping chunk creation', v_dataset_id, v_planned_chunks;
      RETURN NEW;
    END IF;

    -- Calculate number of chunks
    v_num_chunks := GREATEST(1, CEIL(COALESCE(v_dataset.total_size_gb, 1) / v_chunk_size_gb));

    -- Create chunks
    FOR v_i IN 0..(v_num_chunks - 1) LOOP
      -- Generate chunk vector based on index
      v_chunk_vector := (
        SELECT ('{' ||
          (v_i::float / GREATEST(v_num_chunks, 1))::text || ',' ||
          (v_chunk_size_gb::float / 100)::text || ',' ||
          (COALESCE(v_dataset.total_size_gb, 1)::float / 1000)::text || ',' ||
          CASE WHEN v_num_chunks > 50 THEN '1' ELSE '0' END || ',' ||
          CASE WHEN v_chunk_size_gb > 10 THEN '1' ELSE '0' END || ',' ||
          '0,0,0,0,0,0,0,0,0,0,0'
        || '}')::vector(16)
      );

      INSERT INTO public.batch_chunks (
        dataset_id,
        org_id,
        chunk_index,
        status,
        job_type,
        chunk_size_gb,
        chunk_vector,
        payload,
        created_at,
        updated_at
      ) VALUES (
        v_dataset_id,
        v_dataset.org_id,
        v_i,
        'pending',
        'process',
        v_chunk_size_gb,
        v_chunk_vector,
        jsonb_build_object(
          'dataset_id', v_dataset_id,
          'chunk_index', v_i,
          'total_chunks', v_num_chunks
        ),
        NOW(),
        NOW()
      );
    END LOOP;

    RAISE NOTICE 'Created % chunks for dataset %', v_num_chunks, v_dataset_id;

    -- Notify that chunks are ready
    PERFORM pg_notify(
      'chunks_created',
      json_build_object(
        'dataset_id', v_dataset_id,
        'org_id', v_dataset.org_id,
        'chunk_count', v_num_chunks
      )::text
    );

  END IF;

  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."create_chunks_from_plan_job"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_org_with_owner"("org_name" "text", "team_size" integer, "member_name" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
declare
  new_org_id uuid;
begin
  -- Safety: user must be authenticated
  if auth.uid() is null then
    raise exception 'Not authenticated';
  end if;

  -- 1. Create org
  insert into orgs (name, team_size)
  values (org_name, team_size)
  returning id into new_org_id;

  -- 2. Attach user as admin
  insert into org_members (
    org_id,
    user_id,
    role,
    member_name
  )
  values (
    new_org_id,
    auth.uid(),
    'admin',
    member_name
  );

  return new_org_id;
end;
$$;


ALTER FUNCTION "public"."create_org_with_owner"("org_name" "text", "team_size" integer, "member_name" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_scan_job_on_dataset_insert"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.status = 'registered' THEN
        INSERT INTO public.agent_jobs (
            org_id,
            job_type,
            status,
            payload,
            created_at,
            updated_at,
            last_transition_at
        ) VALUES (
            NEW.org_id,
            'scan_dataset',
            'pending',
            jsonb_build_object(
                'dataset_id', NEW.id,
                'source_path', NEW.source_path,
                'storage_type', NEW.storage_type,
                'file_type', NEW.file_type
            ),
            NOW(),
            NOW(),
            NOW()
        );
        
        RAISE NOTICE 'Created scan_dataset job for dataset %', NEW.id;
    END IF;    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."create_scan_job_on_dataset_insert"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."decrypt_vault_secret"("secret_name" "text") RETURNS "text"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    decrypted_value TEXT;
BEGIN
    SELECT decrypted_secret INTO decrypted_value
    FROM vault.decrypted_secrets
    WHERE name = secret_name
    LIMIT 1;

    RETURN decrypted_value;
END;
$$;


ALTER FUNCTION "public"."decrypt_vault_secret"("secret_name" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."device_has_warm_environment"("p_device_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_lock_hash" "text") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_has_warm BOOLEAN := false;
    v_cache JSONB;
BEGIN
    SELECT d.runtime_cache INTO v_cache
    FROM public.devices d
    WHERE d.id = p_device_id;
    
    IF v_cache IS NOT NULL THEN
        SELECT EXISTS(
            SELECT 1 FROM jsonb_array_elements(v_cache) e
            WHERE e->>'runtime_type' = p_runtime_type
            AND e->>'runtime_version' = p_runtime_version
            AND e->>'dependency_lock_hash' = p_dependency_lock_hash
            AND (NOW() - (e->>'last_used_at')::TIMESTAMPTZ) < INTERVAL '30 minutes'
        ) INTO v_has_warm;
    END IF;
    
    RETURN v_has_warm;
END;
$$;


ALTER FUNCTION "public"."device_has_warm_environment"("p_device_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_lock_hash" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."device_matches_requirements"("p_device_id" "uuid", "p_runtime_type" "text", "p_min_python_version" "text", "p_required_arch" "text", "p_required_os" "text") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_device RECORD;
BEGIN
    SELECT * INTO v_device FROM devices WHERE id = p_device_id;
    
    IF p_required_arch IS NOT NULL AND v_device.arch != p_required_arch THEN
        RETURN false;
    END IF;
    
    IF p_required_os IS NOT NULL AND v_device.os != p_required_os THEN
        RETURN false;
    END IF;
    
    RETURN true;
END;
$$;


ALTER FUNCTION "public"."device_matches_requirements"("p_device_id" "uuid", "p_runtime_type" "text", "p_min_python_version" "text", "p_required_arch" "text", "p_required_os" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."device_supports_execution_mode"("p_device_id" "uuid", "p_mode" "text") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_docker_available BOOLEAN;
BEGIN
    SELECT docker_available INTO v_docker_available FROM devices WHERE id = p_device_id;
    
    CASE p_mode
        WHEN 'docker' THEN RETURN v_docker_available = true;
        WHEN 'runtime' THEN RETURN true;
        WHEN 'native' THEN RETURN true;
        ELSE RETURN true;
    END CASE;
END;
$$;


ALTER FUNCTION "public"."device_supports_execution_mode"("p_device_id" "uuid", "p_mode" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."device_supports_runtime"("p_device_id" "uuid", "p_runtime_type" "text") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_supported JSONB;
BEGIN
    SELECT runtime_supported INTO v_supported FROM devices WHERE id = p_device_id;
    IF v_supported IS NULL THEN
        RETURN p_runtime_type = 'native';
    END IF;
    RETURN v_supported ? p_runtime_type OR p_runtime_type = 'native';
END;
$$;


ALTER FUNCTION "public"."device_supports_runtime"("p_device_id" "uuid", "p_runtime_type" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."dispatch_http_jobs_secure"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.system_logs (level, source, message, created_at)
    VALUES ('info', 'cron', 'HTTP jobs dispatch ran', NOW());
END;
$$;


ALTER FUNCTION "public"."dispatch_http_jobs_secure"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."elect_merge_device"("_org_id" "uuid", "_affinity_device_id" "uuid", "_preferred_network_zone" "text") RETURNS "uuid"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    result_device uuid;
BEGIN

    --------------------------------------------------
    -- STRATEGY A: Affinity Device
    --------------------------------------------------
    IF _affinity_device_id IS NOT NULL THEN
        SELECT id
        INTO result_device
        FROM public.devices
        WHERE id = _affinity_device_id
          AND org_id = _org_id
          AND status = 'online'
          AND merge_capable = true;

        IF FOUND THEN
            RETURN result_device;
        ELSE
            RAISE EXCEPTION 'affinity_device_offline'
                USING HINT = _affinity_device_id::text;
        END IF;
    END IF;

    --------------------------------------------------
    -- STRATEGY B: Fastest Online Device
    --------------------------------------------------
    SELECT id
    INTO result_device
    FROM public.devices
    WHERE org_id = _org_id
      AND status = 'online'
      AND merge_capable = true
    ORDER BY
        CASE
            WHEN _preferred_network_zone IS NOT NULL
                 AND network_zone = _preferred_network_zone
            THEN 1 ELSE 0
        END DESC,
        io_bandwidth_mb_s DESC NULLS LAST
    LIMIT 1;

    IF result_device IS NULL THEN
        RAISE EXCEPTION 'no_merge_device_available';
    END IF;

    RETURN result_device;

END;
$$;


ALTER FUNCTION "public"."elect_merge_device"("_org_id" "uuid", "_affinity_device_id" "uuid", "_preferred_network_zone" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."encode_plugin_signature"("sig" "bytea") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    AS $$
    SELECT encode(sig, 'base64')::text;
$$;


ALTER FUNCTION "public"."encode_plugin_signature"("sig" "bytea") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."enqueue_device_online_event"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.status = 'online' AND OLD.status != 'online' THEN
        INSERT INTO public.job_notification_queue (job_id, event_type, payload, org_id)
        VALUES (
            NULL,
            'device_online',
            jsonb_build_object(
                'device_id', NEW.id,
                'org_id', NEW.org_id,
                'online_at', NOW()
            ),
            NEW.org_id
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."enqueue_device_online_event"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."evaluate_alert_rules"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.system_logs (level, source, message, created_at)
    VALUES ('info', 'cron', 'Alert rules evaluated', NOW());
END;
$$;


ALTER FUNCTION "public"."evaluate_alert_rules"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_job uuid := p_job_id::uuid;
  v_agent uuid := p_agent_id::uuid;
  updated_rows INT;
BEGIN
  UPDATE public.agent_jobs
  SET
    agent_id = v_agent,
    status = 'assigned',
    assigned_at = now()
  WHERE
    id = v_job
    AND status IN ('pending', 'queued')
  RETURNING 1 INTO updated_rows;

  IF updated_rows IS NULL THEN
    RETURN FALSE;
  END IF;

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") IS 'DEPRECATED: Use claim_jobs_for_device or claim_next_job_for_device instead. This function lacks proper org_id validation.';



CREATE OR REPLACE FUNCTION "public"."force_assign_job"("p_job_id" "uuid", "p_device_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_updated_rows INTEGER;
BEGIN
  UPDATE public.agent_jobs
  SET agent_id = p_device_id,
      status = 'assigned',
      assigned_at = NOW(),
      updated_at = NOW(),
      lease_expires_at = NOW() + INTERVAL '30 minutes'
  WHERE id = p_job_id
    AND status = 'pending'
  RETURNING 1 INTO v_updated_rows;

  IF v_updated_rows = 0 THEN
    RETURN FALSE;
  END IF;

  INSERT INTO public.job_notification_queue (job_id, agent_id)
  VALUES (p_job_id, p_device_id);

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."force_assign_job"("p_job_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job uuid := p_job_id::uuid;
  v_agent uuid := p_agent_id::uuid;
  v_job_org uuid;
  v_agent_org uuid;
  updated_rows INT;
BEGIN
  IF p_org_id IS NULL THEN
    p_org_id := public.safe_cast_uuid(nullif(current_setting('app.current_org', true), ''));
  END IF;

  IF p_org_id IS NULL THEN
    RAISE EXCEPTION 'org_id is required for force_assign_job';
  END IF;

  SELECT org_id INTO v_job_org FROM public.agent_jobs WHERE id = v_job;
  IF v_job_org IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION 'job org_id mismatch - access denied';
  END IF;

  SELECT org_id INTO v_agent_org FROM public.devices WHERE id = v_agent;
  IF v_agent_org IS DISTINCT FROM p_org_id THEN
    RAISE EXCEPTION 'agent org_id mismatch - access denied';
  END IF;

  UPDATE public.agent_jobs
  SET agent_id = v_agent, status = 'assigned', assigned_at = now(), updated_at = now()
  WHERE id = v_job AND status IN ('pending', 'queued') AND org_id = p_org_id
  RETURNING 1 INTO updated_rows;

  RETURN COALESCE(updated_rows, 0) > 0;
END;
$$;


ALTER FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_advisory_lock_key"("p_uuid" "uuid") RETURNS bigint
    LANGUAGE "plpgsql" IMMUTABLE
    AS $$
BEGIN
  RETURN hashtextextended(p_uuid::text, 0);
END;
$$;


ALTER FUNCTION "public"."get_advisory_lock_key"("p_uuid" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_agent_job_stats"("p_agent_id" "uuid") RETURNS TABLE("total_jobs" bigint, "completed_jobs" bigint, "failed_jobs" bigint, "running_jobs" bigint, "success_rate" numeric, "avg_duration_ms" numeric, "avg_jobs_per_day" numeric, "last_job_at" timestamp with time zone)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    WITH job_stats AS (
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'completed') as completed,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COUNT(*) FILTER (WHERE status IN ('running', 'assigned', 'pending')) as running,
            AVG(duration_ms) as avg_duration,
            MAX(created_at) as last_job
        FROM agent_jobs
        WHERE agent_id = p_agent_id
    ),
    daily_jobs AS (
        SELECT COUNT(*) as jobs_per_day
        FROM agent_jobs
        WHERE agent_id = p_agent_id
          AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(created_at)
    )
    SELECT 
        COALESCE(js.total, 0)::bigint as total_jobs,
        COALESCE(js.completed, 0)::bigint as completed_jobs,
        COALESCE(js.failed, 0)::bigint as failed_jobs,
        COALESCE(js.running, 0)::bigint as running_jobs,
        CASE 
            WHEN js.completed + js.failed > 0 
            THEN ROUND((js.completed::numeric / (js.completed + js.failed)) * 100, 1)
            ELSE 100.0
        END as success_rate,
        COALESCE(js.avg_duration, 0)::numeric as avg_duration_ms,
        COALESCE((SELECT AVG(jobs_per_day) FROM daily_jobs), 0)::numeric as avg_jobs_per_day,
        js.last_job as last_job_at
    FROM job_stats js;
END;
$$;


ALTER FUNCTION "public"."get_agent_job_stats"("p_agent_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_agent_metrics_aggregate"("p_agent_id" "uuid", "p_time_range" "text" DEFAULT '24h'::"text", "p_interval" "text" DEFAULT 'hour'::"text") RETURNS TABLE("period" timestamp with time zone, "avg_cpu" numeric, "avg_memory_free_gb" numeric, "max_cpu" numeric, "min_memory_free_gb" numeric, "metric_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_start_time timestamp with time zone;
BEGIN
    v_start_time := CASE p_time_range
        WHEN '1h' THEN NOW() - INTERVAL '1 hour'
        WHEN '6h' THEN NOW() - INTERVAL '6 hours'
        WHEN '24h' THEN NOW() - INTERVAL '24 hours'
        WHEN '7d' THEN NOW() - INTERVAL '7 days'
        WHEN '30d' THEN NOW() - INTERVAL '30 days'
        ELSE NOW() - INTERVAL '24 hours'
    END;

    RETURN QUERY
    SELECT 
        date_trunc(p_interval, am.created_at) as period,
        AVG(am.metric_value) FILTER (WHERE am.metric_name = 'cpu_usage_percent') as avg_cpu,
        AVG(am.metric_value) FILTER (WHERE am.metric_name = 'memory_free_gb') as avg_memory_free_gb,
        MAX(am.metric_value) FILTER (WHERE am.metric_name = 'cpu_usage_percent') as max_cpu,
        MIN(am.metric_value) FILTER (WHERE am.metric_name = 'memory_free_gb') as min_memory_free_gb,
        COUNT(*) as metric_count
    FROM agent_metrics am
    WHERE am.device_id = p_agent_id
      AND am.created_at >= v_start_time
    GROUP BY date_trunc(p_interval, am.created_at)
    ORDER BY period DESC;
END;
$$;


ALTER FUNCTION "public"."get_agent_metrics_aggregate"("p_agent_id" "uuid", "p_time_range" "text", "p_interval" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_alert_rules"("p_org_id" "uuid") RETURNS TABLE("id" "uuid", "name" "text", "condition_type" "text", "threshold_value" numeric, "threshold_window_minutes" integer, "channel" "text", "enabled" boolean, "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    ar.id, ar.name, ar.condition_type, ar.threshold_value,
    ar.threshold_window_minutes, ar.channel, ar.enabled, ar.created_at
  FROM alert_rules ar
  WHERE ar.org_id = p_org_id
  ORDER BY ar.created_at DESC;
END;
$$;


ALTER FUNCTION "public"."get_alert_rules"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_audit_event_types"() RETURNS "text"[]
    LANGUAGE "sql" STABLE
    AS $$
    SELECT ARRAY['job_assigned','job_completed','job_failed','job_reclaimed','device_online','device_offline',
        'device_heartbeat','token_rotated','token_rotation_failed','plugin_loaded','plugin_execution',
        'pipeline_started','pipeline_completed','pipeline_failed','merge_started','merge_completed','merge_failed',
        'offline_device_lease_cleanup','device_offline_lease_cleanup'];
$$;


ALTER FUNCTION "public"."get_audit_event_types"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_constraints_report"() RETURNS TABLE("constraint_name" "text", "constraint_definition" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT 
    conname::text,
    pg_get_constraintdef(oid)::text
  FROM pg_constraint
  WHERE conrelid = 'datasets'::regclass
  AND contype = 'c'
  ORDER BY conname;
END;
$$;


ALTER FUNCTION "public"."get_constraints_report"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_current_org_id"() RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_org_id uuid;
BEGIN
  SELECT om.org_id INTO v_org_id
  FROM org_members om
  WHERE om.user_id = auth.uid()
  LIMIT 1;
  RETURN v_org_id;
END;
$$;


ALTER FUNCTION "public"."get_current_org_id"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_dashboard_stats"("p_org_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("total_jobs" bigint, "running_jobs" bigint, "completed_jobs" bigint, "failed_jobs" bigint, "pending_jobs" bigint, "total_datasets" bigint, "active_datasets" bigint, "total_devices" bigint, "online_devices" bigint, "busy_devices" bigint, "total_executions" bigint, "running_executions" bigint, "completed_executions" bigint, "failed_executions" bigint)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::bigint FROM agent_jobs WHERE p_org_id IS NULL OR org_id = p_org_id)::bigint AS total_jobs,
    (SELECT COUNT(*)::bigint FROM agent_jobs WHERE status = 'running' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS running_jobs,
    (SELECT COUNT(*)::bigint FROM agent_jobs WHERE status = 'completed' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS completed_jobs,
    (SELECT COUNT(*)::bigint FROM agent_jobs WHERE status = 'failed' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS failed_jobs,
    (SELECT COUNT(*)::bigint FROM agent_jobs WHERE status = 'pending' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS pending_jobs,
    (SELECT COUNT(*)::bigint FROM datasets WHERE p_org_id IS NULL OR org_id = p_org_id)::bigint AS total_datasets,
    (SELECT COUNT(*)::bigint FROM datasets WHERE status IN ('scanned', 'processing', 'merged') AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS active_datasets,
    (SELECT COUNT(*)::bigint FROM devices WHERE p_org_id IS NULL OR org_id = p_org_id)::bigint AS total_devices,
    (SELECT COUNT(*)::bigint FROM devices WHERE status = 'online' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS online_devices,
    (SELECT COUNT(*)::bigint FROM devices WHERE active_workers > 0 AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS busy_devices,
    (SELECT COUNT(*)::bigint FROM executions WHERE p_org_id IS NULL OR org_id = p_org_id)::bigint AS total_executions,
    (SELECT COUNT(*)::bigint FROM executions WHERE status = 'running' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS running_executions,
    (SELECT COUNT(*)::bigint FROM executions WHERE status = 'completed' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS completed_executions,
    (SELECT COUNT(*)::bigint FROM executions WHERE status = 'failed' AND (p_org_id IS NULL OR org_id = p_org_id))::bigint AS failed_executions;
END;
$$;


ALTER FUNCTION "public"."get_dashboard_stats"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_dashboard_summary"() RETURNS TABLE("total_jobs" bigint, "running_jobs" bigint, "completed_jobs" bigint, "failed_jobs" bigint, "queued_jobs" bigint, "success_rate" numeric, "total_datasets" bigint, "total_storage_gb" numeric, "total_files" bigint, "total_agents" bigint, "online_agents" bigint, "busy_agents" bigint, "gpu_agents" bigint, "avg_benchmark_score" numeric)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    WITH job_stats AS (
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status IN ('running', 'assigned')) as running,
            COUNT(*) FILTER (WHERE status = 'completed') as completed,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COUNT(*) FILTER (WHERE status IN ('pending', 'queued')) as queued
        FROM agent_jobs
    ),
    dataset_stats AS (
        SELECT 
            COUNT(*) as total,
            COALESCE(SUM(total_size_gb), 0) as storage_gb,
            COALESCE(SUM(file_count), 0) as files
        FROM datasets
    ),
    device_stats AS (
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'online') as online,
            COUNT(*) FILTER (WHERE status = 'online' AND active_workers > 0) as busy,
            COUNT(*) FILTER (WHERE gpu_available = true) as gpu,
            COALESCE(AVG(benchmark_score), 0) as avg_score
        FROM devices
    )
    SELECT 
        COALESCE(js.total, 0)::bigint as total_jobs,
        COALESCE(js.running, 0)::bigint as running_jobs,
        COALESCE(js.completed, 0)::bigint as completed_jobs,
        COALESCE(js.failed, 0)::bigint as failed_jobs,
        COALESCE(js.queued, 0)::bigint as queued_jobs,
        CASE 
            WHEN js.completed + js.failed > 0 
            THEN ROUND((js.completed::numeric / (js.completed + js.failed)) * 100, 1)
            ELSE 100.0
        END as success_rate,
        COALESCE(ds.total, 0)::bigint as total_datasets,
        COALESCE(ds.storage_gb, 0)::numeric as total_storage_gb,
        COALESCE(ds.files, 0)::bigint as total_files,
        COALESCE(dv.total, 0)::bigint as total_agents,
        COALESCE(dv.online, 0)::bigint as online_agents,
        COALESCE(dv.busy, 0)::bigint as busy_agents,
        COALESCE(dv.gpu, 0)::bigint as gpu_agents,
        COALESCE(dv.avg_score, 0)::numeric as avg_benchmark_score
    FROM job_stats js, dataset_stats ds, device_stats dv;
END;
$$;


ALTER FUNCTION "public"."get_dashboard_summary"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_dataset_executions"("p_dataset_id" "uuid", "p_limit" integer DEFAULT 20) RETURNS TABLE("id" "uuid", "status" "text", "created_at" timestamp with time zone, "completed_at" timestamp with time zone, "current_step_index" integer, "total_steps" integer)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT e.id, e.status, e.created_at, e.completed_at, e.current_step_index, e.total_steps
  FROM executions e
  WHERE e.dataset_id = p_dataset_id
  ORDER BY e.created_at DESC
  LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."get_dataset_executions"("p_dataset_id" "uuid", "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_device_by_id"("p_device_id" "uuid") RETURNS TABLE("id" "uuid", "name" "text", "status" "text", "environment_type" "text", "storage_type" "text", "os" "text", "arch" "text", "cpu_cores_free" integer, "total_cpu_cores" integer, "memory_free_gb" numeric, "total_memory_gb" numeric, "gpu_available" boolean, "gpu_model" "text", "benchmark_score" numeric, "max_concurrency" integer, "active_workers" integer, "last_heartbeat" timestamp with time zone, "capabilities" "text"[], "runtime_supported" "jsonb")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    d.id, d.name, d.status, d.environment_type, d.storage_type,
    d.os, d.arch, d.cpu_cores_free, d.total_cpu_cores,
    d.memory_free_gb, d.total_memory_gb, d.gpu_available,
    d.gpu_model, d.benchmark_score, d.max_concurrency,
    d.active_workers, d.last_heartbeat, d.capabilities,
    d.runtime_supported
  FROM devices d
  WHERE d.id = p_device_id;
END;
$$;


ALTER FUNCTION "public"."get_device_by_id"("p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_device_job_history"("p_device_id" "uuid", "p_limit" integer DEFAULT 20) RETURNS TABLE("job_id" "uuid", "job_type" "text", "status" "text", "created_at" timestamp with time zone, "started_at" timestamp with time zone, "finished_at" timestamp with time zone, "duration_ms" bigint, "error" "text")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    aj.id AS job_id,
    aj.job_type,
    aj.status,
    aj.created_at,
    aj.started_at,
    aj.finished_at,
    aj.duration_ms,
    aj.error
  FROM agent_jobs aj
  WHERE aj.agent_id = p_device_id
  ORDER BY aj.created_at DESC
  LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."get_device_job_history"("p_device_id" "uuid", "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_device_job_stats"("p_device_id" "uuid", "p_job_type" "text", "p_window_hours" integer DEFAULT 24) RETURNS TABLE("avg_duration_ms" numeric, "avg_throughput" numeric, "success_rate" numeric, "job_count" bigint)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE(AVG(duration_ms), 0), COALESCE(AVG(throughput), 0),
        CASE WHEN COUNT(*) > 0 THEN COUNT(*) FILTER (WHERE success)::numeric / COUNT(*)::numeric * 100 ELSE 100 END,
        COUNT(*)::bigint
    FROM device_job_performance
    WHERE device_id = p_device_id AND job_type = p_job_type
      AND created_at > NOW() - (p_window_hours || ' hours')::interval;
END;
$$;


ALTER FUNCTION "public"."get_device_job_stats"("p_device_id" "uuid", "p_job_type" "text", "p_window_hours" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_device_rankings"("org_id" "uuid", "job_type" "text", "chunk_vector" "public"."vector") RETURNS TABLE("id" "uuid", "name" "text", "capabilities" "jsonb", "similarity" double precision)
    LANGUAGE "sql" STABLE
    AS $$
  SELECT d.id,
         d.name,
         d.specs->'capabilities' AS capabilities,
         (dv.profile_vector <#> get_device_rankings.chunk_vector) AS similarity
  FROM devices d
  JOIN device_vectors dv ON d.id = dv.device_id
  WHERE d.org_id = get_device_rankings.org_id
    AND d.status = 'online'
    AND d.specs->'capabilities' ? get_device_rankings.job_type
  ORDER BY similarity DESC;
$$;


ALTER FUNCTION "public"."get_device_rankings"("org_id" "uuid", "job_type" "text", "chunk_vector" "public"."vector") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_device_runtime_cache_summary"("p_device_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_result JSONB := '{}'::jsonb;
    v_device RECORD;
BEGIN
    SELECT d.runtime_cache INTO v_device
    FROM public.devices d
    WHERE d.id = p_device_id;
    
    IF v_device.runtime_cache IS NOT NULL THEN
        v_result := v_device.runtime_cache;
    END IF;
    
    RETURN v_result;
END;
$$;


ALTER FUNCTION "public"."get_device_runtime_cache_summary"("p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_edge_url"() RETURNS "text"
    LANGUAGE "plpgsql" STABLE
    AS $$
DECLARE
  v_url text;
BEGIN
  SELECT value INTO v_url
  FROM public.system_config
  WHERE key = 'edge_url';

  IF v_url IS NULL THEN
    RAISE WARNING 'edge_url not configured in system_config';
  END IF;

  RETURN v_url;
END;
$$;


ALTER FUNCTION "public"."get_edge_url"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_execution_detail"("p_execution_id" "uuid") RETURNS TABLE("execution_id" "uuid", "execution_status" "text", "execution_created_at" timestamp with time zone, "execution_completed_at" timestamp with time zone, "dataset_id" "uuid", "dataset_name" "text", "current_step_index" integer, "total_steps" integer, "step_id" "uuid", "step_index" integer, "step_status" "text", "step_type" "text", "step_error" "text", "step_completed_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    e.id AS execution_id,
    e.status AS execution_status,
    e.created_at AS execution_created_at,
    e.completed_at AS execution_completed_at,
    e.dataset_id,
    d.name AS dataset_name,
    e.current_step_index,
    e.total_steps,
    es.id AS step_id,
    es.step_index,
    es.status AS step_status,
    es.step_type,
    es.error AS step_error,
    es.completed_at AS step_completed_at
  FROM executions e
  LEFT JOIN datasets d ON d.id = e.dataset_id
  LEFT JOIN execution_steps es ON es.execution_id = e.id
  WHERE e.id = p_execution_id
  ORDER BY es.step_index;
END;
$$;


ALTER FUNCTION "public"."get_execution_detail"("p_execution_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_fleet_health"() RETURNS TABLE("id" "uuid", "name" "text", "status" "text", "health_score" numeric, "cpu_usage" numeric, "memory_usage_gb" numeric, "memory_total_gb" numeric, "active_workers" integer, "max_concurrency" integer, "last_heartbeat" timestamp with time zone, "gpu_available" boolean, "environment_type" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        d.id,
        d.name,
        d.status,
        CASE 
            WHEN d.status = 'offline' THEN 0
            WHEN d.last_heartbeat < NOW() - INTERVAL '5 minutes' THEN 25
            WHEN d.cpu_usage_percent > 90 THEN 50
            WHEN d.active_workers >= d.max_concurrency THEN 75
            ELSE 100
        END as health_score,
        COALESCE(d.cpu_usage_percent, 0)::numeric,
        COALESCE(d.total_memory_gb - d.memory_free_gb, 0)::numeric,
        COALESCE(d.total_memory_gb, 0)::numeric,
        COALESCE(d.active_workers, 0)::integer,
        COALESCE(d.max_concurrency, 0)::integer,
        d.last_heartbeat,
        d.gpu_available,
        d.environment_type
    FROM devices d
    ORDER BY d.last_heartbeat DESC NULLS LAST;
END;
$$;


ALTER FUNCTION "public"."get_fleet_health"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_functions_report"() RETURNS TABLE("routine_name" "text", "routine_type" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT 
    r.routine_name::text,
    r.routine_type::text
  FROM information_schema.routines r
  WHERE r.routine_schema = 'public'
  AND r.routine_name LIKE '%chunk%'
  ORDER BY r.routine_name;
END;
$$;


ALTER FUNCTION "public"."get_functions_report"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_job_checkpoints"("p_job_id" "uuid") RETURNS TABLE("id" "uuid", "step_index" integer, "checkpoint_data" "jsonb", "progress_percent" numeric, "checkpointed_at" timestamp with time zone, "is_completed" boolean)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    jc.id, jc.step_index, jc.checkpoint_data,
    jc.progress_percent, jc.checkpointed_at, jc.is_completed
  FROM job_checkpoints jc
  WHERE jc.job_id = p_job_id
  ORDER BY jc.step_index;
END;
$$;


ALTER FUNCTION "public"."get_job_checkpoints"("p_job_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_job_detail"("p_job_id" "uuid") RETURNS TABLE("job_id" "uuid", "job_type" "text", "job_status" "text", "job_created_at" timestamp with time zone, "job_started_at" timestamp with time zone, "job_finished_at" timestamp with time zone, "job_duration_ms" bigint, "job_error" "text", "job_retry_count" integer, "agent_id" "uuid", "agent_name" "text", "execution_id" "uuid", "execution_status" "text", "dataset_id" "uuid", "dataset_name" "text", "plugin_id" "text", "plugin_version" "text")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    aj.id AS job_id,
    aj.job_type,
    aj.status AS job_status,
    aj.created_at AS job_created_at,
    aj.started_at AS job_started_at,
    aj.finished_at AS job_finished_at,
    aj.duration_ms AS job_duration_ms,
    aj.error AS job_error,
    aj.retry_count AS job_retry_count,
    aj.agent_id,
    dev.name AS agent_name,
    aj.execution_id,
    e.status AS execution_status,
    aj.job_dataset_id AS dataset_id,
    d.name AS dataset_name,
    aj.plugin_id,
    aj.plugin_version
  FROM agent_jobs aj
  LEFT JOIN devices dev ON dev.id = aj.agent_id
  LEFT JOIN executions e ON e.id = aj.execution_id
  LEFT JOIN datasets d ON d.id = aj.job_dataset_id
  WHERE aj.id = p_job_id;
END;
$$;


ALTER FUNCTION "public"."get_job_detail"("p_job_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_or_create_runtime_environment"("p_org_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_hash" "text", "p_device_id" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_env_id UUID;
    v_platform TEXT;
BEGIN
    SELECT platform INTO v_platform FROM public.devices WHERE id = p_device_id;
    
    SELECT id INTO v_env_id
    FROM public.runtime_environments
    WHERE org_id = p_org_id
      AND runtime_type = p_runtime_type
      AND runtime_version = p_runtime_version
      AND dependency_hash = p_dependency_hash
      AND platform = COALESCE(v_platform, 'linux/amd64')
      AND invalidated_at IS NULL
    ORDER BY last_used_at DESC
    LIMIT 1;

    IF NOT FOUND THEN
        INSERT INTO public.runtime_environments (
            org_id, runtime_type, runtime_version, dependency_hash,
            device_id, platform, created_at, last_used_at
        ) VALUES (
            p_org_id, p_runtime_type, p_runtime_version, p_dependency_hash,
            p_device_id, COALESCE(v_platform, 'linux/amd64'), NOW(), NOW()
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO v_env_id;
    END IF;

    RETURN v_env_id;
END;
$$;


ALTER FUNCTION "public"."get_or_create_runtime_environment"("p_org_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_hash" "text", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer DEFAULT 100) RETURNS TABLE("id" "uuid", "event_type" "text", "message" "text", "created_at" timestamp with time zone, "org_id" "uuid")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        sl.id,
        sl.event_type,
        sl.message,
        sl.created_at,
        sl.org_id
    FROM system_logs sl
    WHERE sl.org_id = p_org_id
       OR sl.org_id IS NULL
    ORDER BY sl.created_at DESC
    LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer DEFAULT 100, "p_event_type_filter" "text" DEFAULT NULL::"text") RETURNS TABLE("id" "uuid", "event_type" "text", "message" "text", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    SELECT sl.id, sl.event_type, sl.message, sl.created_at
    FROM system_logs sl
    WHERE sl.event_type = ANY(public.get_audit_event_types())
      AND (p_event_type_filter IS NULL OR sl.event_type = p_event_type_filter)
    ORDER BY sl.created_at DESC LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer, "p_event_type_filter" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_org_plugins"("p_org_id" "uuid", "p_os" "text" DEFAULT 'linux'::"text", "p_arch" "text" DEFAULT 'amd64'::"text") RETURNS TABLE("plugin_id" "uuid", "name" "text", "version" "text", "language" "text", "plugin_type" "text", "storage_path" "text", "checksum" "text", "signature" "bytea", "signature_key_id" "text", "resources" "jsonb", "trusted" boolean, "rollout_percentage" integer, "os" "text", "arch" "text", "plugin_group" "text", "network" boolean)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.id AS plugin_id, p.name, p.version, p.language, p.plugin_type,
        p.storage_path, p.checksum, p.signature, p.signature_key_id,
        p.resources, p.trusted, op.rollout_percentage, p.os, p.arch,
        p.plugin_group, COALESCE(p.network, false) AS network
    FROM org_plugins op
    JOIN plugins p ON p.id = op.plugin_id
    WHERE op.org_id = p_org_id
        AND op.enabled = true
        AND (p.os = 'any' OR p.os = p_os)
        AND (p.arch = 'any' OR p.arch = p_arch)
    ORDER BY p.name, p.version DESC;
END;
$$;


ALTER FUNCTION "public"."get_org_plugins"("p_org_id" "uuid", "p_os" "text", "p_arch" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_org_storage_configs"("p_org_id" "uuid") RETURNS TABLE("id" "uuid", "name" "text", "storage_mode" "text", "provider" "text", "bucket_name" "text", "region" "text", "endpoint" "text", "mount_base_path" "text", "is_default" boolean, "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    osc.id, osc.name, osc.storage_mode, osc.provider,
    osc.bucket_name, osc.region, osc.endpoint,
    osc.mount_base_path, osc.is_default, osc.created_at
  FROM org_storage_configs osc
  WHERE osc.org_id = p_org_id
  ORDER BY osc.is_default DESC, osc.created_at DESC;
END;
$$;


ALTER FUNCTION "public"."get_org_storage_configs"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_pipeline_status"("p_org_id" "uuid") RETURNS TABLE("pending_count" bigint, "running_count" bigint, "completed_count" bigint, "failed_count" bigint, "total_count" bigint)
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
    SELECT
        COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
        COUNT(*) FILTER (WHERE status = 'running') as running_count,
        COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
        COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
        COUNT(*) as total_count
    FROM public.agent_jobs
    WHERE org_id = p_org_id;
$$;


ALTER FUNCTION "public"."get_pipeline_status"("p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_pipeline_template"("p_template_id" "uuid") RETURNS TABLE("id" "uuid", "name" "text", "description" "text", "dataset_id" "uuid", "steps" "jsonb", "created_at" timestamp with time zone, "updated_at" timestamp with time zone, "dataset_name" "text")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    pt.id, pt.name, pt.description, pt.dataset_id,
    pt.steps, pt.created_at, pt.updated_at,
    d.name AS dataset_name
  FROM pipeline_templates pt
  LEFT JOIN datasets d ON d.id = pt.dataset_id
  WHERE pt.id = p_template_id;
END;
$$;


ALTER FUNCTION "public"."get_pipeline_template"("p_template_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_plugin_by_id"("p_plugin_id" "uuid") RETURNS TABLE("id" "uuid", "name" "text", "version" "text", "language" "text", "plugin_type" "text", "description" "text", "category" "text", "trusted" boolean, "created_at" timestamp with time zone, "config_schema" "jsonb", "input_schema" "jsonb", "output_schema" "jsonb")
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    p.id, p.name, p.version, p.language, p.plugin_type,
    p.description, p.category, p.trusted, p.created_at,
    p.config_schema, p.input_schema, p.output_schema
  FROM plugins p
  WHERE p.id = p_plugin_id;
END;
$$;


ALTER FUNCTION "public"."get_plugin_by_id"("p_plugin_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_recent_activity"("p_limit" integer DEFAULT 20) RETURNS TABLE("id" "uuid", "job_type" "text", "status" "text", "created_at" timestamp with time zone, "finished_at" timestamp with time zone, "duration_ms" bigint, "agent_name" "text", "dataset_name" "text", "error" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        aj.id,
        aj.job_type,
        aj.status,
        aj.created_at,
        aj.finished_at,
        COALESCE(aj.duration_ms, 0)::bigint,
        COALESCE(d.name, 'Unknown') as agent_name,
        COALESCE(ds.name, 'Unknown') as dataset_name,
        COALESCE(aj.error, '') as error
    FROM agent_jobs aj
    LEFT JOIN devices d ON d.id = aj.agent_id
    LEFT JOIN datasets ds ON ds.id = aj.job_dataset_id
    ORDER BY aj.created_at DESC
    LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."get_recent_activity"("p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_step_outputs"("p_execution_step_id" "uuid") RETURNS TABLE("id" "uuid", "output_key" "text", "output_value" "jsonb", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    so.id, so.output_key, so.output_value, so.created_at
  FROM step_outputs so
  WHERE so.execution_step_id = p_execution_step_id
  ORDER BY so.created_at;
END;
$$;


ALTER FUNCTION "public"."get_step_outputs"("p_execution_step_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_triggers_report"() RETURNS TABLE("table_name" "text", "trigger_name" "text", "event_manipulation" "text", "action_timing" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT 
    t.event_object_table::text,
    t.trigger_name::text,
    t.event_manipulation::text,
    t.action_timing::text
  FROM information_schema.triggers t
  WHERE t.trigger_schema = 'public' 
  AND t.event_object_table IN ('datasets', 'batch_chunks', 'agent_jobs')
  ORDER BY t.event_object_table, t.trigger_name;
END;
$$;


ALTER FUNCTION "public"."get_triggers_report"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_user_org_role"("p_user_id" "uuid") RETURNS TABLE("org_id" "uuid", "role" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY
  SELECT om.org_id, om.role
  FROM org_members om
  WHERE om.user_id = p_user_id;
END;
$$;


ALTER FUNCTION "public"."get_user_org_role"("p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."global_search"("p_org_id" "uuid", "p_query" "text", "p_type" "text" DEFAULT 'all'::"text", "p_limit" integer DEFAULT 20) RETURNS TABLE("result_type" "text", "id" "uuid", "name" "text", "status" "text", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  (
    SELECT
      'job'::text AS result_type,
      aj.id,
      aj.job_type AS name,
      aj.status,
      aj.created_at
    FROM agent_jobs aj
    WHERE aj.org_id = p_org_id
      AND (p_type = 'all' OR p_type = 'jobs')
      AND (aj.job_type ILIKE p_query || '%' OR aj.id::text ILIKE p_query || '%')
    LIMIT p_limit
  )
  UNION ALL
  (
    SELECT
      'dataset'::text AS result_type,
      d.id,
      d.name,
      d.status,
      d.created_at
    FROM datasets d
    WHERE d.org_id = p_org_id
      AND (p_type = 'all' OR p_type = 'datasets')
      AND d.name ILIKE p_query || '%'
    LIMIT p_limit
  )
  UNION ALL
  (
    SELECT
      'device'::text AS result_type,
      dev.id,
      dev.name,
      dev.status,
      dev.created_at
    FROM devices dev
    WHERE dev.org_id = p_org_id
      AND (p_type = 'all' OR p_type = 'agents')
      AND dev.name ILIKE p_query || '%'
    LIMIT p_limit
  );
END;
$$;


ALTER FUNCTION "public"."global_search"("p_org_id" "uuid", "p_query" "text", "p_type" "text", "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."handle_dataset_scan_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_org_id UUID;
    v_job_id UUID;
BEGIN
    -- Get org_id from the dataset
    v_org_id := NEW.org_id;
    
    IF v_org_id IS NULL THEN
        RAISE WARNING 'Dataset % has no org_id, skipping job creation', NEW.id;
        RETURN NEW;
    END IF;
    
    -- Only create job if status is 'pending' or 'scanning'
    IF NEW.status IN ('pending', 'scanning') AND 
       (OLD IS NULL OR OLD.status IS DISTINCT FROM NEW.status) THEN
        
        INSERT INTO public.agent_jobs (
            org_id,
            job_type,
            status,
            input_payload,
            created_at,
            updated_at
        ) VALUES (
            v_org_id,
            'scan_dataset',
            'pending',
            jsonb_build_object(
                'dataset_id', NEW.id,
                'dataset_name', NEW.name,
                'bucket_path', NEW.bucket_path
            ),
            NOW(),
            NOW()
        )
        RETURNING id INTO v_job_id;
        
        RAISE NOTICE 'Created scan_dataset job % for dataset %', v_job_id, NEW.id;
    END IF;
    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."handle_dataset_scan_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."handle_job_failure"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.status = 'failed' AND NEW.retry_count >= COALESCE(NEW.max_retries, 3) THEN
        INSERT INTO public.job_notification_queue (job_id, event_type, payload)
        VALUES (
            NEW.id,
            'job_dead_letter',
            jsonb_build_object(
                'job_id', NEW.id,
                'error_message', NEW.last_error,
                'retry_count', NEW.retry_count
            )
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."handle_job_failure"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."increment_device_job"("dev_id" "uuid", "org_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_dev_org uuid;
BEGIN
  SELECT org_id INTO v_dev_org
  FROM public.devices
  WHERE id = dev_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'device % not found', dev_id;
  END IF;

  IF v_dev_org != org_id THEN
    RAISE EXCEPTION 'org_id mismatch';
  END IF;

  UPDATE public.devices
  SET total_jobs = COALESCE(total_jobs, 0) + 1
  WHERE id = dev_id;
END;
$$;


ALTER FUNCTION "public"."increment_device_job"("dev_id" "uuid", "org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."insert_device_agent_metric"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.device_agent_metrics (
        device_id, cpu_usage_percent, memory_usage_percent, active_jobs, recorded_at
    ) VALUES (
        NEW.id, NEW.cpu_usage_percent, NEW.memory_usage_percent,
        NEW.active_job_count, NOW()
    );
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."insert_device_agent_metric"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."insert_device_agent_metric"("p_device_id" "uuid", "p_org_id" "uuid", "p_source" "text" DEFAULT 'unknown'::"text", "p_metrics" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    INSERT INTO agent_metrics (device_id, org_id, source, metrics)
    VALUES (p_device_id, p_org_id, p_source, p_metrics);
END;
$$;


ALTER FUNCTION "public"."insert_device_agent_metric"("p_device_id" "uuid", "p_org_id" "uuid", "p_source" "text", "p_metrics" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."invoke_auto_assign_best_device"("p_dataset_id" "text") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  INSERT INTO public.http_queue (url, body, headers, idempotency_key)
  VALUES (
    '/functions/v1/auto_assign_best_device',
    json_build_object('dataset_id', p_dataset_id),
    jsonb_build_object('Content-Type', 'application/json'),
    gen_random_uuid()
  )
  ON CONFLICT (idempotency_key) DO NOTHING;
END;
$$;


ALTER FUNCTION "public"."invoke_auto_assign_best_device"("p_dataset_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."invoke_optimal_chunk_size_calculation"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  UPDATE public.chunk_complexity_cache
  SET complexity_score = NEW.throughput,
      last_used_at = NOW()
  WHERE device_id = NEW.device_id;

  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."invoke_optimal_chunk_size_calculation"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_org_admin"("_org_id" "uuid") RETURNS boolean
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.org_members
    WHERE user_id = auth.uid()
      AND org_id = _org_id
      AND role = 'admin'
  );
$$;


ALTER FUNCTION "public"."is_org_admin"("_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_org_member"("_org_id" "uuid") RETURNS boolean
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.org_members
    WHERE user_id = auth.uid()
      AND org_id = _org_id
  );
$$;


ALTER FUNCTION "public"."is_org_member"("_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lease_agent_job"("p_job_id" "uuid", "p_org_id" "uuid", "p_agent_id" "uuid", "p_ttl_secs" integer DEFAULT 1800) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job RECORD;
  v_expires_at TIMESTAMPTZ;
  v_updated_rows INTEGER;
BEGIN
  v_expires_at := NOW() + (p_ttl_secs || ' seconds')::INTERVAL;

  SELECT id, status, agent_id, org_id
  INTO v_job
  FROM public.agent_jobs
  WHERE id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN 
    RAISE EXCEPTION 'job not found';
  END IF;

  IF v_job.org_id != p_org_id THEN
    RAISE EXCEPTION 'device ownership violation - org_id mismatch';
  END IF;

  IF v_job.agent_id != p_agent_id THEN
    RAISE EXCEPTION 'device ownership violation - job assigned to different device';
  END IF;

  IF v_job.status != 'assigned' THEN
    RAISE EXCEPTION 'state transition failed - job must be assigned, got %', v_job.status;
  END IF;

  -- ATOMIC UPSERT for lease
  INSERT INTO leases (job_id, device_id, lease_expires_at, status)
  VALUES (p_job_id, p_agent_id, v_expires_at, 'active')
  ON CONFLICT (job_id) WHERE status = 'active'
  DO UPDATE SET
    device_id = EXCLUDED.device_id,
    lease_expires_at = EXCLUDED.lease_expires_at,
    status = 'active';

  -- EXPLICIT UPDATE to running
  UPDATE public.agent_jobs
  SET status = 'running',
      lease_expires_at = v_expires_at,
      started_at = COALESCE(started_at, NOW()),
      updated_at = NOW(),
      last_transition_at = NOW()
  WHERE id = p_job_id
    AND status = 'assigned'
  RETURNING 1 INTO v_updated_rows;

  IF v_updated_rows IS NULL OR v_updated_rows = 0 THEN
    RAISE EXCEPTION 'state transition failed - concurrent modification detected';
  END IF;

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."lease_agent_job"("p_job_id" "uuid", "p_org_id" "uuid", "p_agent_id" "uuid", "p_ttl_secs" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."list_pipeline_templates"("p_org_id" "uuid" DEFAULT NULL::"uuid", "p_limit" integer DEFAULT 50) RETURNS TABLE("id" "uuid", "name" "text", "description" "text", "dataset_id" "uuid", "dataset_name" "text", "step_count" integer, "created_at" timestamp with time zone, "updated_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    pt.id,
    pt.name,
    pt.description,
    pt.dataset_id,
    d.name AS dataset_name,
    COALESCE(jsonb_array_length(pt.steps), 0)::integer AS step_count,
    pt.created_at,
    pt.updated_at
  FROM pipeline_templates pt
  LEFT JOIN datasets d ON d.id = pt.dataset_id
  WHERE p_org_id IS NULL OR pt.org_id = p_org_id
  ORDER BY pt.updated_at DESC
  LIMIT p_limit;
END;
$$;


ALTER FUNCTION "public"."list_pipeline_templates"("p_org_id" "uuid", "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."list_public_functions"() RETURNS TABLE("function_name" "text", "arguments" "text", "return_type" "text", "language" "text", "function_code" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY SELECT * FROM _internal.list_public_functions();
END;
$$;


ALTER FUNCTION "public"."list_public_functions"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."log_agent_error"("p_agent_id" "text", "p_message" "text") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_sanitized_message TEXT;
BEGIN
    v_sanitized_message := p_message;
    
    v_sanitized_message := regexp_replace(v_sanitized_message, '/[^/\s]+/[^/\s]+', '[PATH]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, '[A-Za-z]:\\[^\\]+\\[^\\]+', '[PATH]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, 's3://[^\s]+', 's3://[BUCKET]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, 'gs://[^\s]+', 'gs://[BUCKET]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, 'https://[^/]+/[^?\s]+', 'https://[STORAGE]/[OBJECT]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, '~[^/\s]+', '~[USER]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[^\s]+', '[IP]/[PATH]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL]', 'g');

    INSERT INTO public.system_logs(
        event_type,
        message,
        created_at
    )
    VALUES(
        'agent_error',
        v_sanitized_message,
        now()
    );
END;
$$;


ALTER FUNCTION "public"."log_agent_error"("p_agent_id" "text", "p_message" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_sanitized_message TEXT;
BEGIN
    v_sanitized_message := _error_message;
    
    -- Remove filesystem paths
    v_sanitized_message := regexp_replace(v_sanitized_message, '/[^/\s]+/[^/\s]+', '[PATH]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, '[A-Za-z]:\\[^\\]+\\[^\\]+', '[PATH]', 'g');
    
    -- Remove object storage URIs
    v_sanitized_message := regexp_replace(v_sanitized_message, 's3://[^\s]+', 's3://[BUCKET]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, 'gs://[^\s]+', 'gs://[BUCKET]', 'g');
    v_sanitized_message := regexp_replace(v_sanitized_message, 'https://[^/]+/[^?\s]+', 'https://[STORAGE]/[OBJECT]', 'g');
    
    -- Remove home directory paths
    v_sanitized_message := regexp_replace(v_sanitized_message, '~[^/\s]+', '~[USER]', 'g');
    
    -- Remove IP addresses in file paths
    v_sanitized_message := regexp_replace(v_sanitized_message, '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[^\s]+', '[IP]/[PATH]', 'g');

    INSERT INTO public.system_logs (event_type, message, created_at)
    VALUES ('agent_error', COALESCE(v_sanitized_message, '[SANITIZATION_FAILED]'), NOW());
END;
$$;


ALTER FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") IS 'Simple logging helper for agent errors.';



CREATE OR REPLACE FUNCTION "public"."manage_agent_job_state"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status NOT IN ('pending') THEN
            RAISE EXCEPTION 'Initial status must be pending';
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.status = OLD.status THEN
            RETURN NEW;
        END IF;

        IF OLD.status = 'pending' AND NEW.status NOT IN ('assigned', 'cancelled', 'failed') THEN
            RAISE EXCEPTION 'Pending jobs can only transition to assigned, cancelled, or failed';
        END IF;

        IF OLD.status = 'assigned' AND NEW.status NOT IN ('running', 'pending', 'cancelled', 'failed') THEN
            RAISE EXCEPTION 'Assigned jobs can only transition to running, pending, cancelled, or failed';
        END IF;

        IF OLD.status = 'running' AND NEW.status NOT IN ('completed', 'failed', 'pending') THEN
            RAISE EXCEPTION 'Running jobs can only transition to completed, failed, or pending';
        END IF;

        IF OLD.status = 'completed' OR OLD.status = 'failed' OR OLD.status = 'cancelled' THEN
            RAISE EXCEPTION 'Final status cannot be changed: %', OLD.status;
        END IF;
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."manage_agent_job_state"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."mark_offline_devices"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    UPDATE public.devices SET status = 'offline', updated_at = NOW()
    WHERE status = 'online' AND last_heartbeat < NOW() - INTERVAL '10 minutes';
END;
$$;


ALTER FUNCTION "public"."mark_offline_devices"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."match_best_device"("p_job_id" "text", "p_org_id" "text" DEFAULT NULL::"text") RETURNS TABLE("device_id" "text", "device_name" "text", "match_score" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    SELECT d.id::TEXT, d.name::TEXT,
        (CASE WHEN d.status = 'online' THEN 1.0 ELSE 0.0 END +
         CASE WHEN d.cpu_usage_percent < 50 THEN 0.3 ELSE 0.0 END +
         CASE WHEN d.memory_usage_percent < 50 THEN 0.2 ELSE 0.0 END +
         CASE WHEN d.last_heartbeat > NOW() - INTERVAL '5 minutes' THEN 0.5 ELSE 0.0 END
        )::FLOAT AS score
    FROM public.devices d
    WHERE d.status = 'online'
    AND (p_org_id IS NULL OR d.org_id = p_org_id)
    AND d.last_heartbeat > NOW() - INTERVAL '10 minutes'
    ORDER BY score DESC LIMIT 5;
END;
$$;


ALTER FUNCTION "public"."match_best_device"("p_job_id" "text", "p_org_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."match_best_device"("_org_id" "uuid", "_chunk_vector" "public"."vector", "_job_type" "text") RETURNS TABLE("id" "uuid", "device_id" "uuid", "score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_safe_vec vector(16);
  v_is_zero  BOOLEAN;
BEGIN

  v_safe_vec := COALESCE(
    _chunk_vector,
    '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'::vector
  );

  SELECT (v_safe_vec <#> v_safe_vec) = 0 INTO v_is_zero;

  IF COALESCE(v_is_zero, true) THEN
    v_safe_vec := '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'::vector;
  END IF;

  RETURN QUERY
  SELECT
    d.id,
    d.id AS device_id,
    (
      (1 - (dv.profile_vector <=> v_safe_vec))

      *

      (
        0.5
        + (COALESCE(d.cpu_cores_free,  4)::float8 / GREATEST(d.total_cpu_cores, 8)::float8) * 0.3
        + (COALESCE(d.memory_free_gb,  8)::float8 / GREATEST(d.total_memory_gb, 32)::float8) * 0.2
      )

      *

      (
        CASE
          WHEN d.max_concurrency > 0
          THEN GREATEST(0.1,
            (d.max_concurrency - COALESCE(d.active_jobs, 0))::float8 / d.max_concurrency
          )
          ELSE 1.0
        END
      )

    ) AS score

  FROM public.devices d
  JOIN public.device_vectors dv ON dv.device_id = d.id
  WHERE d.org_id = _org_id
    AND d.status IN ('online', 'available')
    AND dv.profile_vector IS NOT NULL
    AND (dv.profile_vector <#> dv.profile_vector) > 0
    AND (
      d.max_concurrency IS NULL
      OR COALESCE(d.active_jobs, 0) < d.max_concurrency
    )

  ORDER BY score DESC
  LIMIT 1;

END;
$$;


ALTER FUNCTION "public"."match_best_device"("_org_id" "uuid", "_chunk_vector" "public"."vector", "_job_type" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."match_best_execution_target"("p_org_id" "uuid", "p_job_vector" "public"."vector", "p_job_type" "text", "p_runtime_type" "text" DEFAULT 'native'::"text", "p_execution_mode" "text" DEFAULT 'native'::"text") RETURNS TABLE("device_id" "uuid", "execution_mode" "text", "compatibility_score" numeric)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    AS $$
DECLARE
    v_device RECORD;
    v_score numeric;
BEGIN
    FOR v_device IN
        SELECT 
            d.id,
            d.name,
            d.status,
            d.active_jobs,
            d.max_concurrency,
            d.device_vector,
            d.runtime_supported,
            d.docker_available,
            d.capabilities
        FROM public.devices d
        LEFT JOIN public.device_vectors dv ON d.id = dv.device_id
        WHERE d.org_id = p_org_id
          AND d.status IN ('online', 'available', 'busy')
          AND (d.active_jobs IS NULL OR d.active_jobs < COALESCE(d.max_concurrency, 1))
    LOOP
        -- Check runtime compatibility
        IF p_runtime_type != 'native' THEN
            IF NOT public.device_supports_runtime(v_device.id, p_runtime_type) THEN
                CONTINUE;
            END IF;
        END IF;

        -- Check execution mode compatibility
        IF NOT public.device_supports_execution_mode(v_device.id, p_execution_mode) THEN
            CONTINUE;
        END IF;

        -- Calculate compatibility score
        v_score := 1.0;

        -- Boost score for runtime match
        IF p_runtime_type != 'native' AND v_device.runtime_supported @> to_jsonb(p_runtime_type) THEN
            v_score := v_score * 1.5;
        END IF;

        -- Boost score for docker availability if needed
        IF p_execution_mode = 'docker' AND v_device.docker_available = true THEN
            v_score := v_score * 1.2;
        END IF;

        -- Factor in device similarity if vector available
        IF v_device.device_vector IS NOT NULL THEN
            v_score := v_score * (1.0 + (v_device.device_vector <#> p_job_vector) * 0.5);
        END IF;

        -- Factor in current load
        IF v_device.active_jobs IS NOT NULL AND v_device.max_concurrency IS NOT NULL THEN
            v_score := v_score * (1.0 - (v_device.active_jobs::numeric / v_device.max_concurrency::numeric) * 0.3);
        END IF;

        RETURN QUERY SELECT v_device.id, p_execution_mode, v_score;
    END LOOP;
END;
$$;


ALTER FUNCTION "public"."match_best_execution_target"("p_org_id" "uuid", "p_job_vector" "public"."vector", "p_job_type" "text", "p_runtime_type" "text", "p_execution_mode" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."migrate_enterprise_credentials_to_vault"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT provider, org_id, credentials
    FROM public.enterprise_integrations
    WHERE credentials IS NOT NULL
      AND vault_secret_name IS NULL
  LOOP
    PERFORM vault.create_secret(
      r.provider || ':' || r.org_id,
      r.credentials::text
    );

    UPDATE public.enterprise_integrations
    SET vault_secret_name = r.provider || ':' || r.org_id
    WHERE org_id = r.org_id
      AND provider = r.provider;
  END LOOP;
END;
$$;


ALTER FUNCTION "public"."migrate_enterprise_credentials_to_vault"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."move_job_to_dead_letter"("p_job_id" "uuid", "p_org_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_job RECORD;
BEGIN
  SELECT *
  INTO v_job
  FROM public.agent_jobs
  WHERE id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  IF v_job.org_id != p_org_id THEN
    RAISE EXCEPTION 'org_id mismatch';
  END IF;

  INSERT INTO agent_jobs_dead_letter (
    org_id,
    dataset_id,
    job_type,
    payload,
    retry_count,
    last_error,
    original_job_id,
    failed_at
  )
  VALUES (
    v_job.org_id,
    (v_job.payload->>'dataset_id')::UUID,
    v_job.job_type,
    v_job.payload,
    COALESCE(v_job.retry_count, 0),
    v_job.last_error,
    v_job.id,
    NOW()
  );

  DELETE FROM public.agent_jobs
  WHERE id = p_job_id;

  RETURN true;
END;
$$;


ALTER FUNCTION "public"."move_job_to_dead_letter"("p_job_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."notify_agent_on_dataset_register"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Insert into job_notification_queue for observability
    INSERT INTO public.job_notification_queue (job_id, event_type, payload, org_id)
    VALUES (
        NULL,
        'dataset_registered',
        jsonb_build_object(
            'dataset_id', NEW.id,
            'org_id', NEW.org_id,
            'registered_at', NOW()
        ),
        NEW.org_id
    );
    
    -- Create scan_dataset job in agent_jobs
    -- Using payload to store dataset_id (NOT job_dataset_id column)
    INSERT INTO public.agent_jobs (
        org_id,
        job_type,
        status,
        payload,
        created_at,
        updated_at
    ) VALUES (
        NEW.org_id,
        'scan_dataset',
        'pending',
        jsonb_build_object(
            'dataset_id', NEW.id,
            'source_path', NEW.source_path,
            'storage_type', NEW.storage_type,
            'storage_config_id', NEW.storage_config_id
        ),
        NOW(),
        NOW()
    );
    
    -- Update dataset status to indicate scan job was created
    UPDATE public.datasets 
    SET status = 'scanning', updated_at = NOW()
    WHERE id = NEW.id;
    
    RAISE NOTICE 'Created scan_dataset job for dataset % and updated status to scanning', NEW.id;
    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."notify_agent_on_dataset_register"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."notify_job_queue"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  PERFORM pg_notify('job_queue_update', 'jobs_available');
END;
$$;


ALTER FUNCTION "public"."notify_job_queue"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."notify_merge_complete"("p_dataset_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  PERFORM pg_notify(
    'merge_complete',
    json_build_object(
      'dataset_id', p_dataset_id,
      'event', 'merge_complete',
      'timestamp', now()
    )::text
  );
END;
$$;


ALTER FUNCTION "public"."notify_merge_complete"("p_dataset_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."notify_new_job"("p_org_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid" DEFAULT NULL::"uuid") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    PERFORM pg_notify(
        'job_dispatch_org_' || p_org_id::text,
        json_build_object(
            'job_id', p_job_id,
            'device_id', p_device_id,
            'timestamp', NOW()
        )::text
    );
END;
$$;


ALTER FUNCTION "public"."notify_new_job"("p_org_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."on_agent_job_failed"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$BEGIN
  INSERT INTO public.system_logs(event_type, message, created_at)
  VALUES (
    'job_failed',
    format('Job %s failed on device %s', NEW.id, COALESCE(NEW.agent_id::text,'unknown')),
    now()
  );
  RETURN NEW;
END;$$;


ALTER FUNCTION "public"."on_agent_job_failed"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."on_merge_job_finished"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.status = 'completed' AND NEW.job_type = 'merge' THEN
        UPDATE public.datasets 
        SET status = 'ready', updated_at = NOW()
        WHERE id = NEW.dataset_id;
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."on_merge_job_finished"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."populate_job_runtime_info"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_plugin RECORD;
BEGIN
    SELECT p.language, p.name INTO v_plugin
    FROM plugins p
    WHERE p.id = NEW.plugin_id;

    NEW.runtime_name := v_plugin.name;
    NEW.runtime_language := v_plugin.language;

    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."populate_job_runtime_info"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_chunk_size" integer DEFAULT 1000) RETURNS TABLE("chunk_count" integer, "strategy" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_file_count int;
    v_estimated_chunks int;
BEGIN
    SELECT file_count INTO v_file_count
    FROM public.datasets
    WHERE id = p_dataset_id;

    v_estimated_chunks := CEIL(COALESCE(v_file_count, 0)::float / GREATEST(p_chunk_size, 1)::float);

    RETURN QUERY SELECT v_estimated_chunks, 'file_based'::text;
END;
$$;


ALTER FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_chunk_size" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_org_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_avg_chunk_size FLOAT;
    v_total_size FLOAT;
    v_chunk_count INT;
    i INT;
BEGIN
    SELECT COALESCE(AVG(preferred_chunk_size_gb), 1.0)
    INTO v_avg_chunk_size
    FROM public.devices
    WHERE org_id = p_org_id
      AND status IN ('online', 'available', 'busy')
      AND preferred_chunk_size_gb IS NOT NULL;

    SELECT total_size_gb INTO v_total_size
    FROM public.datasets
    WHERE id = p_dataset_id AND org_id = p_org_id;

    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'Dataset not found');
    END IF;

    v_chunk_count := CEIL(v_total_size / NULLIF(v_avg_chunk_size, 0));

    DELETE FROM public.batch_chunks
    WHERE dataset_id = p_dataset_id
      AND dynamic_size = true
      AND status = 'pending';

    FOR i IN 0..(v_chunk_count - 1) LOOP
        INSERT INTO public.batch_chunks (
            dataset_id, chunk_index, status, job_type,
            chunk_size_gb, dynamic_size, org_id
        ) VALUES (
            p_dataset_id, i, 'pending', 'preprocess',
            v_avg_chunk_size, true, p_org_id
        );
    END LOOP;

    RETURN jsonb_build_object(
        'ok', true,
        'chunks_created', v_chunk_count,
        'chunk_size_gb', v_avg_chunk_size
    );
END;
$$;


ALTER FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."prevent_overassign_agent_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = NEW.id
      AND status IN ('assigned', 'running')
  ) THEN
    RAISE EXCEPTION 'job % is already assigned or running', NEW.id;
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."prevent_overassign_agent_job"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."process_http_queue"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.system_logs (level, source, message, created_at)
    VALUES ('info', 'cron', 'HTTP queue processed', NOW());
END;
$$;


ALTER FUNCTION "public"."process_http_queue"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."process_http_queue"("p_limit" integer DEFAULT 100, "p_max_retries" integer DEFAULT 3) RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_queue_item RECORD;
  v_processed integer := 0;
  v_edge_url text;
  v_cron_secret text;
  v_result record;
BEGIN
  v_edge_url := public.get_edge_url();
  IF v_edge_url IS NULL THEN RETURN 0; END IF;

  SELECT value INTO v_cron_secret FROM public.system_config WHERE key = 'cron_secret';

  FOR v_queue_item IN
    SELECT id, url, body, headers, retry_count FROM public.http_queue
    WHERE processed = false AND retry_count < p_max_retries
      AND (retry_at IS NULL OR retry_at < NOW())
    ORDER BY created_at ASC LIMIT p_limit
  LOOP
    BEGIN
      SELECT * INTO v_result FROM net.http_post(
        url     := v_edge_url || v_queue_item.url,
        headers := COALESCE(v_queue_item.headers, '{}'::jsonb) || jsonb_build_object('x-cron-secret', COALESCE(v_cron_secret, '')),
        body    := v_queue_item.body
      );
      UPDATE public.http_queue SET processed = true, processed_at = NOW(), status_code = v_result.status, result = v_result.content::text WHERE id = v_queue_item.id;
      v_processed := v_processed + 1;
    EXCEPTION WHEN OTHERS THEN
      UPDATE public.http_queue SET retry_count = retry_count + 1, retry_at = NOW() + (EXTRACT(EPOCH FROM (retry_count + 1) * interval '1 minute') || ' seconds')::interval, result = SQLERRM WHERE id = v_queue_item.id;
      IF v_queue_item.retry_count + 1 >= p_max_retries THEN
        INSERT INTO public.system_logs (event_type, message) VALUES ('http_queue_dead_letter', format('http_queue item %s exceeded max retries', v_queue_item.id));
      END IF;
    END;
  END LOOP;
  RETURN v_processed;
END;
$$;


ALTER FUNCTION "public"."process_http_queue"("p_limit" integer, "p_max_retries" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."prune_old_agent_metrics"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_deleted INTEGER;
  v_total INTEGER := 0;
BEGIN
  LOOP
    WITH to_delete AS (
      SELECT ctid
      FROM public.agent_metrics
      WHERE created_at < NOW() - INTERVAL '30 days'
      LIMIT 5000
    )
    DELETE FROM public.agent_metrics
    WHERE ctid IN (SELECT ctid FROM to_delete);

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    EXIT WHEN v_deleted = 0;
    v_total := v_total + v_deleted;
  END LOOP;

  INSERT INTO public.system_logs (event_type, message)
  VALUES (
    'metrics_prune',
    format('Agent metrics pruned: %s rows deleted', v_total)
  );
END;
$$;


ALTER FUNCTION "public"."prune_old_agent_metrics"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."prune_old_system_logs"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    DELETE FROM public.system_logs WHERE created_at < NOW() - INTERVAL '90 days';
END;
$$;


ALTER FUNCTION "public"."prune_old_system_logs"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."queue_assign_scan_job"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  resolved_org_id uuid;
BEGIN
  -- Only act if job is NOT assigned yet
  IF NEW.assigned_at IS NOT NULL THEN
    RETURN NEW;
  END IF;

  -- Resolve org_id if missing (from agent)
  IF NEW.org_id IS NULL AND NEW.agent_id IS NOT NULL THEN
    SELECT org_id
    INTO resolved_org_id
    FROM devices
    WHERE id = NEW.agent_id;
  ELSE
    resolved_org_id := NEW.org_id;
  END IF;

  -- If still no org, do nothing
  IF resolved_org_id IS NULL THEN
    RETURN NEW;
  END IF;

  -- 🔒 ATOMIC ASSIGNMENT (idempotent)
  UPDATE agent_jobs
  SET
    org_id = resolved_org_id,
    assigned_at = now(),
    updated_at = now()
  WHERE id = NEW.id
    AND assigned_at IS NULL;

  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."queue_assign_scan_job"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."queue_job_notification"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.job_notification_queue (job_id, event_type, payload)
    VALUES (
        NEW.id,
        'job_created',
        jsonb_build_object(
            'job_type', NEW.job_type,
            'org_id', NEW.org_id,
            'dataset_id', NEW.dataset_id,
            'status', NEW.status
        )
    );
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."queue_job_notification"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."recalculate_device_vector"("p_device_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  rows int;
  avg_latency numeric;
  avg_throughput numeric;
  new_vec float8[];
  norm float8 := 0;
BEGIN

  SELECT count(*)
  INTO rows
  FROM public.device_benchmarks
  WHERE device_id = p_device_id;

  IF rows = 0 THEN
    RETURN;
  END IF;

  SELECT avg(latency_ms), avg(throughput)
  INTO avg_latency, avg_throughput
  FROM public.device_benchmarks
  WHERE device_id = p_device_id;

  new_vec := ARRAY[
    LEAST(1.0, COALESCE(avg_throughput / 500.0, 0.0)),
    LEAST(1.0, 1.0 - (COALESCE(avg_latency, 2000) / 2000.0)),
    LEAST(1.0, rows::float / 100.0)
  ];

  WHILE array_length(new_vec,1) < 16 LOOP
    new_vec := new_vec || 0.0;
  END LOOP;

  FOR i IN 1..array_length(new_vec,1) LOOP
    norm := norm + (new_vec[i] * new_vec[i]);
  END LOOP;

  norm := sqrt(norm);
  IF norm = 0 THEN norm := 1; END IF;

  FOR i IN 1..array_length(new_vec,1) LOOP
    new_vec[i] := new_vec[i] / norm;
  END LOOP;

  UPDATE public.device_vectors
  SET profile_vector = new_vec::vector,
      last_updated = now()
  WHERE device_id = p_device_id;

  IF NOT FOUND THEN
    INSERT INTO public.device_vectors (
        device_id,
        org_id,
        profile_vector,
        last_updated
    )
    SELECT d.id, d.org_id, new_vec::vector, now()
    FROM public.devices d
    WHERE d.id = p_device_id;
  END IF;

  INSERT INTO public.device_learning_history (
      device_id,
      profile_vector,
      recorded_at
  )
  VALUES (p_device_id, new_vec::vector, now());

END;
$$;


ALTER FUNCTION "public"."recalculate_device_vector"("p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."rechunk_for_device"("p_device_id" "uuid", "p_job_id" "uuid", "p_chunk_count" integer, "p_strategy" "text" DEFAULT 'uniform'::"text") RETURNS TABLE("new_chunk_count" integer)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_count int;
BEGIN
    -- Placeholder implementation
    v_count := p_chunk_count;
    RETURN QUERY SELECT v_count;
END;
$$;


ALTER FUNCTION "public"."rechunk_for_device"("p_device_id" "uuid", "p_job_id" "uuid", "p_chunk_count" integer, "p_strategy" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."rechunk_for_device"("p_dataset_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text" DEFAULT 'process'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_preferred_size FLOAT;
    v_total_size FLOAT;
    v_new_count INT;
    i INT;
BEGIN
    SELECT preferred_chunk_size_gb INTO v_preferred_size
    FROM public.devices
    WHERE id = p_device_id AND org_id = p_org_id;

    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'Device not found');
    END IF;

    SELECT total_size_gb INTO v_total_size
    FROM public.datasets
    WHERE id = p_dataset_id AND org_id = p_org_id;

    DELETE FROM public.batch_chunks
    WHERE dataset_id = p_dataset_id
      AND dynamic_size = true
      AND job_type = p_job_type
      AND status = 'pending';

    v_new_count := CEIL(v_total_size / NULLIF(v_preferred_size, 0));

    FOR i IN 0..(v_new_count - 1) LOOP
        INSERT INTO public.batch_chunks (
            dataset_id, chunk_index, status, job_type,
            chunk_size_gb, dynamic_size, org_id, assigned_device_id
        ) VALUES (
            p_dataset_id, i, 'pending', p_job_type,
            v_preferred_size, true, p_org_id, p_device_id
        );
    END LOOP;

    RETURN jsonb_build_object(
        'ok', true,
        'new_chunks', v_new_count,
        'device_chunk_size_gb', v_preferred_size
    );
END;
$$;


ALTER FUNCTION "public"."rechunk_for_device"("p_dataset_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."reclaim_jobs_from_device"("p_device_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_reclaimed_count integer := 0;
    v_updated_rows integer;
BEGIN
    UPDATE agent_jobs
    SET status = 'pending',
        agent_id = NULL,
        assigned_at = NULL,
        lease_expires_at = NULL,
        retry_count = COALESCE(retry_count, 0) + 1,
        last_error = 'device offline - job auto-reclaimed',
        updated_at = NOW(),
        last_transition_at = NOW()
    WHERE agent_id = p_device_id
      AND status IN ('assigned', 'running')
    RETURNING 1 INTO v_updated_rows;

    v_reclaimed_count := COALESCE(v_updated_rows, 0);

    UPDATE leases
    SET status = 'cancelled'
    WHERE device_id = p_device_id AND status = 'active';

    UPDATE devices
    SET active_jobs = 0
    WHERE id = p_device_id;

    RETURN jsonb_build_object(
        'ok', true,
        'jobs_reclaimed', v_reclaimed_count,
        'device_id', p_device_id,
        'reclaimed_at', NOW()
    );
END;
$$;


ALTER FUNCTION "public"."reclaim_jobs_from_device"("p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."reconcile_device_stale_jobs"("p_device_id" "uuid", "p_org_id" "uuid") RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_count   integer;
  v_dev_org uuid;
BEGIN
  SELECT org_id INTO v_dev_org
  FROM public.devices
  WHERE id = p_device_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'device not found: %', p_device_id;
  END IF;

  IF v_dev_org != p_org_id THEN
    RAISE EXCEPTION 'org_id mismatch for device %', p_device_id;
  END IF;

  UPDATE public.agent_jobs
  SET
    status           = 'pending',
    agent_id         = NULL,
    lease_expires_at = NULL,
    retry_count      = COALESCE(retry_count, 0) + 1,
    last_error       = 'agent reconcile on restart',
    updated_at       = now()
  WHERE agent_id        = p_device_id
    AND org_id          = p_org_id
    AND status          IN ('assigned', 'running')
    AND lease_expires_at < now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;


ALTER FUNCTION "public"."reconcile_device_stale_jobs"("p_device_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision DEFAULT NULL::double precision, "_throughput" double precision DEFAULT NULL::double precision, "_output_token" "text" DEFAULT NULL::"text", "_plugin_id" "text" DEFAULT NULL::"text", "_plugin_version" "text" DEFAULT NULL::"text", "_metrics" "jsonb" DEFAULT NULL::"jsonb") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF _job_id IS NULL THEN
    RAISE EXCEPTION 'job_id is required';
  END IF;

  BEGIN
    UPDATE public.agent_jobs
    SET
      status = _status,
      finished_at = CASE WHEN _status IN ('completed','failed') THEN now() ELSE finished_at END,
      duration_ms = COALESCE(_duration_ms, duration_ms),
      throughput = COALESCE(_throughput, throughput),
      output_token = COALESCE(_output_token, output_token),
      plugin_id = COALESCE(_plugin_id, plugin_id),
      plugin_version = COALESCE(_plugin_version, plugin_version),
      updated_at = now()
    WHERE id = _job_id;

    -- if metrics provided, insert into agent_metrics for telemetry and keep audit
    IF _metrics IS NOT NULL THEN
      INSERT INTO public.agent_metrics(device_id, org_id, metrics, source, created_at)
      SELECT agent_id, org_id, _metrics, 'record_agent_job_result', now()
      FROM public.agent_jobs WHERE id = _job_id;
    END IF;

    -- optional: if job failed, log
    IF lower(_status) = 'failed' THEN
      INSERT INTO public.system_logs(event_type, message, created_at)
      VALUES ('job_failed', format('Job %s reported failed (plugin=%s)', _job_id::text, coalesce(_plugin_id,'<none>')), now());
    END IF;

  EXCEPTION WHEN others THEN
    PERFORM public.log_agent_error(NULL::uuid, _job_id, 'record_agent_job_result failed: ' || SQLERRM);
    RAISE;
  END;
END;
$$;


ALTER FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision, "_throughput" double precision, "_output_token" "text", "_plugin_id" "text", "_plugin_version" "text", "_metrics" "jsonb") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision, "_throughput" double precision, "_output_token" "text", "_plugin_id" "text", "_plugin_version" "text", "_metrics" "jsonb") IS 'Production-safe result recorder: updates job status and stores metrics into agent_metrics.';



CREATE OR REPLACE FUNCTION "public"."record_job_performance"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean DEFAULT true) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    INSERT INTO device_job_performance (device_id, org_id, job_type, duration_ms, throughput, success)
    VALUES (p_device_id, p_org_id, p_job_type, p_duration_ms, p_throughput, p_success);
END;
$$;


ALTER FUNCTION "public"."record_job_performance"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "text", "p_status" "text", "p_error" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    UPDATE public.plugin_executions SET 
        status = p_status, error_message = p_error, finished_at = NOW()
    WHERE id = p_execution_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object('success', false, 'error', 'Execution not found');
    END IF;
    RETURN jsonb_build_object('success', true, 'execution_id', p_execution_id);
END;
$$;


ALTER FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "text", "p_status" "text", "p_error" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "uuid", "p_status" "text", "p_error" "text" DEFAULT NULL::"text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  UPDATE public.plugin_execution_history
  SET 
    status = p_status,
    finished_at = NOW(),
    error = p_error,
    execution_duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - started_at))::INTEGER
  WHERE id = p_execution_id;
END;
$$;


ALTER FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "uuid", "p_status" "text", "p_error" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."record_plugin_execution_start"("p_org_id" "text", "p_plugin_id" "text", "p_job_id" "text", "p_device_id" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE v_execution_id TEXT;
BEGIN
    v_execution_id := gen_random_uuid()::TEXT;
    INSERT INTO public.plugin_executions (
        id, org_id, plugin_id, job_id, device_id, status, started_at
    ) VALUES (
        v_execution_id, p_org_id, p_plugin_id, p_job_id, p_device_id, 'running', NOW()
    );
    RETURN jsonb_build_object('execution_id', v_execution_id);
END;
$$;


ALTER FUNCTION "public"."record_plugin_execution_start"("p_org_id" "text", "p_plugin_id" "text", "p_job_id" "text", "p_device_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."record_plugin_execution_start"("p_org_id" "uuid", "p_plugin_id" "uuid", "p_job_id" "uuid" DEFAULT NULL::"uuid", "p_device_id" "uuid" DEFAULT NULL::"uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_execution_id UUID;
BEGIN
  v_execution_id := gen_random_uuid();
  
  INSERT INTO public.plugin_execution_history (
    id,
    org_id,
    plugin_id,
    job_id,
    device_id,
    status,
    started_at
  ) VALUES (
    v_execution_id,
    p_org_id,
    p_plugin_id,
    p_job_id,
    p_device_id,
    'started',
    NOW()
  );
  
  RETURN v_execution_id;
END;
$$;


ALTER FUNCTION "public"."record_plugin_execution_start"("p_org_id" "uuid", "p_plugin_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer DEFAULT 30, "_max_retries" integer DEFAULT 5) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  rec RECORD;
  requeued_count int := 0;
  failed_count int := 0;
  now_ts timestamptz := now();
  v_stuck_time timestamptz;
  v_job_stuck_time timestamptz;
  v_updated_rows int;
BEGIN
  v_stuck_time := now_ts - (_timeout_minutes || ' minutes')::interval;

  FOR rec IN
    SELECT id, org_id, agent_id, status, assigned_at, started_at, retry_count
    FROM public.agent_jobs
    WHERE status IN ('assigned', 'running')
      AND (
        (assigned_at IS NOT NULL AND assigned_at < v_stuck_time)
        OR (started_at IS NOT NULL AND started_at < v_stuck_time)
        OR (assigned_at IS NULL AND agent_id IS NOT NULL AND created_at < v_stuck_time)
      )
  LOOP
    BEGIN
      SELECT COALESCE(rec.assigned_at, rec.started_at, rec.created_at) INTO v_job_stuck_time;
      IF v_job_stuck_time >= v_stuck_time THEN CONTINUE; END IF;

      IF COALESCE(rec.retry_count, 0) >= _max_retries THEN
        UPDATE public.agent_jobs
        SET status = 'failed',
            completed = true,
            error = 'stuck job auto-failed: exceeded max retries',
            finished_at = now_ts,
            updated_at = now_ts,
            last_transition_at = now_ts,
            last_error = 'stuck job auto-failed: exceeded max retries'
        WHERE id = rec.id;

        DELETE FROM leases WHERE job_id = rec.id;

        INSERT INTO public.system_logs(event_type, message, created_at)
        VALUES ('job_auto_failed', format('Job %s auto-failed: retry_count=%s', rec.id, rec.retry_count), now_ts);
        failed_count := failed_count + 1;
      ELSE
        -- Requeue: ALWAYS clear agent_id when setting status to pending
        UPDATE public.agent_jobs
        SET status = 'pending',
            agent_id = NULL,
            assigned_at = NULL,
            started_at = NULL,
            lease_expires_at = NULL,
            retry_count = COALESCE(rec.retry_count, 0) + 1,
            updated_at = now_ts,
            last_transition_at = now_ts,
            last_error = 'stuck job requeued: ' || now()::text
        WHERE id = rec.id
        RETURNING id INTO v_updated_rows;

        DELETE FROM leases WHERE job_id = rec.id;

        IF v_updated_rows > 0 THEN
          INSERT INTO public.system_logs(event_type, message, created_at)
          VALUES ('job_requeued', format('Job %s requeued: was stuck in %s state', rec.id, rec.status), now_ts);
          requeued_count := requeued_count + 1;
        END IF;
      END IF;
    END;
  END LOOP;

  -- Also fix any existing inconsistent states
  UPDATE public.agent_jobs
  SET agent_id = NULL,
      assigned_at = NULL,
      lease_expires_at = NULL,
      last_error = 'fixed: stale agent_id cleared during recover_stuck_jobs',
      updated_at = now_ts
  WHERE status = 'pending'
    AND agent_id IS NOT NULL
    AND last_error != 'fixed: stale agent_id cleared during recover_stuck_jobs';

  GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
  IF v_updated_rows > 0 THEN
    INSERT INTO public.system_logs(event_type, message, created_at)
    VALUES ('jobs_fixed', format('Fixed %s jobs with stale agent_id', v_updated_rows), now_ts);
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'requeued', requeued_count,
    'failed', failed_count,
    'fixed_inconsistent', v_updated_rows,
    'checked_at', now_ts
  );
EXCEPTION WHEN OTHERS THEN
  PERFORM public.log_agent_error(NULL::uuid, NULL::uuid, 'recover_stuck_jobs failed: ' || SQLERRM);
  RETURN jsonb_build_object('ok', false, 'error', SQLERRM);
END;
$$;


ALTER FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer, "_max_retries" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer, "_max_retries" integer) IS 'Find assigned/running jobs older than timeout -> requeue or fail (safe).';



CREATE OR REPLACE FUNCTION "public"."refresh_dataset_merge_lock"("p_lock_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_lock RECORD;
  v_new_expires_at TIMESTAMPTZ;
BEGIN
  SELECT * INTO v_lock
  FROM public.dataset_merge_locks
  WHERE id = p_lock_id AND status = 'active';

  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;

  v_new_expires_at := now() + INTERVAL '30 minutes';

  -- Atomically update the lock with fresh expires_at
  -- acquired_at remains unchanged (represents when lock was first acquired)
  UPDATE public.dataset_merge_locks
  SET heartbeat_at = now(),
      expires_at = v_new_expires_at,
      updated_at = now()
  WHERE id = p_lock_id;

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."refresh_dataset_merge_lock"("p_lock_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."release_dataset_merge_lock"("p_lock_id" "uuid", "p_org_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    UPDATE dataset_merge_locks
    SET status = 'released', released_at = NOW()
    WHERE id = p_lock_id
        AND org_id = p_org_id
        AND status = 'active';
END;
$$;


ALTER FUNCTION "public"."release_dataset_merge_lock"("p_lock_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."release_lease"("p_job_id" "uuid", "p_device_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_updated_rows INTEGER;
BEGIN
  UPDATE leases
  SET status = 'released'
  WHERE job_id = p_job_id 
    AND device_id = p_device_id
    AND status = 'active'
    AND lease_expires_at > NOW()
  RETURNING 1 INTO v_updated_rows;

  IF v_updated_rows IS NULL OR v_updated_rows = 0 THEN
    RAISE EXCEPTION 'lease release failed - lease not found or already released';
  END IF;

  RETURN TRUE;
END;
$$;


ALTER FUNCTION "public"."release_lease"("p_job_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."rotate_agent_token"("p_device_id" "uuid", "p_org_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_old_token_hash TEXT;
    v_new_token_hash TEXT;
    v_token_id UUID;
BEGIN
    -- Verify org ownership
    IF NOT EXISTS (
        SELECT 1 FROM devices
        WHERE id = p_device_id AND org_id = p_org_id
    ) THEN
        RAISE EXCEPTION 'Device not found in org';
    END IF;

    -- Get current token hash
    SELECT token_hash INTO v_old_token_hash
    FROM device_tokens
    WHERE device_id = p_device_id AND status = 'active'
    ORDER BY created_at DESC
    LIMIT 1;

    -- Generate new token
    v_token_id := gen_random_uuid();
    v_new_token_hash := encode(sha256(random()::text::bytea), 'hex');

    -- Archive old token
    UPDATE device_tokens
    SET status = 'rotated', rotated_at = NOW()
    WHERE device_id = p_device_id AND status = 'active';

    -- Insert new token
    INSERT INTO device_tokens (id, device_id, token_hash, status, created_at)
    VALUES (v_token_id, p_device_id, v_new_token_hash, 'active', NOW());
END;
$$;


ALTER FUNCTION "public"."rotate_agent_token"("p_device_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."rotate_device_token"("p_device_id" "uuid", "p_token_hash" "text") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_org_id UUID;
BEGIN
    SELECT org_id INTO v_org_id FROM devices WHERE id = p_device_id;
    IF v_org_id IS NULL THEN
        RAISE EXCEPTION 'Device not found';
    END IF;
    PERFORM rotate_agent_token(p_device_id, v_org_id);
END;
$$;


ALTER FUNCTION "public"."rotate_device_token"("p_device_id" "uuid", "p_token_hash" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."run_all_validation_tests"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  RETURN QUERY SELECT * FROM test_case_1_device_available();
  RETURN QUERY SELECT * FROM test_case_2_no_device();
  RETURN QUERY SELECT * FROM test_case_3_multiple_chunks();
  RETURN QUERY SELECT * FROM test_case_4_state_machine();
  RETURN QUERY SELECT * FROM test_case_5_no_duplicates();
END;
$$;


ALTER FUNCTION "public"."run_all_validation_tests"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."safe_cast_uuid"("input" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" IMMUTABLE
    AS $$
BEGIN
  RETURN input::uuid;
EXCEPTION WHEN invalid_text_representation THEN
  RETURN NULL;
END;
$$;


ALTER FUNCTION "public"."safe_cast_uuid"("input" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."safe_http_post"("target_url" "text", "payload" "jsonb", "headers" "jsonb" DEFAULT "jsonb_build_object"('Content-Type', 'application/json')) RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
declare
  result record;
begin
  begin
    -- Try to send HTTP POST
    select * into result from net.http_post(
      target_url,
      payload,
      '{}'::jsonb,
      headers
    );

    -- Log non-200 responses
    if (result.status is null or result.status >= 400) then
      insert into public.system_logs(event_type, message, created_at)
      values (
        'http_warning',
        format('safe_http_post to %s returned status %s', target_url, coalesce(result.status::text,'null')),
        now()
      );
    end if;

  exception when others then
    -- Log any exception without interrupting parent transaction
    insert into public.system_logs(event_type, message, created_at)
    values (
      'http_error',
      format('safe_http_post failed: %s', sqlerrm),
      now()
    );
  end;
end;
$$;


ALTER FUNCTION "public"."safe_http_post"("target_url" "text", "payload" "jsonb", "headers" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector" DEFAULT NULL::"public"."vector") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE
    AS $$
DECLARE
    v_device_id uuid;
    v_default_vector vector(16) := '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'::vector(16);
    v_vector vector;
BEGIN
    v_vector := COALESCE(p_chunk_vector, v_default_vector);

    WITH candidate_devices AS (
        SELECT d.id, d.status, d.active_jobs, d.max_concurrency, d.merge_capable,
               dv.profile_vector, djts.avg_throughput as job_type_throughput,
               (dv.profile_vector <#> v_vector) * -1 as similarity,
               COALESCE(djts.avg_throughput, 1.0) as affinity_score,
               CASE WHEN p_job_type = 'merge' AND d.merge_capable = true THEN 1.5
                    WHEN p_job_type = 'merge' THEN 0.1 ELSE 1.0 END as job_type_bonus
        FROM devices d
        LEFT JOIN device_vectors dv ON dv.device_id = d.id
        LEFT JOIN device_job_type_stats djts ON djts.device_id = d.id AND djts.job_type = p_job_type
        WHERE d.org_id = p_org_id AND d.status IN ('online', 'available', 'busy')
          AND (d.active_jobs IS NULL OR d.active_jobs < COALESCE(d.max_concurrency, 1))
    )
    SELECT id INTO v_device_id
    FROM candidate_devices
    ORDER BY similarity * affinity_score * job_type_bonus DESC, active_jobs ASC NULLS FIRST
    LIMIT 1;

    IF v_device_id IS NULL THEN
        SELECT id INTO v_device_id FROM devices
        WHERE org_id = p_org_id AND status IN ('online', 'available', 'busy')
          AND (active_jobs IS NULL OR active_jobs < COALESCE(max_concurrency, 1))
        ORDER BY active_jobs ASC NULLS FIRST LIMIT 1;
    END IF;

    RETURN v_device_id;
END;
$$;


ALTER FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector") IS 'select_best_device selects best device for job.
    Includes busy devices if they have capacity (active_jobs < max_concurrency).
    This allows multi-job devices to receive more work.';



CREATE OR REPLACE FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text" DEFAULT 'any'::"text", "p_chunk_vector" "jsonb" DEFAULT NULL::"jsonb", "p_prefer_online" boolean DEFAULT true, "p_strict_availability" boolean DEFAULT false) RETURNS TABLE("device_id" "uuid", "score" numeric)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_use_vector BOOLEAN;
    v_status_filter TEXT[];
BEGIN
    v_use_vector := p_chunk_vector IS NOT NULL AND jsonb_array_length(p_chunk_vector) > 0;

    IF p_prefer_online THEN
        v_status_filter := ARRAY['online', 'available', 'busy'];
    ELSE
        v_status_filter := ARRAY['available', 'busy'];
    END IF;

    IF p_strict_availability THEN
        v_status_filter := ARRAY['available'];
    END IF;

    RETURN QUERY
    SELECT d.id, COALESCE(
        CASE
            WHEN v_use_vector THEN
                1.0 - (d.embedding <=> (p_chunk_vector::vector))
            ELSE
                NULL
        END,
        CASE
            WHEN d.status = 'available' THEN 1.0
            WHEN d.status = 'busy' THEN 0.7
            WHEN d.status = 'online' THEN 0.5
            ELSE 0.0
        END
    ) AS score
    FROM devices d
    WHERE d.org_id = p_org_id
        AND d.status = ANY(v_status_filter)
        AND d.deleted_at IS NULL
    ORDER BY score DESC
    LIMIT 1;
END;
$$;


ALTER FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "jsonb", "p_prefer_online" boolean, "p_strict_availability" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."sentra_rls_table_status"() RETURNS TABLE("schema" "text", "table_name" "text", "rls_enabled" boolean, "rls_forced" boolean)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  RETURN QUERY SELECT * FROM _internal.sentra_rls_table_status();
END;
$$;


ALTER FUNCTION "public"."sentra_rls_table_status"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_alert_rules_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := public.get_current_org_id();
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_alert_rules_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_default_storage_config"("p_org_id" "uuid", "p_config_id" "uuid") RETURNS TABLE("updated_id" "uuid")
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- Lock all configs for this org (prevents race conditions)
  PERFORM 1
  FROM org_storage_configs
  WHERE org_id = p_org_id
  FOR UPDATE;

  -- Validate config belongs to org
  IF NOT EXISTS (
    SELECT 1
    FROM org_storage_configs
    WHERE id = p_config_id
      AND org_id = p_org_id
  ) THEN
    RAISE EXCEPTION 'Invalid config_id for this org';
  END IF;

  -- Single atomic update: exactly one default
  RETURN QUERY
  UPDATE org_storage_configs
  SET is_default = (id = p_config_id),
      updated_at = now()
  WHERE org_id = p_org_id
  RETURNING id;

END;
$$;


ALTER FUNCTION "public"."set_default_storage_config"("p_org_id" "uuid", "p_config_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_device_vector"("p_device_id" "uuid", "p_vec_literal" "text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  UPDATE public.device_vectors AS v
  SET profile_vector = p_vec_literal::vector,
      last_updated   = now()
  WHERE v.device_id = p_device_id;
END;
$$;


ALTER FUNCTION "public"."set_device_vector"("p_device_id" "uuid", "p_vec_literal" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_enterprise_integrations_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := public.get_current_org_id();
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_enterprise_integrations_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_execution_policies_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := public.get_current_org_id();
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_execution_policies_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_http_queue_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := public.get_current_org_id();
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_http_queue_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_job_notification_queue_org_id"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  UPDATE job_notification_queue j
  SET org_id = COALESCE(
    (SELECT org_id FROM agent_jobs WHERE id = j.job_id),
    (SELECT d.org_id FROM devices d WHERE d.id = (SELECT device_id FROM leases WHERE job_id = j.job_id LIMIT 1))
  )
  WHERE j.org_id IS NULL;
END;
$$;


ALTER FUNCTION "public"."set_job_notification_queue_org_id"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_org_id_from_record"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_org_id TEXT;
BEGIN
    -- Try to get org_id from various sources based on table
    IF TG_TABLE_NAME = 'agent_jobs' AND NEW.org_id IS NULL THEN
        SELECT org_id INTO v_org_id FROM public.datasets WHERE id = NEW.dataset_id;
        NEW.org_id := v_org_id;
    ELSIF TG_TABLE_NAME = 'batch_chunks' AND NEW.org_id IS NULL THEN
        SELECT org_id INTO v_org_id FROM public.datasets WHERE id = NEW.dataset_id;
        NEW.org_id := v_org_id;
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_org_id_from_record"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_plugin_execution_history_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := COALESCE(
      (SELECT d.org_id FROM devices d WHERE d.id = NEW.device_id),
      (SELECT op.org_id FROM org_plugins op WHERE op.plugin_id = NEW.plugin_id)
    );
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_plugin_execution_history_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := public.get_current_org_id();
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_runtime_environments_org_id"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  UPDATE runtime_environments r
  SET org_id = COALESCE(
    (SELECT d.org_id FROM devices d WHERE d.id = r.device_id),
    (SELECT om.org_id FROM org_members om WHERE om.user_id = auth.uid() LIMIT 1)
  )
  WHERE r.org_id IS NULL;
END;
$$;


ALTER FUNCTION "public"."set_runtime_environments_org_id"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_runtime_environments_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := COALESCE(
      (SELECT d.org_id FROM devices d WHERE d.id = NEW.device_id),
      public.get_current_org_id()
    );
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_runtime_environments_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_vector_datasets_org_id_trigger"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.org_id IS NULL THEN
    NEW.org_id := COALESCE(
      public.get_current_org_id(),
      (SELECT org_id FROM user_orgs WHERE user_id = auth.uid() LIMIT 1)
    );
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."set_vector_datasets_org_id_trigger"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."should_run_plugin"("p_device_id" "uuid", "p_rollout_percentage" integer) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  v_device_hash INTEGER;
BEGIN
  -- If rollout is 100%, always run
  IF p_rollout_percentage >= 100 THEN
    RETURN TRUE;
  END IF;

  -- If rollout is 0%, never run
  IF p_rollout_percentage <= 0 THEN
    RETURN FALSE;
  END IF;

  -- Use abs() to fix modulo bias on negative hash values
  v_device_hash := abs(('x' || substr(md5(p_device_id::text), 1, 8))::bit(32)::integer);

  RETURN (v_device_hash % 100) < p_rollout_percentage;
END;
$$;


ALTER FUNCTION "public"."should_run_plugin"("p_device_id" "uuid", "p_rollout_percentage" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF p_rollout_percentage >= 100 THEN
    RETURN true;
  END IF;
  
  IF p_rollout_percentage <= 0 THEN
    RETURN false;
  END IF;
  
  RETURN (abs(md5(p_device_id::text)::bigint % 100) < p_rollout_percentage);
END;
$$;


ALTER FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) IS 'Determines if a device should run a plugin based on rollout percentage using MD5 hash';



CREATE OR REPLACE FUNCTION "public"."start_job"("p_job_id" "uuid", "p_agent_id" "uuid" DEFAULT NULL::"uuid") RETURNS json
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE v_job agent_jobs%ROWTYPE;
BEGIN
  UPDATE agent_jobs aj
  SET status = 'running', started_at = NOW(), agent_id = p_agent_id
  WHERE aj.id = p_job_id AND aj.status = 'pending'
  RETURNING * INTO v_job;
  
  IF v_job.id IS NULL THEN
    RETURN json_build_object('ok', false, 'error', 'Job not found');
  END IF;
  
  RETURN json_build_object('ok', true, 'job_id', v_job.id, 'status', v_job.status);
END; $$;


ALTER FUNCTION "public"."start_job"("p_job_id" "uuid", "p_agent_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."store_s3_credentials_to_vault"("p_org_id" "uuid", "p_access_key_id" "text", "p_secret_access_key" "text", "p_provider" "text", "p_secret_name" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_secret_name TEXT;
    v_credentials JSONB;
BEGIN
    -- Generate safe secret name (no decimals)
    v_secret_name := COALESCE(
        p_secret_name,
        'org_' || p_org_id::TEXT || '_' || p_provider || '_creds_' ||
        FLOOR(EXTRACT(EPOCH FROM NOW()))
    );

    -- Build credentials JSON
    v_credentials := jsonb_build_object(
        'access_key_id', p_access_key_id,
        'secret_access_key', p_secret_access_key,
        'provider', p_provider,
        'created_at', NOW()
    );

    -- Store in Vault
    PERFORM vault.create_secret(
        v_credentials::text,
        v_secret_name,
        'S3 credentials for org ' || p_org_id::TEXT
    );

    -- Return only what matters
    RETURN jsonb_build_object(
        'secret_name', v_secret_name
    );

EXCEPTION WHEN OTHERS THEN
    -- Let error propagate to Supabase (DO NOT swallow)
    RAISE EXCEPTION 'Vault error: %', SQLERRM;
END;
$$;


ALTER FUNCTION "public"."store_s3_credentials_to_vault"("p_org_id" "uuid", "p_access_key_id" "text", "p_secret_access_key" "text", "p_provider" "text", "p_secret_name" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."store_plugin_signing_key_to_vault"("p_org_id" "uuid", "p_private_key_b64" "text", "p_secret_name" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_secret_name TEXT;
BEGIN
    v_secret_name := COALESCE(
        p_secret_name,
        'org_' || p_org_id::TEXT || '_ed25519_priv_' || FLOOR(EXTRACT(EPOCH FROM NOW()))
    );

    PERFORM vault.create_secret(
        p_private_key_b64,
        v_secret_name,
        'Ed25519 private key for org ' || p_org_id::TEXT
    );

    RETURN jsonb_build_object('secret_name', v_secret_name);
END;
$$;

ALTER FUNCTION "public"."store_plugin_signing_key_to_vault"("p_org_id" "uuid", "p_private_key_b64" "text", "p_secret_name" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."system_health_heartbeat"() RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    INSERT INTO public.system_logs (level, source, message, created_at)
    VALUES ('info', 'cron', 'System health check heartbeat', NOW());
END;
$$;


ALTER FUNCTION "public"."system_health_heartbeat"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer DEFAULT 10, "_recovery_timeout" integer DEFAULT 30, "_max_retries" integer DEFAULT 5) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  now_ts timestamptz := now();
  device_rec RECORD;
  stale_count int := 0;
  recovered jsonb;
  assign_results jsonb := '[]'::jsonb;
  org_row RECORD;
BEGIN
  -- mark stale devices offline
  FOR device_rec IN
    SELECT id, name, status, last_heartbeat
    FROM public.devices
    WHERE status = 'online'
      AND (last_heartbeat IS NULL OR last_heartbeat < now_ts - (_stale_device_minutes || ' minutes')::interval)
  LOOP
    UPDATE public.devices
    SET status = 'offline', updated_at = now_ts
    WHERE id = device_rec.id;

    INSERT INTO public.system_logs(event_type, message, created_at)
    VALUES ('device_stale', format('Device %s (%s) marked offline', device_rec.id::text, coalesce(device_rec.name,'unknown')), now_ts);

    stale_count := stale_count + 1;
  END LOOP;

  recovered := public.recover_stuck_jobs(_recovery_timeout, _max_retries);

  FOR org_row IN
    SELECT DISTINCT org_id FROM public.agent_jobs WHERE status = 'pending'
  LOOP
    BEGIN
      assign_results := assign_results || jsonb_build_array(jsonb_build_object('org_id', org_row.org_id, 'result', public.assign_agent_job(org_row.org_id)));
    EXCEPTION WHEN OTHERS THEN
      assign_results := assign_results || jsonb_build_array(jsonb_build_object('org_id', org_row.org_id, 'error', SQLERRM));
    END;
  END LOOP;

  RETURN jsonb_build_object('ok', true, 'stale_devices_marked_offline', stale_count, 'recover_summary', recovered, 'assign_attempts', assign_results, 'heartbeat_at', now_ts);
EXCEPTION WHEN OTHERS THEN
  PERFORM public.log_agent_error(NULL::uuid, NULL::uuid, 'system_health_heartbeat failed: ' || SQLERRM);
  RETURN jsonb_build_object('ok', false, 'error', SQLERRM);
END;
$$;


ALTER FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer, "_recovery_timeout" integer, "_max_retries" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer, "_recovery_timeout" integer, "_max_retries" integer) IS 'Heartbeat: mark stale devices offline, recover stuck jobs, try assign per org.';



CREATE OR REPLACE FUNCTION "public"."test_case_1_device_available"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_test_org UUID := gen_random_uuid();
  v_test_device UUID := gen_random_uuid();
  v_job_id UUID;
  v_result BOOLEAN := TRUE;
  v_details TEXT;
BEGIN
  -- Setup: Create test device
  INSERT INTO public.devices (id, org_id, status, merge_capable, active_jobs, max_concurrency)
  VALUES (v_test_device, v_test_org, 'online', true, 0, 3);

  -- Insert dataset (should trigger immediate assignment)
  INSERT INTO public.datasets (id, org_id, name, status)
  VALUES (gen_random_uuid(), v_test_org, 'test_dataset_device_available', 'registered');

  -- Verify job was inserted as assigned
  SELECT id INTO v_job_id
  FROM public.agent_jobs
  WHERE org_id = v_test_org
    AND job_type = 'scan_dataset'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_job_id IS NULL THEN
    RETURN QUERY SELECT 'test_case_1_device_available'::TEXT, FALSE::BOOLEAN, 
           'No job created'::TEXT;
    RETURN;
  END IF;

  -- Check job state
  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = v_job_id
      AND status = 'assigned'
      AND agent_id = v_test_device
      AND assigned_at IS NOT NULL
      AND lease_expires_at IS NOT NULL
  ) THEN
    RETURN QUERY SELECT 'test_case_1_device_available'::TEXT, FALSE::BOOLEAN,
           'Job not assigned correctly'::TEXT;
    RETURN;
  END IF;

  -- Check notification queue
  IF NOT EXISTS (
    SELECT 1 FROM public.job_notification_queue
    WHERE job_id = v_job_id AND agent_id = v_test_device
  ) THEN
    RETURN QUERY SELECT 'test_case_1_device_available'::TEXT, FALSE::BOOLEAN,
           'Notification not queued'::TEXT;
    RETURN;
  END IF;

  -- Cleanup
  DELETE FROM public.agent_jobs WHERE id = v_job_id;
  DELETE FROM public.devices WHERE id = v_test_device;

  RETURN QUERY SELECT 'test_case_1_device_available'::TEXT, TRUE::BOOLEAN,
         'Device available: job assigned immediately'::TEXT;
END;
$$;


ALTER FUNCTION "public"."test_case_1_device_available"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."test_case_2_no_device"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_test_org UUID := gen_random_uuid();
  v_job_id UUID;
BEGIN
  -- No devices in this org

  -- Insert dataset
  INSERT INTO public.datasets (id, org_id, name, status)
  VALUES (gen_random_uuid(), v_test_org, 'test_dataset_no_device', 'registered');

  -- Verify job was inserted as pending
  SELECT id INTO v_job_id
  FROM public.agent_jobs
  WHERE org_id = v_test_org
    AND job_type = 'scan_dataset'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_job_id IS NULL THEN
    RETURN QUERY SELECT 'test_case_2_no_device'::TEXT, FALSE::BOOLEAN,
           'No job created'::TEXT;
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = v_job_id
      AND status = 'pending'
      AND agent_id IS NULL
  ) THEN
    RETURN QUERY SELECT 'test_case_2_no_device'::TEXT, FALSE::BOOLEAN,
           'Job should be pending with no device'::TEXT;
    RETURN;
  END IF;

  -- Cleanup
  DELETE FROM public.agent_jobs WHERE id = v_job_id;

  RETURN QUERY SELECT 'test_case_2_no_device'::TEXT, TRUE::BOOLEAN,
         'No device: job queued as pending'::TEXT;
END;
$$;


ALTER FUNCTION "public"."test_case_2_no_device"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."test_case_3_multiple_chunks"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_test_org UUID := gen_random_uuid();
  v_test_device1 UUID := gen_random_uuid();
  v_test_device2 UUID := gen_random_uuid();
  v_chunk1 UUID := gen_random_uuid();
  v_chunk2 UUID := gen_random_uuid();
  v_job_count INT;
BEGIN
  -- Setup: Create test devices
  INSERT INTO public.devices (id, org_id, status, merge_capable, active_jobs, max_concurrency)
  VALUES 
    (v_test_device1, v_test_org, 'online', true, 0, 3),
    (v_test_device2, v_test_org, 'online', true, 0, 3);

  -- Insert multiple chunks
  INSERT INTO public.batch_chunks (id, org_id, status, chunk_index)
  VALUES 
    (v_chunk1, v_test_org, 'pending', 0),
    (v_chunk2, v_test_org, 'pending', 1);

  -- Verify multiple jobs created
  SELECT COUNT(*) INTO v_job_count
  FROM public.agent_jobs
  WHERE org_id = v_test_org
    AND job_type = 'preprocess'
    AND payload->>'chunk_id' IN (v_chunk1::TEXT, v_chunk2::TEXT);

  IF v_job_count < 2 THEN
    RETURN QUERY SELECT 'test_case_3_multiple_chunks'::TEXT, FALSE::BOOLEAN,
           'Expected 2 jobs, got ' || v_job_count::TEXT;
    RETURN;
  END IF;

  -- Verify payload contains chunk_id
  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE payload->>'chunk_id' = v_chunk1::TEXT
      AND payload->>'chunk_index' = '0'
  ) THEN
    RETURN QUERY SELECT 'test_case_3_multiple_chunks'::TEXT, FALSE::BOOLEAN,
           'Payload missing chunk_id'::TEXT;
    RETURN;
  END IF;

  -- Cleanup
  DELETE FROM public.agent_jobs WHERE org_id = v_test_org;
  DELETE FROM public.devices WHERE id IN (v_test_device1, v_test_device2);

  RETURN QUERY SELECT 'test_case_3_multiple_chunks'::TEXT, TRUE::BOOLEAN,
         'Multiple chunks: each assigned independently'::TEXT;
END;
$$;


ALTER FUNCTION "public"."test_case_3_multiple_chunks"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."test_case_4_state_machine"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_job_id UUID := gen_random_uuid();
  v_test_org UUID := gen_random_uuid();
  v_test_device UUID := gen_random_uuid();
BEGIN
  -- Setup
  INSERT INTO public.devices (id, org_id, status, merge_capable)
  VALUES (v_test_device, v_test_org, 'online', true);

  -- Test: Insert job directly as 'assigned' (NULL → assigned)
  BEGIN
    INSERT INTO public.agent_jobs (id, org_id, job_type, status, agent_id, assigned_at)
    VALUES (v_job_id, v_test_org, 'test', 'assigned', v_test_device, NOW());
  EXCEPTION WHEN OTHERS THEN
    -- Cleanup
    DELETE FROM public.agent_jobs WHERE id = v_job_id;
    DELETE FROM public.devices WHERE id = v_test_device;
    
    RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, FALSE::BOOLEAN,
           'NULL→assigned failed: ' || SQLERRM;
    RETURN;
  END;

  -- Verify inserted correctly
  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = v_job_id AND status = 'assigned'
  ) THEN
    DELETE FROM public.agent_jobs WHERE id = v_job_id;
    DELETE FROM public.devices WHERE id = v_test_device;
    
    RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, FALSE::BOOLEAN,
           'Job not in assigned state';
    RETURN;
  END IF;

  -- Test: assigned → running should succeed (via trigger)
  UPDATE public.agent_jobs
  SET status = 'running'
  WHERE id = v_job_id;

  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = v_job_id AND status = 'running'
  ) THEN
    DELETE FROM public.agent_jobs WHERE id = v_job_id;
    DELETE FROM public.devices WHERE id = v_test_device;
    
    RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, FALSE::BOOLEAN,
           'assigned→running failed';
    RETURN;
  END IF;

  -- Test: running → completed should succeed
  UPDATE public.agent_jobs
  SET status = 'completed', finished_at = NOW()
  WHERE id = v_job_id;

  IF NOT EXISTS (
    SELECT 1 FROM public.agent_jobs
    WHERE id = v_job_id AND status = 'completed'
  ) THEN
    DELETE FROM public.agent_jobs WHERE id = v_job_id;
    DELETE FROM public.devices WHERE id = v_test_device;
    
    RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, FALSE::BOOLEAN,
           'running→completed failed';
    RETURN;
  END IF;

  -- Test: completed → pending should FAIL (terminal state)
  BEGIN
    UPDATE public.agent_jobs
    SET status = 'pending'
    WHERE id = v_job_id;
    
    -- If we get here, the update succeeded (bad)
    DELETE FROM public.agent_jobs WHERE id = v_job_id;
    DELETE FROM public.devices WHERE id = v_test_device;
    
    RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, FALSE::BOOLEAN,
           'Terminal state transition not blocked';
    RETURN;
  EXCEPTION WHEN OTHERS THEN
    -- Expected - terminal state should be blocked
  END;

  -- Cleanup
  DELETE FROM public.agent_jobs WHERE id = v_job_id;
  DELETE FROM public.devices WHERE id = v_test_device;

  RETURN QUERY SELECT 'test_case_4_state_machine'::TEXT, TRUE::BOOLEAN,
         'State machine: NULL→assigned works, invalid blocked'::TEXT;
END;
$$;


ALTER FUNCTION "public"."test_case_4_state_machine"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."test_case_5_no_duplicates"() RETURNS TABLE("test_name" "text", "passed" boolean, "details" "text")
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_test_org UUID := gen_random_uuid();
  v_test_device UUID := gen_random_uuid();
  v_chunk_id UUID := gen_random_uuid();
  v_job_count INT;
BEGIN
  -- Setup
  INSERT INTO public.devices (id, org_id, status, merge_capable)
  VALUES (v_test_device, v_test_org, 'online', true);

  -- Insert chunk
  INSERT INTO public.batch_chunks (id, org_id, status, chunk_index)
  VALUES (v_chunk_id, v_test_org, 'pending', 0);

  -- Count jobs for this chunk
  SELECT COUNT(*) INTO v_job_count
  FROM public.agent_jobs
  WHERE payload->>'chunk_id' = v_chunk_id::TEXT;

  IF v_job_count != 1 THEN
    DELETE FROM public.batch_chunks WHERE id = v_chunk_id;
    DELETE FROM public.agent_jobs WHERE payload->>'chunk_id' = v_chunk_id::TEXT;
    DELETE FROM public.devices WHERE id = v_test_device;

    RETURN QUERY SELECT 'test_case_5_no_duplicates'::TEXT, FALSE::BOOLEAN,
           'Expected 1 job, got ' || v_job_count::TEXT;
    RETURN;
  END IF;

  -- Cleanup
  DELETE FROM public.batch_chunks WHERE id = v_chunk_id;
  DELETE FROM public.agent_jobs WHERE payload->>'chunk_id' = v_chunk_id::TEXT;
  DELETE FROM public.devices WHERE id = v_test_device;

  RETURN QUERY SELECT 'test_case_5_no_duplicates'::TEXT, TRUE::BOOLEAN,
         'No duplicate jobs created'::TEXT;
END;
$$;


ALTER FUNCTION "public"."test_case_5_no_duplicates"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."touch_device_vector"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  update public.device_vectors
  set last_updated = now()
  where device_id = new.device_id;
  return new;
end;
$$;


ALTER FUNCTION "public"."touch_device_vector"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."trg_cleanup_leases_on_offline"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    -- Clean ONLY when transitioning TO offline (not already offline)
    -- OLD.status IS NOT NULL ensures this isn't new insert
    IF OLD.status IS NOT NULL 
       AND OLD.status != 'offline' 
       AND NEW.status = 'offline' THEN
        
        -- Release claimed jobs for this device
        UPDATE agent_jobs
        SET status = 'pending',
            agent_id = NULL,
            assigned_at = NULL,
            lease_expires_at = NULL,
            retry_count = COALESCE(retry_count, 0) + 1,
            last_error = 'device went offline - job released',
            updated_at = NOW()
        WHERE agent_id = NEW.id
          AND status IN ('claimed', 'assigned');
        
        -- Also update leases table
        UPDATE leases
        SET status = 'cancelled'
        WHERE device_id = NEW.id
          AND status = 'active';
    END IF;
    
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."trg_cleanup_leases_on_offline"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_dataset_merge_metadata"("p_dataset_id" "uuid", "p_merge_time_ms" double precision DEFAULT NULL::double precision, "p_merged_size_gb" double precision DEFAULT NULL::double precision, "p_verified" boolean DEFAULT false) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    UPDATE public.datasets
    SET 
        status = 'merged',
        merged_at = NOW(),
        merged_size_gb = p_merged_size_gb,
        merge_time_ms = p_merge_time_ms,
        merged_output_verified = p_verified,
        updated_at = NOW()
    WHERE id = p_dataset_id;
END;
$$;


ALTER FUNCTION "public"."update_dataset_merge_metadata"("p_dataset_id" "uuid", "p_merge_time_ms" double precision, "p_merged_size_gb" double precision, "p_verified" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_dataset_scan"("p_dataset_id" "uuid", "p_scan_metadata" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_updated_rows INT;
BEGIN
    UPDATE public.datasets
    SET 
        status = 'scanned',
        scanned_at = NOW(),
        updated_at = NOW(),
        metadata = COALESCE(p_scan_metadata, metadata)
    WHERE id = p_dataset_id
    AND status = 'scanning'
    RETURNING 1 INTO v_updated_rows;
    
    IF v_updated_rows > 0 THEN
        RETURN jsonb_build_object(
            'ok', true,
            'dataset_id', p_dataset_id,
            'status', 'scanned'
        );
    ELSE
        RETURN jsonb_build_object(
            'ok', false,
            'error', 'dataset not found or not in scanning state'
        );
    END IF;
END;
$$;


ALTER FUNCTION "public"."update_dataset_scan"("p_dataset_id" "uuid", "p_scan_metadata" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_dataset_status_on_merge_complete"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
  IF NEW.status = 'completed' 
     AND OLD.status IS DISTINCT FROM 'completed' 
     AND NEW.job_type = 'merge_dataset' THEN
    UPDATE public.datasets
    SET status = 'merged',
        merged_at = NOW(),
        updated_at = NOW()
    WHERE job_dataset_id = NEW.job_dataset_id;
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_dataset_status_on_merge_complete"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_dataset_status_on_scan_complete"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF NEW.scan_status = 'complete' AND OLD.scan_status != 'complete' THEN
        UPDATE public.datasets SET status = 'scan_complete' WHERE id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_dataset_status_on_scan_complete"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_dataset_storage_type"("p_dataset_id" "uuid", "p_storage_type" "text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN
    UPDATE datasets 
    SET storage_type = p_storage_type, 
        updated_at = now() 
    WHERE id = p_dataset_id;
END;
$$;


ALTER FUNCTION "public"."update_dataset_storage_type"("p_dataset_id" "uuid", "p_storage_type" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_status" "text", "p_duration_ms" bigint) RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    -- Insert with dedup: only insert if no similar record in last 60 seconds
    INSERT INTO device_benchmarks (device_id, org_id, test_name, status, duration_ms, created_at)
    SELECT p_device_id, p_org_id, 'job_completion_' || p_job_type, p_status, p_duration_ms, NOW()
    WHERE NOT EXISTS (
        SELECT 1 FROM device_benchmarks
        WHERE device_id = p_device_id
          AND test_name = 'job_completion_' || p_job_type
          AND created_at > NOW() - INTERVAL '60 seconds'
    );
END;
$$;


ALTER FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_status" "text", "p_duration_ms" bigint) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
    v_test_name text := 'job_completion_' || p_job_type;
BEGIN
    -- Update device_job_type_stats (upsert - this works correctly)
    INSERT INTO device_job_type_stats (
        device_id, org_id, job_type, 
        avg_duration_ms, avg_throughput, 
        job_count, success_count, last_updated
    )
    VALUES (
        p_device_id, p_org_id, p_job_type,
        COALESCE(p_duration_ms, 0), 
        COALESCE(p_throughput, 0),
        1, 
        CASE WHEN p_success THEN 1 ELSE 0 END,
        now()
    )
    ON CONFLICT (device_id, job_type) DO UPDATE SET
        avg_duration_ms = COALESCE(
            (device_job_type_stats.avg_duration_ms * device_job_type_stats.job_count + COALESCE(p_duration_ms, 0))
            / NULLIF(device_job_type_stats.job_count + 1, 0),
            COALESCE(p_duration_ms, 0)
        ),
        avg_throughput = COALESCE(
            (device_job_type_stats.avg_throughput * device_job_type_stats.job_count + COALESCE(p_throughput, 0))
            / NULLIF(device_job_type_stats.job_count + 1, 0),
            COALESCE(p_throughput, 0)
        ),
        job_count = device_job_type_stats.job_count + 1,
        success_count = device_job_type_stats.success_count + CASE WHEN p_success THEN 1 ELSE 0 END,
        last_updated = now();
    
    -- Insert benchmark WITH DEDUP - only if not exists in last 60 seconds
    INSERT INTO device_benchmarks (
        device_id, org_id, test_name,
        latency_ms, throughput,
        created_at, updated_at
    )
    SELECT p_device_id, p_org_id, v_test_name, p_duration_ms, p_throughput, now(), now()
    WHERE NOT EXISTS (
        SELECT 1 FROM device_benchmarks
        WHERE device_id = p_device_id 
          AND test_name = v_test_name
          AND created_at > NOW() - INTERVAL '60 seconds'
    );

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'update_device_job_stats error: %', SQLERRM;
END;
$$;


ALTER FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_device_job_type_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE v_existing record;
BEGIN
    SELECT * INTO v_existing FROM device_job_type_stats WHERE device_id = p_device_id AND job_type = p_job_type;
    IF FOUND THEN
        UPDATE device_job_type_stats SET
            avg_duration_ms = COALESCE(v_existing.avg_duration_ms, p_duration_ms) * 0.7 + p_duration_ms * 0.3,
            avg_throughput = COALESCE(v_existing.avg_throughput, p_throughput) * 0.7 + p_throughput * 0.3,
            job_count = v_existing.job_count + 1,
            success_count = v_existing.success_count + CASE WHEN p_success THEN 1 ELSE 0 END,
            last_updated = NOW()
        WHERE device_id = p_device_id AND job_type = p_job_type;
    ELSE
        INSERT INTO device_job_type_stats (device_id, org_id, job_type, avg_duration_ms, avg_throughput, job_count, success_count)
        VALUES (p_device_id, p_org_id, p_job_type, p_duration_ms, p_throughput, 1, CASE WHEN p_success THEN 1 ELSE 0 END);
    END IF;
END;
$$;


ALTER FUNCTION "public"."update_device_job_type_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_device_vector"("device_id" "uuid", "new_vec" "text") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
begin
  -- Safely convert the JSON array text '[0.1,0.2,...]' into a pgvector
  update public.device_vectors
  set profile_vector = vector_from_json(new_vec::jsonb),
      last_updated   = now()
  where device_id = update_device_vector.device_id;
end;
$$;


ALTER FUNCTION "public"."update_device_vector"("device_id" "uuid", "new_vec" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    UPDATE dataset_merge_locks
    SET heartbeat_at = now(),
        expires_at = now() + (30 || ' minutes')::INTERVAL,
        updated_at = now()
    WHERE id = p_lock_id
    AND status = 'active';
    
    RETURN FOUND;
END;
$$;


ALTER FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid", "p_org_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  v_lock_org uuid;
BEGIN
  SELECT d.org_id INTO v_lock_org
  FROM dataset_merge_locks l JOIN public.devices d ON d.id = l.device_id
  WHERE l.id = p_lock_id;
  IF NOT FOUND THEN RETURN false; END IF;
  IF v_lock_org != p_org_id THEN RAISE EXCEPTION 'org_id mismatch'; END IF;
  UPDATE dataset_merge_locks SET heartbeat_at = now(), expires_at = now() + INTERVAL '30 minutes', updated_at = now()
  WHERE id = p_lock_id AND status = 'active';
  RETURN FOUND;
END;
$$;


ALTER FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid", "p_org_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_org_storage_configs_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_org_storage_configs_updated_at"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_pipeline_timestamp"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_pipeline_timestamp"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_vector_dataset_count"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  update vector_datasets
  set total_vectors = total_vectors + 1
  where id = new.dataset_id;
  return new;
end;
$$;


ALTER FUNCTION "public"."update_vector_dataset_count"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."validate_claim_secret"("p_claim" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$DECLARE
  v_org_id     uuid;
  v_used_count integer;
  v_max_uses   integer;
BEGIN
  SELECT id, claim_secret_used_count, COALESCE(claim_secret_max_uses, 1)
  INTO v_org_id, v_used_count, v_max_uses
  FROM public.orgs
  WHERE claim_secret = p_claim
    AND (claim_secret_expires_at IS NULL OR claim_secret_expires_at > now())
  FOR UPDATE OF orgs;

  IF v_org_id IS NULL THEN
    RAISE EXCEPTION 'Invalid or expired claim code';
  END IF;

  IF v_used_count >= v_max_uses THEN
    RAISE EXCEPTION 'Claim code has reached its maximum use count (%)', v_max_uses;
  END IF;

  UPDATE public.orgs
  SET claim_secret_used_count = claim_secret_used_count + 1,
      claim_secret_expires_at = CASE
        WHEN (claim_secret_used_count + 1) >= v_max_uses THEN now() - INTERVAL '1 second'
        ELSE claim_secret_expires_at
      END
  WHERE id = v_org_id
    AND claim_secret_used_count = v_used_count
    AND (claim_secret_expires_at IS NULL OR claim_secret_expires_at > now());

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Claim code validation failed - concurrent modification detected';
  END IF;

  RETURN v_org_id;
END;$$;


ALTER FUNCTION "public"."validate_claim_secret"("p_claim" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."validate_plugin_config"("p_config" "jsonb", "p_config_schema" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE
    AS $$
DECLARE
    v_errors jsonb := '[]'::jsonb;
    v_field text;
    v_schema_field jsonb;
    v_value jsonb;
    v_required text[];
BEGIN
    IF p_config_schema ? 'required' THEN
        v_required := p_config_schema->'required';
        FOREACH v_field IN ARRAY v_required LOOP
            IF NOT (p_config ? v_field) OR p_config->v_field IS NULL THEN
                v_errors := v_errors || jsonb_build_object('field', v_field, 'error', 'required field is missing');
            END IF;
        END LOOP;
    END IF;

    IF p_config_schema ? 'properties' THEN
        FOR v_field, v_schema_field IN SELECT key, value FROM jsonb_each_text(p_config_schema->'properties') LOOP
            v_value := p_config->v_field;
            IF v_value IS NOT NULL AND p_config_schema->'properties'->v_field ? 'type' THEN
                DECLARE
                    v_type text;
                BEGIN
                    v_type := p_config_schema->'properties'->v_field->>'type';
                    IF v_type = 'integer' AND jsonb_typeof(v_value) != 'number' THEN
                        v_errors := v_errors || jsonb_build_object('field', v_field, 'error', 'must be integer');
                    ELSIF v_type = 'string' AND jsonb_typeof(v_value) != 'string' THEN
                        v_errors := v_errors || jsonb_build_object('field', v_field, 'error', 'must be string');
                    ELSIF v_type = 'boolean' AND jsonb_typeof(v_value) != 'boolean' THEN
                        v_errors := v_errors || jsonb_build_object('field', v_field, 'error', 'must be boolean');
                    END IF;
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
            END IF;
        END LOOP;
    END IF;

    RETURN jsonb_build_object('valid', jsonb_array_length(v_errors) = 0, 'errors', v_errors);
END;
$$;


ALTER FUNCTION "public"."validate_plugin_config"("p_config" "jsonb", "p_config_schema" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."verify_job_lease"("p_job_id" "text", "p_device_id" "text") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_job UUID;
    v_device UUID;
BEGIN
    v_job := p_job_id::UUID;
    v_device := p_device_id::UUID;

    RETURN EXISTS (
        SELECT 1 FROM agent_jobs j
        WHERE j.id = v_job
            AND j.agent_id = v_device
            AND j.status IN ('assigned', 'running')
            AND j.lease_expires_at > NOW()
    );
END;
$$;


ALTER FUNCTION "public"."verify_job_lease"("p_job_id" "text", "p_device_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."verify_job_lease"("p_job_id" "uuid", "p_device_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM leases
        WHERE job_id = p_job_id
          AND device_id = p_device_id
          AND (lease_expires_at IS NULL OR lease_expires_at > NOW())
    ) INTO v_exists;
    
    RETURN COALESCE(v_exists, FALSE);
END;
$$;


ALTER FUNCTION "public"."verify_job_lease"("p_job_id" "uuid", "p_device_id" "uuid") OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."agent_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "agent_id" "uuid",
    "job_type" "text" NOT NULL,
    "payload" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "completed" boolean DEFAULT false,
    "error" "text",
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "org_id" "uuid" NOT NULL,
    "assigned_at" timestamp with time zone,
    "duration_ms" double precision,
    "throughput" double precision,
    "output_token" "text",
    "plugin_id" "text",
    "plugin_version" "text",
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "retry_count" integer DEFAULT 0,
    "lease_expires_at" timestamp with time zone,
    "last_error" "text",
    "dead_lettered" boolean DEFAULT false,
    "execution_id" "uuid",
    "execution_step_id" "uuid",
    "max_retries" integer DEFAULT 3,
    "job_dataset_id" "uuid" GENERATED ALWAYS AS ((("payload" ->> 'dataset_id'::"text"))::"uuid") STORED,
    "job_chunk_id" "uuid" GENERATED ALWAYS AS ((("payload" ->> 'chunk_id'::"text"))::"uuid") STORED,
    "last_transition_at" timestamp with time zone DEFAULT "now"(),
    "runtime_type" "text" DEFAULT 'python'::"text",
    "runtime_dependencies" "jsonb" DEFAULT '{}'::"jsonb",
    "entrypoint" "text",
    "execution_mode" "text" DEFAULT 'native'::"text",
    "environment_strict" boolean DEFAULT false,
    "execution_timeout_seconds" integer DEFAULT 300,
    "dependency_lock_hash" "text",
    "idempotency_key" "text",
    "checkpoint_id" "uuid",
    "environment_id" "uuid",
    "run_id" "uuid" DEFAULT "gen_random_uuid"(),
    "attempt_number" integer DEFAULT 1,
    "output_data" "jsonb",
    "output_size_bytes" bigint,
    "log_size_bytes" bigint,
    "failure_classification" "text",
    "actual_execution_mode" "text",
    "fallback_reason" "text",
    "heartbeat_at" timestamp with time zone,
    CONSTRAINT "agent_jobs_assignment_valid" CHECK ((("assigned_at" IS NULL) OR ("agent_id" IS NOT NULL))),
    CONSTRAINT "agent_jobs_execution_mode_check" CHECK (("execution_mode" = ANY (ARRAY['docker'::"text", 'runtime'::"text", 'native'::"text"]))),
    CONSTRAINT "agent_jobs_failure_classification_check" CHECK (("failure_classification" = ANY (ARRAY['infra_error'::"text", 'dependency_error'::"text", 'user_code_error'::"text", 'timeout_error'::"text", 'memory_error'::"text", 'unknown'::"text"]))),
    CONSTRAINT "agent_jobs_job_type_check" CHECK (("job_type" = ANY (ARRAY['scan'::"text", 'scan_dataset'::"text", 'preprocess'::"text", 'process'::"text", 'process_dataset'::"text", 'merge'::"text", 'merge_dataset'::"text", 'validate'::"text", 'export'::"text", 'import'::"text"]))),
    CONSTRAINT "agent_jobs_runtime_type_check" CHECK (("runtime_type" = ANY (ARRAY['python'::"text", 'node'::"text", 'native'::"text"]))),
    CONSTRAINT "agent_jobs_status_canonical" CHECK (("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text", 'completed'::"text", 'failed'::"text", 'dead'::"text"]))),
    CONSTRAINT "valid_job_type" CHECK (("job_type" = ANY (ARRAY['process'::"text", 'preprocess'::"text", 'scan'::"text", 'scan_dataset'::"text", 'merge'::"text", 'merge_dataset'::"text", 'unknown'::"text"])))
);

ALTER TABLE ONLY "public"."agent_jobs" FORCE ROW LEVEL SECURITY;


ALTER TABLE "public"."agent_jobs" OWNER TO "postgres";


COMMENT ON COLUMN "public"."agent_jobs"."runtime_type" IS 'Runtime type: python, node, or native. Read-only for org users, managed by service_role.';



COMMENT ON COLUMN "public"."agent_jobs"."runtime_dependencies" IS 'JSONB for runtime dependencies (pip packages, npm packages, etc.). Read-only for org users.';



COMMENT ON COLUMN "public"."agent_jobs"."execution_mode" IS 'Execution mode: docker, runtime, or native. Read-only for org users, managed by service_role.';



COMMENT ON COLUMN "public"."agent_jobs"."environment_strict" IS 'If true, restricts environment variables for sandboxed execution.';



CREATE TABLE IF NOT EXISTS "public"."agent_jobs_archive" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "agent_id" "uuid",
    "job_type" "text" NOT NULL,
    "payload" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "completed" boolean DEFAULT false,
    "error" "text",
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "org_id" "uuid",
    "assigned_at" timestamp with time zone,
    "duration_ms" double precision,
    "throughput" double precision,
    "output_token" "text",
    "plugin_id" "text",
    "plugin_version" "text",
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "retry_count" integer DEFAULT 0,
    "lease_expires_at" timestamp with time zone,
    "last_error" "text",
    "dead_lettered" boolean DEFAULT false,
    "execution_id" "uuid",
    "execution_step_id" "uuid",
    "processed_path" "text",
    CONSTRAINT "agent_jobs_archive_status_canonical" CHECK (("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text", 'completed'::"text", 'failed'::"text", 'dead'::"text"]))),
    CONSTRAINT "agent_jobs_assignment_valid" CHECK ((("assigned_at" IS NULL) OR ("agent_id" IS NOT NULL)))
);


ALTER TABLE "public"."agent_jobs_archive" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."agent_jobs_dead_letter" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "dataset_id" "uuid",
    "job_type" "text" NOT NULL,
    "payload" "jsonb",
    "retry_count" integer DEFAULT 0 NOT NULL,
    "last_error" "text",
    "original_job_id" "uuid",
    "failed_at" timestamp with time zone DEFAULT "now"(),
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."agent_jobs_dead_letter" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."agent_metrics" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid",
    "org_id" "uuid",
    "metrics" "jsonb" NOT NULL,
    "concurrency_returned" integer,
    "load_factor" numeric(4,3),
    "source" "text" DEFAULT 'agent_health_policy'::"text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."agent_metrics" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."agent_worker_activity" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid",
    "device_id" "uuid",
    "worker_id" integer,
    "job_type" "text",
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "duration_ms" integer,
    "status" "text",
    "error" "text"
);


ALTER TABLE "public"."agent_worker_activity" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."alert_history" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "alert_rule_id" "uuid",
    "org_id" "uuid",
    "triggered_at" timestamp with time zone DEFAULT "now"(),
    "condition_type" "text",
    "actual_value" numeric,
    "threshold_value" numeric,
    "notification_sent" boolean DEFAULT false,
    "notification_sent_at" timestamp with time zone
);


ALTER TABLE "public"."alert_history" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."alert_rules" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "condition_type" "text" NOT NULL,
    "threshold_value" numeric NOT NULL,
    "threshold_window_minutes" integer DEFAULT 5,
    "channel" "text" NOT NULL,
    "channel_config" "jsonb" DEFAULT '{}'::"jsonb",
    "enabled" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "created_by" "uuid",
    CONSTRAINT "alert_rules_channel_check" CHECK (("channel" = ANY (ARRAY['email'::"text", 'webhook'::"text", 'slack'::"text"]))),
    CONSTRAINT "alert_rules_condition_type_check" CHECK (("condition_type" = ANY (ARRAY['stuck_jobs'::"text", 'device_offline'::"text", 'job_failure_rate'::"text", 'merge_failure'::"text", 'pipeline_failed'::"text", 'device_error'::"text"])))
);


ALTER TABLE "public"."alert_rules" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."batch_chunks" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "batch_id" "uuid",
    "org_id" "uuid",
    "chunk_index" integer,
    "status" "text" DEFAULT 'pending'::"text",
    "embedding" "public"."vector"(384),
    "processed_at" timestamp with time zone,
    "metadata" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "job_type" "text" DEFAULT 'preprocess'::"text",
    "merged_in" boolean DEFAULT false,
    "similarity_score" double precision,
    "chunk_size_gb" double precision,
    "required_io" double precision,
    "parallel_ratio" double precision,
    "dynamic_size" boolean DEFAULT true,
    "type" "text" DEFAULT 'preprocess'::"text",
    "assigned_device_id" "uuid",
    "dataset_id" "uuid",
    "chunk_vector" "public"."vector"(16),
    "assigned_at" timestamp with time zone,
    CONSTRAINT "chk_batch_chunks_status" CHECK (("status" = ANY (ARRAY['pending'::"text", 'processing'::"text", 'processed'::"text", 'failed'::"text", 'skipped'::"text"])))
);


ALTER TABLE "public"."batch_chunks" OWNER TO "postgres";


COMMENT ON TABLE "public"."batch_chunks" IS 'Stores dataset chunks assigned to Kickin agents for processing.';



COMMENT ON COLUMN "public"."batch_chunks"."dynamic_size" IS 'When TRUE, chunk can be re-sized dynamically based on device capabilities';



CREATE TABLE IF NOT EXISTS "public"."bootstrap_rate_limits" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "client_key" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."bootstrap_rate_limits" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."chunk_complexity_cache" (
    "dataset_id" "uuid" NOT NULL,
    "chunk_id" "uuid" NOT NULL,
    "complexity_score" numeric(5,2),
    "last_used_at" timestamp with time zone
);


ALTER TABLE "public"."chunk_complexity_cache" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."chunk_profiles" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "chunk_id" "uuid",
    "dataset_id" "uuid",
    "complexity_vector" "public"."vector"(16),
    "metadata" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."chunk_profiles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."dataset_merge_locks" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "agent_id" "uuid" NOT NULL,
    "device_id" "uuid",
    "acquired_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "heartbeat_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "expires_at" timestamp with time zone NOT NULL,
    "status" "text" DEFAULT 'active'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "dataset_merge_locks_status_canonical" CHECK (("status" = ANY (ARRAY['active'::"text", 'expired'::"text", 'released'::"text", 'cancelled'::"text"])))
);


ALTER TABLE "public"."dataset_merge_locks" OWNER TO "postgres";


COMMENT ON CONSTRAINT "dataset_merge_locks_status_canonical" ON "public"."dataset_merge_locks" IS 'Unified status constraint. Supersedes previous duplicate constraints.';



CREATE TABLE IF NOT EXISTS "public"."datasets" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "name" "text",
    "file_type" "text",
    "total_size_gb" double precision,
    "file_count" bigint,
    "avg_file_size_mb" double precision,
    "status" "text" DEFAULT 'registered'::"text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "scan_assigned_device" "uuid",
    "scan_assigned_at" timestamp with time zone,
    "scanned_at" timestamp with time zone,
    "storage_type" "text" DEFAULT 'local'::"text",
    "job_type" "text",
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "metadata" "jsonb",
    "merged_output_verified" boolean DEFAULT false,
    "merged_at" timestamp with time zone,
    "merged_size_gb" double precision,
    "merge_time_ms" double precision,
    "affinity_device_id" "uuid",
    "dataset_checksum" "text",
    "disk_space_check_enabled" boolean DEFAULT true,
    "merge_strategy" "text" DEFAULT 'auto'::"text",
    "merge_started_at" timestamp with time zone,
    "merge_completed_at" timestamp with time zone,
    "merge_error" "text",
    "storage_config_id" "uuid",
    "source_path" "text",
    "detected_columns" "jsonb",
    "scan_completed" boolean DEFAULT false,
    CONSTRAINT "datasets_job_type_check" CHECK ((("job_type" IS NULL) OR ("job_type" = ANY (ARRAY['preprocess'::"text", 'embedding'::"text", 'merge'::"text", 'scan'::"text", 'index'::"text", 'validate'::"text"])))),
    CONSTRAINT "datasets_merge_strategy_check" CHECK (("merge_strategy" = ANY (ARRAY['auto'::"text", 'sequential'::"text", 'tree'::"text"]))),
    CONSTRAINT "datasets_status_check" CHECK (("status" = ANY (ARRAY['registered'::"text", 'scanning'::"text", 'scanned'::"text", 'chunked'::"text", 'processing'::"text", 'merge_pending'::"text", 'merging'::"text", 'merged'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."datasets" OWNER TO "postgres";


COMMENT ON TABLE "public"."datasets" IS 'Metadata for raw datasets registered by clients, linked to batch_chunks and merge jobs.';



COMMENT ON COLUMN "public"."datasets"."file_count" IS 'Number of files detected during scan';



COMMENT ON COLUMN "public"."datasets"."scanned_at" IS 'Timestamp when dataset was last scanned';



COMMENT ON COLUMN "public"."datasets"."storage_config_id" IS 'FK to org_storage_configs for storage credentials';



COMMENT ON COLUMN "public"."datasets"."source_path" IS 'S3 prefix or local path for the dataset';



COMMENT ON COLUMN "public"."datasets"."detected_columns" IS 'JSONB column structure detected during scan';



COMMENT ON COLUMN "public"."datasets"."scan_completed" IS 'Whether the scan has completed (used by edge function)';



CREATE TABLE IF NOT EXISTS "public"."device_benchmarks" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid",
    "org_id" "uuid",
    "test_name" "text" DEFAULT 'embedding_test'::"text",
    "latency_ms" double precision,
    "throughput" double precision,
    "model_used" "text",
    "timestamp" timestamp with time zone DEFAULT "now"(),
    "extra" "jsonb",
    "score_vector" "public"."vector"(16),
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."device_benchmarks" OWNER TO "postgres";


COMMENT ON TABLE "public"."device_benchmarks" IS 'Logs performance metrics for each completed chunk, used to update learning vectors.';



CREATE TABLE IF NOT EXISTS "public"."device_claims" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "claim_code" "text" NOT NULL,
    "device_id" "uuid",
    "used_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "expires_at" timestamp with time zone DEFAULT ("now"() + '24:00:00'::interval)
);


ALTER TABLE "public"."device_claims" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid" NOT NULL,
    "org_id" "uuid" NOT NULL,
    "event_type" "text" NOT NULL,
    "payload" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "processed_at" timestamp with time zone,
    CONSTRAINT "chk_device_events_status" CHECK (("status" = ANY (ARRAY['pending'::"text", 'processing'::"text", 'processed'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."device_events" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_job_performance" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid" NOT NULL,
    "org_id" "uuid" NOT NULL,
    "job_type" "text" NOT NULL,
    "duration_ms" numeric,
    "throughput" numeric,
    "success" boolean DEFAULT true NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."device_job_performance" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_job_type_stats" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid" NOT NULL,
    "org_id" "uuid" NOT NULL,
    "job_type" "text" NOT NULL,
    "avg_duration_ms" numeric,
    "avg_throughput" numeric,
    "job_count" integer DEFAULT 0,
    "success_count" integer DEFAULT 0,
    "last_updated" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."device_job_type_stats" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_learning_history" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid",
    "profile_vector" "public"."vector"(16),
    "recorded_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."device_learning_history" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_vectors" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "device_id" "uuid",
    "org_id" "uuid",
    "profile_vector" "public"."vector"(16),
    "last_updated" timestamp with time zone DEFAULT "now"(),
    "metadata" "jsonb",
    CONSTRAINT "device_vectors_vector_length_check" CHECK (("public"."vector_dims"("profile_vector") = 16))
);


ALTER TABLE "public"."device_vectors" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."devices" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "name" "text",
    "type" "text",
    "status" "text" DEFAULT 'offline'::"text" NOT NULL,
    "last_seen" timestamp with time zone,
    "current_job_id" "uuid",
    "specs" "jsonb",
    "benchmark_ms" double precision,
    "benchmark_score" double precision DEFAULT 0,
    "total_jobs" integer DEFAULT 0,
    "total_embeddings" integer DEFAULT 0,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "environment_type" "text",
    "storage_type" "text",
    "network_zone" "text",
    "capabilities" "text"[],
    "active_jobs" integer DEFAULT 0,
    "assigned_job_id" "uuid",
    "memory_free_gb" double precision,
    "io_bandwidth_mb_s" double precision,
    "network_latency_ms" double precision,
    "cpu_cores_free" integer,
    "preferred_chunk_size_gb" double precision,
    "merge_capable" boolean DEFAULT true,
    "device_vector" "public"."vector"(64),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "total_cpu_cores" integer DEFAULT 8,
    "total_memory_gb" numeric DEFAULT 32,
    "last_heartbeat" timestamp with time zone DEFAULT "now"(),
    "mount_path" "text",
    "max_concurrency" integer DEFAULT 1,
    "active_workers" integer DEFAULT 0,
    "cpu_usage_percent" numeric(5,2),
    "gpu_available" boolean DEFAULT false,
    "last_policy_update" timestamp with time zone,
    "last_refresh" timestamp with time zone DEFAULT "now"(),
    "gpu_model" "text",
    "cuda_version" "text",
    "gpu_memory_gb" double precision,
    "gpu_driver_version" "text",
    "gpu_capability_score" double precision DEFAULT 0,
    "access_token_hash" "text",
    "revoked_at" timestamp with time zone,
    "token_rotate_fail_count" integer DEFAULT 0,
    "os" "text" DEFAULT 'linux'::"text" NOT NULL,
    "arch" "text" DEFAULT 'amd64'::"text" NOT NULL,
    "has_cgo" boolean DEFAULT true NOT NULL,
    "docker_available" boolean DEFAULT false,
    "runtime_supported" "jsonb" DEFAULT '["native"]'::"jsonb",
    "runtime_versions" "jsonb" DEFAULT '{}'::"jsonb",
    "runtime_cache" "jsonb",
    "region" "text",
    "storage_region" "text",
    "platform" "text" DEFAULT 'linux/amd64'::"text",
    "python_version" "text",
    "node_version" "text",
    "required_arch" "text",
    "required_os" "text",
    "last_claimed_at" timestamp with time zone,
    "redis_url" "text",
    CONSTRAINT "chk_devices_status" CHECK (("status" = ANY (ARRAY['online'::"text", 'offline'::"text", 'available'::"text", 'busy'::"text", 'error'::"text", 'draining'::"text"])))
);


ALTER TABLE "public"."devices" OWNER TO "postgres";


COMMENT ON TABLE "public"."devices" IS 'Stores all active compute agents (CPU/GPU/Edge). Each agent self-updates status and performance metrics.';



COMMENT ON COLUMN "public"."devices"."max_concurrency" IS 'Recommended concurrency returned by edge function';



COMMENT ON COLUMN "public"."devices"."active_workers" IS 'Current active worker threads reported by agent heartbeat';



COMMENT ON COLUMN "public"."devices"."os" IS 'Operating system of this device';



COMMENT ON COLUMN "public"."devices"."arch" IS 'CPU architecture of this device';



COMMENT ON COLUMN "public"."devices"."has_cgo" IS 'Whether this device has CGO enabled';



CREATE OR REPLACE VIEW "public"."device_learning_progress_view" AS
 SELECT "d"."id" AS "device_id",
    "d"."name" AS "device_name",
    "d"."org_id",
    "dv"."profile_vector",
    "dv"."last_updated",
    "round"(("avg"("b"."latency_ms"))::numeric, 2) AS "avg_latency_ms",
    "round"(("avg"("b"."throughput"))::numeric, 2) AS "avg_throughput",
    "count"("b"."id") AS "total_benchmarks",
    "max"("b"."timestamp") AS "last_benchmark_time",
    ( SELECT "b2"."latency_ms"
           FROM "public"."device_benchmarks" "b2"
          WHERE ("b2"."device_id" = "d"."id")
          ORDER BY "b2"."timestamp" DESC
         LIMIT 1) AS "latest_latency",
    ( SELECT "b2"."throughput"
           FROM "public"."device_benchmarks" "b2"
          WHERE ("b2"."device_id" = "d"."id")
          ORDER BY "b2"."timestamp" DESC
         LIMIT 1) AS "latest_throughput",
    "round"((((("avg"("b"."latency_ms") - ( SELECT "b3"."latency_ms"
           FROM "public"."device_benchmarks" "b3"
          WHERE ("b3"."device_id" = "d"."id")
          ORDER BY "b3"."timestamp" DESC
         LIMIT 1)) / NULLIF("avg"("b"."latency_ms"), (0)::double precision)) * (100)::double precision))::numeric, 2) AS "latency_improvement_pct",
    "round"(((((( SELECT "b4"."throughput"
           FROM "public"."device_benchmarks" "b4"
          WHERE ("b4"."device_id" = "d"."id")
          ORDER BY "b4"."timestamp" DESC
         LIMIT 1) - "avg"("b"."throughput")) / NULLIF("avg"("b"."throughput"), (0)::double precision)) * (100)::double precision))::numeric, 2) AS "throughput_gain_pct",
    "round"((COALESCE(( SELECT "avg"(("v1"."profile_vector" OPERATOR("public".<->) "v2"."profile_vector")) AS "avg"
           FROM ("public"."device_learning_history" "v1"
             JOIN "public"."device_learning_history" "v2" ON ((("v1"."device_id" = "v2"."device_id") AND ("v1"."recorded_at" < "v2"."recorded_at"))))
          WHERE ("v1"."device_id" = "d"."id")), (0)::double precision))::numeric, 4) AS "stability_score"
   FROM (("public"."devices" "d"
     LEFT JOIN "public"."device_vectors" "dv" ON (("dv"."device_id" = "d"."id")))
     LEFT JOIN "public"."device_benchmarks" "b" ON (("b"."device_id" = "d"."id")))
  GROUP BY "d"."id", "d"."name", "d"."org_id", "dv"."profile_vector", "dv"."last_updated";


ALTER VIEW "public"."device_learning_progress_view" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_policies" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "device_id" "uuid",
    "min_concurrency" integer DEFAULT 1,
    "max_concurrency" integer DEFAULT 1,
    "cooldown_seconds" integer DEFAULT 60,
    "enabled" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone,
    CONSTRAINT "chk_device_policies_concurrency" CHECK (((("min_concurrency" IS NULL) OR ("min_concurrency" >= 0)) AND (("max_concurrency" IS NULL) OR ("max_concurrency" >= 1)) AND (("min_concurrency" IS NULL) OR ("max_concurrency" IS NULL) OR ("min_concurrency" <= "max_concurrency"))))
);


ALTER TABLE "public"."device_policies" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."device_ranking_view" AS
 SELECT "d"."id",
    "d"."name",
    ("d"."specs" -> 'capabilities'::"text") AS "capabilities",
    ("dv"."profile_vector" OPERATOR("public".<#>) ( SELECT ("array_agg"(("random"() - (0.5)::double precision)))::"public"."vector"(16) AS "array_agg"
           FROM "generate_series"(1, 16) "generate_series"("generate_series"))) AS "similarity"
   FROM ("public"."devices" "d"
     JOIN "public"."device_vectors" "dv" ON (("d"."id" = "dv"."device_id")))
  WHERE ("d"."status" = 'online'::"text")
  ORDER BY ("dv"."profile_vector" OPERATOR("public".<#>) ( SELECT ("array_agg"(("random"() - (0.5)::double precision)))::"public"."vector"(16) AS "array_agg"
           FROM "generate_series"(1, 16) "generate_series"("generate_series"))) DESC;


ALTER VIEW "public"."device_ranking_view" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."device_routing_rules" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "device_id" "uuid",
    "job_type" "text" NOT NULL,
    "action" "text" NOT NULL,
    "priority" integer DEFAULT 0,
    "enabled" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "created_by" "uuid",
    CONSTRAINT "device_routing_rules_action_check" CHECK (("action" = ANY (ARRAY['prefer'::"text", 'exclude'::"text", 'require'::"text"])))
);


ALTER TABLE "public"."device_routing_rules" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."device_summary_view" AS
 SELECT "d"."id" AS "device_id",
    "d"."org_id",
    "d"."name",
    "d"."environment_type",
    "d"."storage_type",
    "d"."status",
    "dv"."profile_vector",
    "db"."latency_ms",
    "db"."throughput",
    "dv"."last_updated"
   FROM (("public"."devices" "d"
     LEFT JOIN "public"."device_vectors" "dv" ON (("d"."id" = "dv"."device_id")))
     LEFT JOIN LATERAL ( SELECT "b"."latency_ms",
            "b"."throughput"
           FROM "public"."device_benchmarks" "b"
          WHERE ("b"."device_id" = "d"."id")
          ORDER BY "b"."timestamp" DESC
         LIMIT 1) "db" ON (true));


ALTER VIEW "public"."device_summary_view" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."dismissed_alerts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "alert_id" "uuid" NOT NULL,
    "org_id" "uuid" NOT NULL,
    "dismissed_at" timestamp with time zone DEFAULT "now"(),
    "dismissed_by" "uuid",
    "reason" "text"
);


ALTER TABLE "public"."dismissed_alerts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."enterprise_integrations" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "provider" "text",
    "role_arn" "text",
    "external_id" "text",
    "client_id" "text",
    "region" "text",
    "last_refreshed" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "vault_secret_name" "text",
    CONSTRAINT "enterprise_integrations_provider_check" CHECK (("provider" = ANY (ARRAY['aws'::"text", 'gcp'::"text", 'azure'::"text"])))
);


ALTER TABLE "public"."enterprise_integrations" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."environment_cache" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "runtime_type" "text" NOT NULL,
    "runtime_version" "text" NOT NULL,
    "dependency_lock_hash" "text" NOT NULL,
    "storage_path" "text" NOT NULL,
    "checksum" "text" NOT NULL,
    "size_bytes" bigint NOT NULL,
    "compressed" boolean DEFAULT false NOT NULL,
    "compatible_runtimes" "jsonb" DEFAULT '["python", "node"]'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "last_used_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "use_count" integer DEFAULT 0 NOT NULL,
    "deleted_at" timestamp with time zone,
    "platform" "text" DEFAULT 'linux/amd64'::"text",
    CONSTRAINT "environment_cache_runtime_type_check" CHECK (("runtime_type" = ANY (ARRAY['python'::"text", 'node'::"text", 'native'::"text"])))
);


ALTER TABLE "public"."environment_cache" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."execution_policies" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "name" "text" NOT NULL,
    "max_retries" integer DEFAULT 3,
    "retry_backoff_seconds" integer DEFAULT 5,
    "retry_backoff_multiplier" double precision DEFAULT 2.0,
    "max_retry_delay_seconds" integer DEFAULT 300,
    "default_timeout_seconds" integer DEFAULT 300,
    "hard_timeout_seconds" integer DEFAULT 600,
    "retryable_errors" "text"[] DEFAULT ARRAY['timeout'::"text", 'connection'::"text", 'network'::"text"],
    "fatal_errors" "text"[] DEFAULT ARRAY['untrusted'::"text", 'security'::"text"],
    "mode_priority" "jsonb" DEFAULT '[{"mode": "runtime", "fallback": ["docker", "native"]}]'::"jsonb",
    "enabled" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."execution_policies" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."execution_steps" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "execution_id" "uuid" NOT NULL,
    "step_index" integer NOT NULL,
    "step_type" "text" NOT NULL,
    "plugin_id" "text",
    "script_id" "text",
    "config" "jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "agent_job_id" "uuid",
    "requires_approval" boolean DEFAULT false NOT NULL,
    "retry_count" integer DEFAULT 0 NOT NULL,
    "error" "text",
    "approved_by" "uuid",
    "approved_at" timestamp with time zone,
    "output_keys" "jsonb" DEFAULT '[]'::"jsonb",
    "next_step_input_mapping" "jsonb" DEFAULT '{}'::"jsonb",
    "runtime_override" "jsonb",
    "output_json" "jsonb",
    "org_id" "uuid",
    CONSTRAINT "execution_steps_status_check" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'awaiting_approval'::"text", 'completed'::"text", 'failed'::"text", 'failed_continuing'::"text", 'skipped'::"text"])))
);


ALTER TABLE "public"."execution_steps" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."executions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "pipeline_template_id" "uuid",
    "status" "text" DEFAULT 'running'::"text" NOT NULL,
    "current_step_index" integer DEFAULT 0 NOT NULL,
    "total_steps" integer DEFAULT 0 NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "degraded_steps" integer DEFAULT 0 NOT NULL,
    CONSTRAINT "executions_status_check" CHECK (("status" = ANY (ARRAY['running'::"text", 'awaiting_approval'::"text", 'completed'::"text", 'completed_with_warnings'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."executions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."http_queue" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "url" "text" NOT NULL,
    "body" "jsonb" NOT NULL,
    "headers" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "processed" boolean DEFAULT false,
    "processed_at" timestamp with time zone,
    "result" "text",
    "status_code" integer,
    "retry_count" integer DEFAULT 0,
    "retry_at" timestamp with time zone,
    "idempotency_key" "uuid",
    "org_id" "uuid"
);


ALTER TABLE "public"."http_queue" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."job_checkpoints" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "execution_id" "uuid" NOT NULL,
    "run_id" "uuid" NOT NULL,
    "step_index" integer NOT NULL,
    "checkpoint_data" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "progress_percent" numeric DEFAULT 0 NOT NULL,
    "checkpointed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "is_completed" boolean DEFAULT false NOT NULL,
    "expires_at" timestamp with time zone DEFAULT ("now"() + '7 days'::interval) NOT NULL,
    "org_id" "uuid",
    CONSTRAINT "job_checkpoints_progress_percent_check" CHECK ((("progress_percent" >= (0)::numeric) AND ("progress_percent" <= (100)::numeric)))
);


ALTER TABLE "public"."job_checkpoints" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."job_notification_queue" (
    "id" bigint NOT NULL,
    "job_id" "uuid",
    "agent_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "processed" boolean DEFAULT false,
    "org_id" "uuid",
    "event_type" "text",
    "payload" "jsonb"
);


ALTER TABLE "public"."job_notification_queue" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."job_notification_queue_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."job_notification_queue_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."job_notification_queue_id_seq" OWNED BY "public"."job_notification_queue"."id";



CREATE TABLE IF NOT EXISTS "public"."leases" (
    "job_id" "uuid" NOT NULL,
    "device_id" "uuid" NOT NULL,
    "lease_expires_at" timestamp with time zone NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "status" "text" DEFAULT 'active'::"text",
    "org_id" "uuid",
    CONSTRAINT "chk_leases_expires_future" CHECK ((("status" <> 'active'::"text") OR ("lease_expires_at" > "now"()))),
    CONSTRAINT "leases_status_canonical" CHECK (("status" = ANY (ARRAY['active'::"text", 'expired'::"text", 'released'::"text", 'cancelled'::"text"])))
);

ALTER TABLE ONLY "public"."leases" FORCE ROW LEVEL SECURITY;


ALTER TABLE "public"."leases" OWNER TO "postgres";


COMMENT ON CONSTRAINT "leases_status_canonical" ON "public"."leases" IS 'Unified status constraint. Supersedes previous triplicate constraints.';



CREATE TABLE IF NOT EXISTS "public"."org_members" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "user_id" "uuid",
    "role" "text" DEFAULT 'member'::"text",
    "joined_at" timestamp with time zone DEFAULT "now"(),
    "member_name" "text",
    "member_email" "text",
    "deleted_at" timestamp with time zone
);

ALTER TABLE ONLY "public"."org_members" FORCE ROW LEVEL SECURITY;


ALTER TABLE "public"."org_members" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."org_plugins" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "plugin_id" "uuid" NOT NULL,
    "enabled" boolean DEFAULT true,
    "rollout_percentage" integer DEFAULT 100,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    CONSTRAINT "org_plugins_rollout_percentage_check" CHECK ((("rollout_percentage" >= 0) AND ("rollout_percentage" <= 100)))
);


ALTER TABLE "public"."org_plugins" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."org_quotas" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "max_concurrent_jobs" integer DEFAULT 10 NOT NULL,
    "max_cpu_cores" integer DEFAULT 100 NOT NULL,
    "max_memory_gb" integer DEFAULT 256 NOT NULL,
    "max_storage_gb" integer DEFAULT 1000 NOT NULL,
    "max_environments" integer DEFAULT 50 NOT NULL,
    "max_plugins" integer DEFAULT 100 NOT NULL,
    "period_start" timestamp with time zone DEFAULT "now"() NOT NULL,
    "period_end" timestamp with time zone DEFAULT ("now"() + '1 mon'::interval) NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "plan_name" "text" DEFAULT 'free'::"text" NOT NULL,
    "max_devices" integer DEFAULT 5 NOT NULL,
    "rate_limit_jobs_per_minute" integer DEFAULT 60 NOT NULL
);


ALTER TABLE "public"."org_quotas" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."org_storage_configs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "storage_mode" "text" NOT NULL,
    "provider" "text",
    "bucket_name" "text",
    "region" "text",
    "endpoint" "text",
    "mount_base_path" "text",
    "vault_secret_name" "text",
    "is_default" boolean DEFAULT false,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "name" "text",
    CONSTRAINT "org_storage_configs_storage_mode_check" CHECK (("storage_mode" = ANY (ARRAY['shared_mount'::"text", 'object_storage'::"text"]))),
    CONSTRAINT "provider_required_check" CHECK (((("storage_mode" = 'object_storage'::"text") AND ("provider" IS NOT NULL)) OR ("storage_mode" = 'shared_mount'::"text")))
);


ALTER TABLE "public"."org_storage_configs" OWNER TO "postgres";


COMMENT ON COLUMN "public"."org_storage_configs"."provider" IS 'Storage provider type (aws_s3, gcs, azure_blob, s3_compatible)';



COMMENT ON COLUMN "public"."org_storage_configs"."vault_secret_name" IS 'Supabase Vault secret name for stored credentials';



COMMENT ON COLUMN "public"."org_storage_configs"."name" IS 'User-friendly name for the storage config';



CREATE TABLE IF NOT EXISTS "public"."org_usage" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "metric_name" "text" NOT NULL,
    "metric_value" bigint DEFAULT 0 NOT NULL,
    "period_start" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."org_usage" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."orgs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "plan" "text" DEFAULT 'free'::"text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "claim_secret" "text",
    "team_size" integer,
    "auth_id" "uuid",
    "claim_secret_expires_at" timestamp with time zone,
    "claim_secret_used_count" integer DEFAULT 0,
    "claim_secret_max_uses" integer DEFAULT 1,
    "claim_code" "text"
);


ALTER TABLE "public"."orgs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."pipeline_templates" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "description" "text",
    "steps" "jsonb" NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "dataset_id" "uuid"
);


ALTER TABLE "public"."pipeline_templates" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."plan_limits" (
    "plan_name" "text" NOT NULL,
    "max_devices" integer DEFAULT 10,
    "max_concurrent_jobs" integer DEFAULT 50,
    "max_storage_gb" numeric DEFAULT 100,
    "max_plugins" integer DEFAULT 20,
    "max_org_members" integer DEFAULT 5,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."plan_limits" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."plugin_execution_history" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "plugin_id" "uuid" NOT NULL,
    "job_id" "uuid",
    "device_id" "uuid",
    "status" "text" DEFAULT 'started'::"text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"(),
    "finished_at" timestamp with time zone,
    "error" "text",
    "execution_duration_ms" integer,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "rows_processed" integer,
    "output_summary" "jsonb"
);


ALTER TABLE "public"."plugin_execution_history" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."plugin_signing_keys" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "public_key" "text" NOT NULL,
    "algorithm" "text" DEFAULT 'ed25519'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "expires_at" timestamp with time zone,
    "revoked_at" timestamp with time zone,
    "vault_secret_name" "text"
);

COMMENT ON COLUMN "public"."plugin_signing_keys"."vault_secret_name" IS 'Supabase Vault secret name for the Ed25519 private key (PKCS8 format, base64)';

COMMENT ON COLUMN "public"."plugin_signing_keys"."vault_secret_name" IS 'Supabase Vault secret name for the Ed25519 private key (PKCS8 format, base64)';


ALTER TABLE "public"."plugin_signing_keys" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."plugins" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "version" "text" NOT NULL,
    "language" "text" DEFAULT 'python'::"text" NOT NULL,
    "plugin_type" "text" DEFAULT 'client'::"text" NOT NULL,
    "storage_path" "text" NOT NULL,
    "checksum" "text",
    "signature" "bytea",
    "signature_key_id" "text",
    "resources" "jsonb" DEFAULT '{}'::"jsonb",
    "trusted" boolean DEFAULT false,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "signature_verified" boolean DEFAULT false,
    "category" "text" DEFAULT 'processing'::"text" NOT NULL,
    "config_schema" "jsonb" DEFAULT '{}'::"jsonb",
    "input_schema" "jsonb" DEFAULT '{}'::"jsonb",
    "output_schema" "jsonb" DEFAULT '{}'::"jsonb",
    "description" "text",
    "os" "text" DEFAULT 'linux'::"text" NOT NULL,
    "arch" "text" DEFAULT 'amd64'::"text" NOT NULL,
    "plugin_group" "text",
    "runtime_type" "text",
    "runtime_dependencies" "jsonb" DEFAULT '{}'::"jsonb",
    "execution_modes" "jsonb" DEFAULT '{"fallback": ["docker", "native"], "preferred": "runtime"}'::"jsonb",
    "plugin_entrypoint" "text",
    "network" boolean DEFAULT false,
    "created_by" "uuid"
);


ALTER TABLE "public"."plugins" OWNER TO "postgres";


COMMENT ON COLUMN "public"."plugins"."os" IS 'Operating system target for this plugin binary (linux, darwin, windows, any)';



COMMENT ON COLUMN "public"."plugins"."arch" IS 'CPU architecture target for this plugin binary (amd64, arm64, any)';



COMMENT ON COLUMN "public"."plugins"."plugin_group" IS 'Optional grouping identifier for related platform variants';



CREATE TABLE IF NOT EXISTS "public"."runtime_environments" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "runtime_type" "text" NOT NULL,
    "runtime_version" "text" NOT NULL,
    "dependency_hash" "text" NOT NULL,
    "environment_path" "text" NOT NULL,
    "device_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "last_used_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "use_count" integer DEFAULT 0 NOT NULL,
    "invalidated_at" timestamp with time zone,
    "invalidation_reason" "text",
    "platform" "text" DEFAULT 'linux/amd64'::"text",
    CONSTRAINT "runtime_environments_runtime_type_check" CHECK (("runtime_type" = ANY (ARRAY['python'::"text", 'node'::"text", 'native'::"text"])))
);


ALTER TABLE "public"."runtime_environments" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."step_outputs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "execution_step_id" "uuid" NOT NULL,
    "output_key" "text" NOT NULL,
    "output_value" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "org_id" "uuid"
);


ALTER TABLE "public"."step_outputs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."system_config" (
    "key" "text" NOT NULL,
    "value" "text" NOT NULL
);


ALTER TABLE "public"."system_config" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."system_logs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "event_type" "text" NOT NULL,
    "message" "text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "org_id" "uuid"
);

ALTER TABLE ONLY "public"."system_logs" FORCE ROW LEVEL SECURITY;


ALTER TABLE "public"."system_logs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."user_orgs" AS
 SELECT "om"."id",
    "om"."user_id",
    "om"."org_id",
    "om"."role",
    "om"."joined_at",
    "o"."name" AS "org_name"
   FROM ("public"."org_members" "om"
     JOIN "public"."orgs" "o" ON (("o"."id" = "om"."org_id")));


ALTER VIEW "public"."user_orgs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_active_agent_jobs" AS
 SELECT "id",
    "org_id",
    "agent_id",
    "job_type",
    "status",
    "created_at",
    "assigned_at",
    "finished_at"
   FROM "public"."agent_jobs"
  WHERE ("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text"]));


ALTER VIEW "public"."v_active_agent_jobs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_active_jobs" AS
 SELECT "aj"."id" AS "job_id",
    "aj"."org_id",
    "aj"."agent_id",
    "aj"."job_type",
    "aj"."status",
    "aj"."created_at",
    "aj"."assigned_at",
    "aj"."started_at",
    "aj"."finished_at",
    "aj"."retry_count",
    "aj"."lease_expires_at",
    "aj"."last_error",
    "aj"."plugin_id",
    "aj"."execution_id",
    "aj"."execution_step_id",
        CASE
            WHEN (("aj"."status" = ANY (ARRAY['assigned'::"text", 'running'::"text"])) AND ("aj"."lease_expires_at" < "now"())) THEN true
            ELSE false
        END AS "is_stuck",
        CASE
            WHEN ("aj"."status" = ANY (ARRAY['assigned'::"text", 'running'::"text"])) THEN (EXTRACT(epoch FROM ("now"() - COALESCE("aj"."assigned_at", "aj"."created_at"))))::integer
            ELSE NULL::integer
        END AS "runtime_seconds",
        CASE
            WHEN ("aj"."lease_expires_at" IS NOT NULL) THEN (EXTRACT(epoch FROM ("aj"."lease_expires_at" - "now"())))::integer
            ELSE NULL::integer
        END AS "lease_ttl_seconds",
    "d"."name" AS "device_name",
    "d"."status" AS "device_status",
    "d"."active_jobs" AS "device_active_jobs",
    "d"."max_concurrency" AS "device_max_concurrency"
   FROM ("public"."agent_jobs" "aj"
     LEFT JOIN "public"."devices" "d" ON (("aj"."agent_id" = "d"."id")))
  WHERE ("aj"."status" <> ALL (ARRAY['completed'::"text", 'dead'::"text"]));


ALTER VIEW "public"."v_active_jobs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_device_claim_eligible" AS
 SELECT "id" AS "device_id",
    "org_id",
    "name",
    "status",
    "active_jobs",
    "max_concurrency",
        CASE
            WHEN ("status" = ANY (ARRAY['online'::"text", 'available'::"text", 'busy'::"text"])) THEN true
            ELSE false
        END AS "can_claim_jobs",
        CASE
            WHEN ("status" = 'offline'::"text") THEN 'Device is offline and cannot claim jobs'::"text"
            WHEN ("status" <> ALL (ARRAY['online'::"text", 'available'::"text", 'busy'::"text"])) THEN 'Device status does not allow job claiming'::"text"
            WHEN ("active_jobs" >= "max_concurrency") THEN 'Device at concurrency limit'::"text"
            ELSE NULL::"text"
        END AS "claim_blocked_reason"
   FROM "public"."devices" "d";


ALTER VIEW "public"."v_device_claim_eligible" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_device_health" AS
 SELECT "id" AS "device_id",
    "org_id",
    "name",
    "status",
    "active_jobs",
    "max_concurrency",
    "memory_free_gb",
    "cpu_cores_free",
        CASE
            WHEN ("status" = 'offline'::"text") THEN 'offline'::"text"
            WHEN ("status" = 'error'::"text") THEN 'error'::"text"
            WHEN ("last_heartbeat" < ("now"() - '00:02:00'::interval)) THEN 'stale'::"text"
            WHEN ("active_jobs" >= "max_concurrency") THEN 'at_capacity'::"text"
            ELSE 'healthy'::"text"
        END AS "health_status"
   FROM "public"."devices" "d";


ALTER VIEW "public"."v_device_health" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_device_runtime_capabilities" AS
 SELECT "id",
    "org_id",
    "name",
    "status",
    "docker_available",
    "runtime_supported",
    "runtime_versions",
    "capabilities",
        CASE
            WHEN (("docker_available" = true) AND ("runtime_supported" @> '["python", "node"]'::"jsonb")) THEN 'full'::"text"
            WHEN (("runtime_supported" @> '["python"]'::"jsonb") OR ("runtime_supported" @> '["node"]'::"jsonb")) THEN 'partial'::"text"
            ELSE 'minimal'::"text"
        END AS "runtime_capability_level"
   FROM "public"."devices" "d";


ALTER VIEW "public"."v_device_runtime_capabilities" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_job_lifecycle" AS
 SELECT "j"."id" AS "job_id",
    "j"."org_id",
    "j"."agent_id",
    "d"."name" AS "agent_name",
    "j"."job_type",
    "j"."status",
    "j"."created_at",
    "j"."assigned_at",
    "j"."started_at",
    "j"."finished_at",
    (EXTRACT(epoch FROM ("j"."finished_at" - "j"."started_at")) * (1000)::numeric) AS "run_time_ms",
    "j"."plugin_id",
    "j"."plugin_version"
   FROM ("public"."agent_jobs" "j"
     LEFT JOIN "public"."devices" "d" ON (("j"."agent_id" = "d"."id")))
  ORDER BY "j"."created_at" DESC;


ALTER VIEW "public"."v_job_lifecycle" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_job_timeline" AS
 SELECT "aj"."id" AS "job_id",
    "aj"."org_id",
    "aj"."job_type",
    "aj"."status",
    "aj"."created_at",
    "aj"."assigned_at",
    "aj"."started_at",
    "aj"."finished_at",
    (EXTRACT(epoch FROM (COALESCE("aj"."assigned_at", "aj"."created_at") - "aj"."created_at")))::integer AS "queue_time_ms",
    (EXTRACT(epoch FROM (COALESCE("aj"."started_at", "aj"."assigned_at") - COALESCE("aj"."assigned_at", "aj"."created_at"))))::integer AS "assignment_to_start_ms",
    (EXTRACT(epoch FROM ("aj"."finished_at" - COALESCE("aj"."started_at", "aj"."assigned_at"))))::integer AS "execution_time_ms",
    (EXTRACT(epoch FROM ("aj"."finished_at" - "aj"."created_at")))::integer AS "total_lifecycle_ms",
    "aj"."duration_ms",
    "aj"."retry_count",
    "aj"."throughput",
    "aj"."last_error",
    "d"."name" AS "device_name"
   FROM ("public"."agent_jobs" "aj"
     LEFT JOIN "public"."devices" "d" ON (("aj"."agent_id" = "d"."id")));


ALTER VIEW "public"."v_job_timeline" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_pending_runtime_jobs" AS
 SELECT "id",
    "org_id",
    "job_type",
    "status",
    "runtime_type",
    "execution_mode",
    "runtime_dependencies",
    "created_at",
    "plugin_id",
    "execution_id"
   FROM "public"."agent_jobs" "aj"
  WHERE (("status" = 'pending'::"text") AND ("runtime_type" <> 'native'::"text"));


ALTER VIEW "public"."v_pending_runtime_jobs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_recent_failures" AS
 SELECT "aj"."id" AS "job_id",
    "aj"."org_id",
    "aj"."job_type",
    "aj"."status",
    "aj"."last_error",
    "aj"."retry_count",
    "aj"."created_at",
    "aj"."finished_at",
    "aj"."agent_id",
    "d"."name" AS "device_name"
   FROM ("public"."agent_jobs" "aj"
     LEFT JOIN "public"."devices" "d" ON (("aj"."agent_id" = "d"."id")))
  WHERE (("aj"."status" = ANY (ARRAY['failed'::"text", 'dead'::"text"])) AND (COALESCE("aj"."finished_at", "aj"."created_at") > ("now"() - '24:00:00'::interval)));


ALTER VIEW "public"."v_recent_failures" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_stuck_job_summary" AS
 SELECT "count"(*) FILTER (WHERE (("status" = ANY (ARRAY['assigned'::"text", 'running'::"text"])) AND (COALESCE("assigned_at", "started_at") < ("now"() - '00:30:00'::interval)))) AS "stuck_jobs_30m",
    "count"(*) FILTER (WHERE ("status" = 'pending'::"text")) AS "pending_jobs",
    "now"() AS "generated_at"
   FROM "public"."agent_jobs";


ALTER VIEW "public"."v_stuck_job_summary" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_stuck_jobs" AS
 SELECT "aj"."id" AS "job_id",
    "aj"."org_id",
    "aj"."agent_id",
    "aj"."job_type",
    "aj"."status",
    "aj"."created_at",
    "aj"."assigned_at",
    "aj"."lease_expires_at",
    "aj"."retry_count",
    "aj"."last_error",
    (EXTRACT(epoch FROM ("now"() - COALESCE("aj"."assigned_at", "aj"."created_at"))))::integer AS "stuck_seconds",
    "d"."name" AS "device_name",
    "d"."status" AS "device_status",
    "d"."last_heartbeat" AS "device_last_heartbeat"
   FROM ("public"."agent_jobs" "aj"
     LEFT JOIN "public"."devices" "d" ON (("aj"."agent_id" = "d"."id")))
  WHERE (("aj"."status" = ANY (ARRAY['assigned'::"text", 'running'::"text"])) AND ("aj"."lease_expires_at" < "now"()));


ALTER VIEW "public"."v_stuck_jobs" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_system_health" AS
 SELECT "now"() AS "snapshot_at",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE ("agent_jobs"."status" = 'pending'::"text")) AS "pending_jobs",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE ("agent_jobs"."status" = 'assigned'::"text")) AS "assigned_jobs",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE ("agent_jobs"."status" = 'running'::"text")) AS "running_jobs",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE ("agent_jobs"."status" = 'completed'::"text")) AS "completed_jobs",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE ("agent_jobs"."status" = 'failed'::"text")) AS "failed_jobs",
    ( SELECT "count"(*) AS "count"
           FROM "public"."devices"
          WHERE ("devices"."status" = 'online'::"text")) AS "online_devices",
    ( SELECT "count"(*) AS "count"
           FROM "public"."devices"
          WHERE ("devices"."status" = 'offline'::"text")) AS "offline_devices",
    ( SELECT "count"(*) AS "count"
           FROM "public"."devices"
          WHERE ("devices"."status" = 'busy'::"text")) AS "busy_devices",
    ( SELECT "count"(*) AS "count"
           FROM "public"."devices"
          WHERE ("devices"."status" = 'error'::"text")) AS "error_devices",
    ( SELECT "count"(*) AS "count"
           FROM "public"."http_queue"
          WHERE ("http_queue"."processed" = false)) AS "pending_queue_items",
    ( SELECT "count"(*) AS "count"
           FROM "public"."http_queue"
          WHERE (("http_queue"."processed" = false) AND ("http_queue"."retry_count" >= 3))) AS "queue_dead_letters",
    ( SELECT "count"(*) AS "count"
           FROM "public"."system_logs"
          WHERE (("system_logs"."event_type" ~~ '%error%'::"text") AND ("system_logs"."created_at" > ("now"() - '00:05:00'::interval)))) AS "errors_last_5min",
    ( SELECT "count"(*) AS "count"
           FROM "public"."agent_jobs"
          WHERE (("agent_jobs"."status" = ANY (ARRAY['assigned'::"text", 'running'::"text"])) AND ("agent_jobs"."lease_expires_at" < "now"()))) AS "stuck_jobs";


ALTER VIEW "public"."v_system_health" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."vector_batches" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_id" "uuid",
    "org_id" "uuid",
    "batch_number" integer,
    "raw_file_path" "text",
    "file_type" "text",
    "status" "text" DEFAULT 'pending'::"text",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."vector_batches" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."vector_datasets" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "user_id" "uuid",
    "name" "text",
    "description" "text",
    "total_vectors" integer DEFAULT 0,
    "last_batch" integer DEFAULT 0,
    "qdrant_collection" "text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "storage_type" "text",
    "storage_uri_root" "text",
    "storage_access_ref" "text",
    "output_path" "text",
    "output_format" "text" DEFAULT 'parquet'::"text",
    "merge_status" "text" DEFAULT 'pending'::"text",
    "merged_at" timestamp with time zone
);


ALTER TABLE "public"."vector_datasets" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."vector_store" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_id" "uuid",
    "org_id" "uuid",
    "batch_id" "uuid",
    "chunk_id" "uuid",
    "qdrant_id" "text",
    "embedding" "public"."vector"(384),
    "metadata" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."vector_store" OWNER TO "postgres";


ALTER TABLE ONLY "public"."job_notification_queue" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."job_notification_queue_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."agent_jobs_archive"
    ADD CONSTRAINT "agent_jobs_archive_execution_step_id_key" UNIQUE ("execution_step_id");



ALTER TABLE ONLY "public"."agent_jobs_archive"
    ADD CONSTRAINT "agent_jobs_archive_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_checkpoint_unique" UNIQUE ("checkpoint_id");



ALTER TABLE ONLY "public"."agent_jobs_dead_letter"
    ADD CONSTRAINT "agent_jobs_dead_letter_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."agent_metrics"
    ADD CONSTRAINT "agent_metrics_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."agent_worker_activity"
    ADD CONSTRAINT "agent_worker_activity_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."alert_history"
    ADD CONSTRAINT "alert_history_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."alert_rules"
    ADD CONSTRAINT "alert_rules_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."batch_chunks"
    ADD CONSTRAINT "batch_chunks_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."bootstrap_rate_limits"
    ADD CONSTRAINT "bootstrap_rate_limits_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."dataset_merge_locks"
    ADD CONSTRAINT "chk_merge_locks_single_active" EXCLUDE USING "gist" ("dataset_id" WITH =, "tstzrange"("acquired_at", "expires_at") WITH &&) WHERE (("status" = 'active'::"text"));



ALTER TABLE ONLY "public"."chunk_complexity_cache"
    ADD CONSTRAINT "chunk_complexity_cache_pkey" PRIMARY KEY ("dataset_id", "chunk_id");



ALTER TABLE ONLY "public"."chunk_profiles"
    ADD CONSTRAINT "chunk_profiles_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."dataset_merge_locks"
    ADD CONSTRAINT "dataset_merge_locks_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."datasets"
    ADD CONSTRAINT "datasets_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_benchmarks"
    ADD CONSTRAINT "device_benchmarks_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_claims"
    ADD CONSTRAINT "device_claims_org_id_claim_code_key" UNIQUE ("org_id", "claim_code");



ALTER TABLE ONLY "public"."device_claims"
    ADD CONSTRAINT "device_claims_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_events"
    ADD CONSTRAINT "device_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_job_performance"
    ADD CONSTRAINT "device_job_performance_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_job_type_stats"
    ADD CONSTRAINT "device_job_type_stats_device_id_job_type_key" UNIQUE ("device_id", "job_type");



ALTER TABLE ONLY "public"."device_job_type_stats"
    ADD CONSTRAINT "device_job_type_stats_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_learning_history"
    ADD CONSTRAINT "device_learning_history_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_policies"
    ADD CONSTRAINT "device_policies_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_routing_rules"
    ADD CONSTRAINT "device_routing_rules_org_id_device_id_job_type_action_key" UNIQUE ("org_id", "device_id", "job_type", "action");



ALTER TABLE ONLY "public"."device_routing_rules"
    ADD CONSTRAINT "device_routing_rules_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."device_vectors"
    ADD CONSTRAINT "device_vectors_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."devices"
    ADD CONSTRAINT "devices_org_id_name_key" UNIQUE ("org_id", "name");



ALTER TABLE ONLY "public"."devices"
    ADD CONSTRAINT "devices_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."dismissed_alerts"
    ADD CONSTRAINT "dismissed_alerts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."enterprise_integrations"
    ADD CONSTRAINT "enterprise_integrations_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."environment_cache"
    ADD CONSTRAINT "environment_cache_org_id_runtime_type_runtime_version_depen_key" UNIQUE ("org_id", "runtime_type", "runtime_version", "dependency_lock_hash");



ALTER TABLE ONLY "public"."environment_cache"
    ADD CONSTRAINT "environment_cache_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."execution_policies"
    ADD CONSTRAINT "execution_policies_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."execution_steps"
    ADD CONSTRAINT "execution_steps_execution_id_step_index_key" UNIQUE ("execution_id", "step_index");



ALTER TABLE ONLY "public"."execution_steps"
    ADD CONSTRAINT "execution_steps_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."executions"
    ADD CONSTRAINT "executions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."http_queue"
    ADD CONSTRAINT "http_queue_idempotency_key_key" UNIQUE ("idempotency_key");



ALTER TABLE ONLY "public"."http_queue"
    ADD CONSTRAINT "http_queue_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."job_checkpoints"
    ADD CONSTRAINT "job_checkpoints_job_id_step_index_key" UNIQUE ("job_id", "step_index");



ALTER TABLE ONLY "public"."job_checkpoints"
    ADD CONSTRAINT "job_checkpoints_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."job_notification_queue"
    ADD CONSTRAINT "job_notification_queue_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."leases"
    ADD CONSTRAINT "leases_pkey" PRIMARY KEY ("job_id");



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_org_email_unique" UNIQUE ("org_id", "member_email");



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."org_plugins"
    ADD CONSTRAINT "org_plugins_org_id_plugin_id_key" UNIQUE ("org_id", "plugin_id");



ALTER TABLE ONLY "public"."org_plugins"
    ADD CONSTRAINT "org_plugins_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."org_quotas"
    ADD CONSTRAINT "org_quotas_org_id_key" UNIQUE ("org_id");



ALTER TABLE ONLY "public"."org_quotas"
    ADD CONSTRAINT "org_quotas_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."org_storage_configs"
    ADD CONSTRAINT "org_storage_configs_org_provider_unique" UNIQUE ("org_id", "provider");



ALTER TABLE ONLY "public"."org_storage_configs"
    ADD CONSTRAINT "org_storage_configs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."org_usage"
    ADD CONSTRAINT "org_usage_org_id_metric_name_period_start_key" UNIQUE ("org_id", "metric_name", "period_start");



ALTER TABLE ONLY "public"."org_usage"
    ADD CONSTRAINT "org_usage_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."orgs"
    ADD CONSTRAINT "orgs_claim_secret_key" UNIQUE ("claim_secret");



ALTER TABLE ONLY "public"."orgs"
    ADD CONSTRAINT "orgs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."pipeline_templates"
    ADD CONSTRAINT "pipeline_templates_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."plan_limits"
    ADD CONSTRAINT "plan_limits_pkey" PRIMARY KEY ("plan_name");



ALTER TABLE ONLY "public"."plugin_execution_history"
    ADD CONSTRAINT "plugin_execution_history_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."plugin_signing_keys"
    ADD CONSTRAINT "plugin_signing_keys_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."plugins"
    ADD CONSTRAINT "plugins_name_version_os_arch_key" UNIQUE ("name", "version", "os", "arch");



ALTER TABLE ONLY "public"."plugins"
    ADD CONSTRAINT "plugins_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."runtime_environments"
    ADD CONSTRAINT "runtime_environments_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."step_outputs"
    ADD CONSTRAINT "step_outputs_execution_step_id_output_key_key" UNIQUE ("execution_step_id", "output_key");



ALTER TABLE ONLY "public"."step_outputs"
    ADD CONSTRAINT "step_outputs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."system_config"
    ADD CONSTRAINT "system_config_pkey" PRIMARY KEY ("key");



ALTER TABLE ONLY "public"."system_logs"
    ADD CONSTRAINT "system_logs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "uq_agent_jobs_execution_step_id" UNIQUE ("execution_step_id");



ALTER TABLE ONLY "public"."vector_batches"
    ADD CONSTRAINT "vector_batches_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."vector_datasets"
    ADD CONSTRAINT "vector_datasets_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."vector_store"
    ADD CONSTRAINT "vector_store_pkey" PRIMARY KEY ("id");



CREATE INDEX "agent_jobs_archive_agent_id_completed_idx" ON "public"."agent_jobs_archive" USING "btree" ("agent_id", "completed");



CREATE INDEX "agent_jobs_archive_agent_id_status_assigned_at_idx" ON "public"."agent_jobs_archive" USING "btree" ("agent_id", "status", "assigned_at") WHERE ("completed" = false);



CREATE INDEX "agent_jobs_archive_assigned_at_idx" ON "public"."agent_jobs_archive" USING "btree" ("assigned_at");



CREATE UNIQUE INDEX "agent_jobs_archive_execution_step_id_idx" ON "public"."agent_jobs_archive" USING "btree" ("execution_step_id") WHERE ("execution_step_id" IS NOT NULL);



CREATE UNIQUE INDEX "agent_jobs_archive_expr_idx" ON "public"."agent_jobs_archive" USING "btree" ((("payload" ->> 'dataset_id'::"text"))) WHERE (("job_type" = 'merge'::"text") AND ("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text"])));



CREATE INDEX "agent_jobs_archive_retry_count_idx" ON "public"."agent_jobs_archive" USING "btree" ("retry_count");



CREATE INDEX "agent_jobs_archive_status_created_at_idx" ON "public"."agent_jobs_archive" USING "btree" ("status", "created_at");



CREATE INDEX "chunk_profiles_complexity_idx" ON "public"."chunk_profiles" USING "ivfflat" ("complexity_vector") WITH ("lists"='100');



CREATE UNIQUE INDEX "dataset_merge_locks_active_idx" ON "public"."dataset_merge_locks" USING "btree" ("dataset_id") WHERE ("status" = 'active'::"text");



CREATE INDEX "device_vectors_profile_idx" ON "public"."device_vectors" USING "ivfflat" ("profile_vector") WITH ("lists"='100');



CREATE INDEX "devices_org_idx" ON "public"."devices" USING "btree" ("org_id");



CREATE INDEX "idx_agent_activity_device" ON "public"."agent_worker_activity" USING "btree" ("device_id");



CREATE INDEX "idx_agent_activity_jobtype" ON "public"."agent_worker_activity" USING "btree" ("job_type");



CREATE INDEX "idx_agent_activity_time" ON "public"."agent_worker_activity" USING "btree" ("finished_at");



CREATE INDEX "idx_agent_jobs_agent_completed" ON "public"."agent_jobs" USING "btree" ("agent_id", "completed");



CREATE INDEX "idx_agent_jobs_agent_id" ON "public"."agent_jobs" USING "btree" ("agent_id");



CREATE INDEX "idx_agent_jobs_agent_status" ON "public"."agent_jobs" USING "btree" ("agent_id", "status") WHERE ("status" = ANY (ARRAY['assigned'::"text", 'running'::"text"]));



CREATE INDEX "idx_agent_jobs_archive_agent_id" ON "public"."agent_jobs_archive" USING "btree" ("agent_id");



CREATE INDEX "idx_agent_jobs_archive_created_at" ON "public"."agent_jobs_archive" USING "btree" ("created_at");



CREATE INDEX "idx_agent_jobs_archive_org_id" ON "public"."agent_jobs_archive" USING "btree" ("org_id");



CREATE INDEX "idx_agent_jobs_archive_status" ON "public"."agent_jobs_archive" USING "btree" ("status");



CREATE INDEX "idx_agent_jobs_assigned_at" ON "public"."agent_jobs" USING "btree" ("assigned_at");



CREATE INDEX "idx_agent_jobs_assigned_lease_expires" ON "public"."agent_jobs" USING "btree" ("lease_expires_at") WHERE ("status" = 'assigned'::"text");



CREATE INDEX "idx_agent_jobs_chunk_id" ON "public"."agent_jobs" USING "btree" ("job_chunk_id") WHERE ("job_chunk_id" IS NOT NULL);



CREATE INDEX "idx_agent_jobs_cleanup" ON "public"."agent_jobs" USING "btree" ("status", "lease_expires_at") WHERE ("status" = 'assigned'::"text");



CREATE INDEX "idx_agent_jobs_created_at" ON "public"."agent_jobs" USING "btree" ("created_at");



CREATE INDEX "idx_agent_jobs_dataset_id" ON "public"."agent_jobs" USING "btree" (((("payload" ->> 'dataset_id'::"text"))::"uuid")) WHERE (("payload" ->> 'dataset_id'::"text") ~* '^[0-9a-f-]{36}$'::"text");



CREATE INDEX "idx_agent_jobs_dead_letter_org" ON "public"."agent_jobs_dead_letter" USING "btree" ("org_id", "failed_at");



CREATE INDEX "idx_agent_jobs_execution_id" ON "public"."agent_jobs" USING "btree" ("execution_id");



CREATE INDEX "idx_agent_jobs_execution_mode" ON "public"."agent_jobs" USING "btree" ("execution_mode") WHERE ("status" = 'pending'::"text");



CREATE UNIQUE INDEX "idx_agent_jobs_execution_step_id" ON "public"."agent_jobs" USING "btree" ("execution_step_id") WHERE ("execution_step_id" IS NOT NULL);



CREATE INDEX "idx_agent_jobs_expired_leases" ON "public"."agent_jobs" USING "btree" ("org_id", "status", "lease_expires_at") WHERE (("status" = 'assigned'::"text") AND ("lease_expires_at" IS NOT NULL));



CREATE UNIQUE INDEX "idx_agent_jobs_idempotency_key" ON "public"."agent_jobs" USING "btree" ("org_id", "idempotency_key") WHERE ("idempotency_key" IS NOT NULL);



CREATE INDEX "idx_agent_jobs_lease_expiry" ON "public"."agent_jobs" USING "btree" ("lease_expires_at") WHERE ("status" = 'assigned'::"text");



CREATE INDEX "idx_agent_jobs_org_agent" ON "public"."agent_jobs" USING "btree" ("org_id", "agent_id") WHERE ("agent_id" IS NOT NULL);



CREATE INDEX "idx_agent_jobs_org_id" ON "public"."agent_jobs" USING "btree" ("org_id");



CREATE INDEX "idx_agent_jobs_org_id_partial" ON "public"."agent_jobs" USING "btree" ("org_id") WHERE ("org_id" IS NOT NULL);



CREATE INDEX "idx_agent_jobs_org_status" ON "public"."agent_jobs" USING "btree" ("org_id", "status");



CREATE INDEX "idx_agent_jobs_org_status_created" ON "public"."agent_jobs" USING "btree" ("org_id", "status", "created_at") WHERE ("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text"]));



CREATE INDEX "idx_agent_jobs_pending_org_id" ON "public"."agent_jobs" USING "btree" ("org_id") WHERE ("status" = 'pending'::"text");



CREATE INDEX "idx_agent_jobs_pending_with_agent" ON "public"."agent_jobs" USING "btree" ("org_id", "status", "agent_id", "created_at") WHERE (("status" = 'pending'::"text") AND ("agent_id" IS NOT NULL));



CREATE INDEX "idx_agent_jobs_plan_chunks" ON "public"."agent_jobs" USING "btree" ("job_dataset_id", "job_type", "status") WHERE ("job_type" = 'plan_chunks'::"text");



CREATE INDEX "idx_agent_jobs_ready" ON "public"."agent_jobs" USING "btree" ("agent_id", "status", "assigned_at") WHERE ("completed" = false);



CREATE INDEX "idx_agent_jobs_retry_count" ON "public"."agent_jobs" USING "btree" ("retry_count");



CREATE INDEX "idx_agent_jobs_runtime_type" ON "public"."agent_jobs" USING "btree" ("runtime_type") WHERE ("status" = 'pending'::"text");



CREATE INDEX "idx_agent_jobs_status" ON "public"."agent_jobs" USING "btree" ("status");



CREATE INDEX "idx_agent_jobs_status_created" ON "public"."agent_jobs" USING "btree" ("status", "created_at");



CREATE INDEX "idx_agent_jobs_status_lease" ON "public"."agent_jobs" USING "btree" ("status", "lease_expires_at") WHERE ("status" = 'assigned'::"text");



CREATE INDEX "idx_agent_jobs_status_leases" ON "public"."agent_jobs" USING "btree" ("status", "lease_expires_at") WHERE ("status" = ANY (ARRAY['assigned'::"text", 'running'::"text"]));



CREATE INDEX "idx_agent_jobs_status_org" ON "public"."agent_jobs" USING "btree" ("org_id", "status", "created_at") WHERE ("status" <> ALL (ARRAY['completed'::"text", 'failed'::"text", 'dead_lettered'::"text"]));



CREATE INDEX "idx_agent_jobs_status_org_id" ON "public"."agent_jobs" USING "btree" ("status", "org_id");



CREATE INDEX "idx_agent_metrics_device" ON "public"."agent_metrics" USING "btree" ("device_id");



CREATE INDEX "idx_agent_metrics_device_created_at" ON "public"."agent_metrics" USING "btree" ("device_id", "created_at");



CREATE INDEX "idx_agent_metrics_device_id_created_at" ON "public"."agent_metrics" USING "btree" ("device_id", "created_at" DESC);



CREATE INDEX "idx_agent_metrics_org_created_at" ON "public"."agent_metrics" USING "btree" ("org_id", "created_at");



CREATE INDEX "idx_agent_worker_activity_device_hour" ON "public"."agent_worker_activity" USING "btree" ("device_id", "started_at" DESC);



CREATE INDEX "idx_agent_worker_activity_device_id" ON "public"."agent_worker_activity" USING "btree" ("device_id");



CREATE INDEX "idx_agent_worker_activity_finished" ON "public"."agent_worker_activity" USING "btree" ("finished_at") WHERE ("finished_at" IS NOT NULL);



CREATE INDEX "idx_agent_worker_activity_job_id" ON "public"."agent_worker_activity" USING "btree" ("job_id");



CREATE INDEX "idx_alert_history_org" ON "public"."alert_history" USING "btree" ("org_id", "triggered_at" DESC);



CREATE INDEX "idx_alert_rules_org_enabled" ON "public"."alert_rules" USING "btree" ("org_id", "enabled");



CREATE INDEX "idx_awa_device_finished_at" ON "public"."agent_worker_activity" USING "btree" ("device_id", "finished_at" DESC);



CREATE INDEX "idx_batch_chunks_dataset_status" ON "public"."batch_chunks" USING "btree" ("dataset_id", "org_id", "status") WHERE ("status" = 'pending'::"text");



CREATE INDEX "idx_batch_chunks_device_assignment" ON "public"."batch_chunks" USING "btree" ("assigned_device_id", "status", "dataset_id") WHERE ("assigned_device_id" IS NOT NULL);



CREATE INDEX "idx_batch_chunks_dynamic" ON "public"."batch_chunks" USING "btree" ("dataset_id", "dynamic_size", "status") WHERE (("dynamic_size" = true) AND ("status" = 'pending'::"text"));



CREATE INDEX "idx_batch_chunks_org_dataset" ON "public"."batch_chunks" USING "btree" ("org_id", "dataset_id");



CREATE INDEX "idx_batch_chunks_org_id_partial" ON "public"."batch_chunks" USING "btree" ("org_id") WHERE ("org_id" IS NOT NULL);



CREATE INDEX "idx_batch_chunks_status" ON "public"."batch_chunks" USING "btree" ("status");



CREATE INDEX "idx_bootstrap_rate_limits_client_key" ON "public"."bootstrap_rate_limits" USING "btree" ("client_key");



CREATE INDEX "idx_bootstrap_rate_limits_created_at" ON "public"."bootstrap_rate_limits" USING "btree" ("created_at");



CREATE UNIQUE INDEX "idx_dataset_merge_locks_active" ON "public"."dataset_merge_locks" USING "btree" ("dataset_id") WHERE ("status" = 'active'::"text");



CREATE INDEX "idx_dataset_merge_locks_agent_id" ON "public"."dataset_merge_locks" USING "btree" ("agent_id");



CREATE INDEX "idx_dataset_merge_locks_dataset_id" ON "public"."dataset_merge_locks" USING "btree" ("dataset_id");



CREATE INDEX "idx_dataset_merge_locks_expires_at" ON "public"."dataset_merge_locks" USING "btree" ("expires_at");



CREATE INDEX "idx_dataset_merge_locks_status" ON "public"."dataset_merge_locks" USING "btree" ("status");



CREATE INDEX "idx_datasets_detected_columns" ON "public"."datasets" USING "gin" ("detected_columns") WHERE ("detected_columns" IS NOT NULL);



CREATE INDEX "idx_datasets_id" ON "public"."datasets" USING "btree" ("id");



CREATE INDEX "idx_datasets_merge_status" ON "public"."datasets" USING "btree" ("status") WHERE ("status" = ANY (ARRAY['merge_pending'::"text", 'merging'::"text"]));



CREATE INDEX "idx_datasets_org_id" ON "public"."datasets" USING "btree" ("org_id");



CREATE INDEX "idx_datasets_org_id_partial" ON "public"."datasets" USING "btree" ("org_id") WHERE ("org_id" IS NOT NULL);



CREATE INDEX "idx_datasets_org_status" ON "public"."datasets" USING "btree" ("org_id", "status");



CREATE INDEX "idx_datasets_source_path" ON "public"."datasets" USING "btree" ("org_id", "source_path") WHERE ("source_path" IS NOT NULL);



CREATE INDEX "idx_datasets_status_scanned" ON "public"."datasets" USING "btree" ("id", "status") WHERE ("status" = 'scanned'::"text");



CREATE INDEX "idx_datasets_storage_config" ON "public"."datasets" USING "btree" ("storage_config_id") WHERE ("storage_config_id" IS NOT NULL);



CREATE INDEX "idx_device_benchmarks_device_id" ON "public"."device_benchmarks" USING "btree" ("device_id");



CREATE INDEX "idx_device_claims_code" ON "public"."device_claims" USING "btree" ("claim_code") WHERE ("used_at" IS NULL);



CREATE INDEX "idx_device_events_device_id" ON "public"."device_events" USING "btree" ("device_id", "created_at" DESC);



CREATE INDEX "idx_device_events_pending" ON "public"."device_events" USING "btree" ("status", "created_at");



CREATE INDEX "idx_device_events_status" ON "public"."device_events" USING "btree" ("status") WHERE ("status" = 'pending'::"text");



CREATE INDEX "idx_device_job_perf_device_jobtype" ON "public"."device_job_performance" USING "btree" ("device_id", "job_type", "created_at" DESC);



CREATE INDEX "idx_device_job_type_stats_device" ON "public"."device_job_type_stats" USING "btree" ("device_id", "job_type");



CREATE INDEX "idx_device_learning_history_device_id" ON "public"."device_learning_history" USING "btree" ("device_id", "recorded_at" DESC);



CREATE INDEX "idx_device_policies_org_device" ON "public"."device_policies" USING "btree" ("org_id", "device_id");



CREATE INDEX "idx_device_routing_rules_org" ON "public"."device_routing_rules" USING "btree" ("org_id", "job_type", "action", "enabled");



CREATE INDEX "idx_device_vectors_hnsw" ON "public"."device_vectors" USING "hnsw" ("profile_vector" "public"."vector_cosine_ops") WITH ("m"='16', "ef_construction"='64');



CREATE UNIQUE INDEX "idx_device_vectors_org_device" ON "public"."device_vectors" USING "btree" ("org_id", "device_id");



CREATE INDEX "idx_device_vectors_profile_vector_ivfflat" ON "public"."device_vectors" USING "ivfflat" ("profile_vector") WITH ("lists"='100');



CREATE INDEX "idx_devices_access_token_hash" ON "public"."devices" USING "btree" ("access_token_hash");



CREATE INDEX "idx_devices_auth_lookup" ON "public"."devices" USING "btree" ("access_token_hash") WHERE (("revoked_at" IS NULL) AND ("status" = ANY (ARRAY['online'::"text", 'available'::"text", 'busy'::"text"])));



CREATE INDEX "idx_devices_docker_available" ON "public"."devices" USING "btree" ("docker_available") WHERE ("docker_available" = true);



CREATE INDEX "idx_devices_last_heartbeat" ON "public"."devices" USING "btree" ("last_heartbeat" DESC);



CREATE INDEX "idx_devices_org_id_partial" ON "public"."devices" USING "btree" ("org_id") WHERE ("org_id" IS NOT NULL);



CREATE INDEX "idx_devices_org_id_status" ON "public"."devices" USING "btree" ("org_id", "status");



CREATE INDEX "idx_devices_org_status" ON "public"."devices" USING "btree" ("org_id", "status");



CREATE INDEX "idx_devices_org_status_heartbeat" ON "public"."devices" USING "btree" ("org_id", "status", "last_heartbeat" DESC) WHERE ("status" = ANY (ARRAY['online'::"text", 'available'::"text"]));



CREATE INDEX "idx_devices_os_arch" ON "public"."devices" USING "btree" ("os", "arch");



CREATE INDEX "idx_devices_runtime_supported" ON "public"."devices" USING "gin" ("runtime_supported");



CREATE INDEX "idx_devices_status_heartbeat" ON "public"."devices" USING "btree" ("status", "last_heartbeat");



CREATE INDEX "idx_devices_token_hash" ON "public"."devices" USING "btree" ("access_token_hash") WHERE ("revoked_at" IS NULL);



CREATE INDEX "idx_dismissed_alerts_alert_id" ON "public"."dismissed_alerts" USING "btree" ("alert_id");



CREATE INDEX "idx_dismissed_alerts_org_id" ON "public"."dismissed_alerts" USING "btree" ("org_id");



CREATE INDEX "idx_env_cache_lookup" ON "public"."environment_cache" USING "btree" ("org_id", "runtime_type", "runtime_version", "dependency_lock_hash", "platform") WHERE ("deleted_at" IS NULL);



CREATE INDEX "idx_env_cache_lru" ON "public"."environment_cache" USING "btree" ("last_used_at");



CREATE INDEX "idx_environment_cache_lookup" ON "public"."environment_cache" USING "btree" ("org_id", "runtime_type", "runtime_version", "dependency_lock_hash") WHERE ("deleted_at" IS NULL);



CREATE INDEX "idx_environment_cache_lru" ON "public"."environment_cache" USING "btree" ("last_used_at");



CREATE INDEX "idx_execution_steps_exec_status" ON "public"."execution_steps" USING "btree" ("execution_id", "step_index", "status");



CREATE INDEX "idx_execution_steps_execution" ON "public"."execution_steps" USING "btree" ("execution_id");



CREATE INDEX "idx_execution_steps_execution_id" ON "public"."execution_steps" USING "btree" ("execution_id");



CREATE INDEX "idx_execution_steps_status" ON "public"."execution_steps" USING "btree" ("status");



CREATE INDEX "idx_executions_created_at" ON "public"."executions" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_executions_dataset" ON "public"."executions" USING "btree" ("dataset_id");



CREATE INDEX "idx_executions_org" ON "public"."executions" USING "btree" ("org_id");



CREATE INDEX "idx_executions_org_id_partial" ON "public"."executions" USING "btree" ("org_id") WHERE ("org_id" IS NOT NULL);



CREATE INDEX "idx_executions_org_status" ON "public"."executions" USING "btree" ("org_id", "status");



CREATE INDEX "idx_executions_status_created" ON "public"."executions" USING "btree" ("status", "created_at") WHERE ("status" = 'running'::"text");



CREATE INDEX "idx_http_queue_processed" ON "public"."http_queue" USING "btree" ("processed") WHERE ("processed" = false);



CREATE INDEX "idx_http_queue_processed_age" ON "public"."http_queue" USING "btree" ("processed", "created_at") WHERE ("processed" = false);



CREATE INDEX "idx_http_queue_retry_at" ON "public"."http_queue" USING "btree" ("retry_at") WHERE ("retry_at" IS NOT NULL);



CREATE INDEX "idx_http_queue_unprocessed" ON "public"."http_queue" USING "btree" ("processed", "retry_count", "retry_at") WHERE ("processed" = false);



CREATE INDEX "idx_job_checkpoints_expires" ON "public"."job_checkpoints" USING "btree" ("expires_at") WHERE ("is_completed" = false);



CREATE INDEX "idx_job_checkpoints_job" ON "public"."job_checkpoints" USING "btree" ("job_id");



CREATE INDEX "idx_job_notification_queue_pending" ON "public"."job_notification_queue" USING "btree" ("processed") WHERE ("processed" = false);



CREATE INDEX "idx_job_notification_queue_processed" ON "public"."job_notification_queue" USING "btree" ("processed", "created_at") WHERE ("processed" = true);



CREATE INDEX "idx_jobs_status_org" ON "public"."agent_jobs" USING "btree" ("org_id", "status");



CREATE UNIQUE INDEX "idx_leases_device_active" ON "public"."leases" USING "btree" ("device_id") WHERE ("status" = 'active'::"text");



CREATE INDEX "idx_leases_device_expiry" ON "public"."leases" USING "btree" ("device_id", "lease_expires_at");



CREATE INDEX "idx_leases_device_id_expires_at" ON "public"."leases" USING "btree" ("device_id", "lease_expires_at");



CREATE INDEX "idx_leases_device_status" ON "public"."leases" USING "btree" ("device_id", "status") WHERE ("status" = 'active'::"text");



CREATE INDEX "idx_leases_job" ON "public"."leases" USING "btree" ("job_id");



CREATE INDEX "idx_leases_status_expires" ON "public"."leases" USING "btree" ("status", "lease_expires_at") WHERE ("status" = 'active'::"text");



CREATE UNIQUE INDEX "idx_one_default_per_org" ON "public"."org_storage_configs" USING "btree" ("org_id") WHERE ("is_default" = true);



CREATE INDEX "idx_org_members_org_role" ON "public"."org_members" USING "btree" ("org_id", "role") WHERE ("role" = 'admin'::"text");



CREATE INDEX "idx_org_plugins_enabled" ON "public"."org_plugins" USING "btree" ("enabled");



CREATE INDEX "idx_org_plugins_org_id" ON "public"."org_plugins" USING "btree" ("org_id");



CREATE INDEX "idx_org_storage_configs_default" ON "public"."org_storage_configs" USING "btree" ("org_id", "is_default") WHERE ("is_default" = true);



CREATE INDEX "idx_org_storage_configs_org_id" ON "public"."org_storage_configs" USING "btree" ("org_id");



CREATE INDEX "idx_org_usage_org_period" ON "public"."org_usage" USING "btree" ("org_id", "period_start");



CREATE INDEX "idx_orgs_claim_secret" ON "public"."orgs" USING "btree" ("claim_secret");



CREATE INDEX "idx_pipeline_templates_created_at" ON "public"."pipeline_templates" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_pipeline_templates_dataset_id" ON "public"."pipeline_templates" USING "btree" ("dataset_id");



CREATE INDEX "idx_pipeline_templates_org" ON "public"."pipeline_templates" USING "btree" ("org_id");



CREATE INDEX "idx_plugin_execution_history_created_at" ON "public"."plugin_execution_history" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_plugin_execution_history_device_id" ON "public"."plugin_execution_history" USING "btree" ("device_id");



CREATE INDEX "idx_plugin_execution_history_job_id" ON "public"."plugin_execution_history" USING "btree" ("job_id");



CREATE INDEX "idx_plugin_execution_history_org_id" ON "public"."plugin_execution_history" USING "btree" ("org_id");



CREATE UNIQUE INDEX "idx_plugins_lookup" ON "public"."plugins" USING "btree" ("name", "version", "os", "arch");



CREATE INDEX "idx_plugins_name_version" ON "public"."plugins" USING "btree" ("name", "version");



CREATE INDEX "idx_plugins_os_arch" ON "public"."plugins" USING "btree" ("os", "arch");



CREATE INDEX "idx_runtime_envs_device" ON "public"."runtime_environments" USING "btree" ("device_id");



CREATE INDEX "idx_runtime_envs_org" ON "public"."runtime_environments" USING "btree" ("org_id", "runtime_type");



CREATE INDEX "idx_step_outputs_execution" ON "public"."step_outputs" USING "btree" ("execution_step_id");



CREATE INDEX "idx_system_logs_created_at" ON "public"."system_logs" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_system_logs_event_type" ON "public"."system_logs" USING "btree" ("event_type");



CREATE INDEX "idx_system_logs_event_type_age" ON "public"."system_logs" USING "btree" ("event_type", "created_at");



CREATE INDEX "idx_system_logs_org_id" ON "public"."system_logs" USING "btree" ("org_id");



CREATE INDEX "idx_vector_store_batch_id" ON "public"."vector_store" USING "btree" ("batch_id");



CREATE INDEX "idx_vector_store_dataset_id" ON "public"."vector_store" USING "btree" ("dataset_id");



CREATE INDEX "idx_worker_activity_device_job_time" ON "public"."agent_worker_activity" USING "btree" ("device_id", "job_type", "finished_at" DESC) WHERE ("finished_at" IS NOT NULL);



CREATE UNIQUE INDEX "leases_active_idx" ON "public"."leases" USING "btree" ("job_id") WHERE ("status" = 'active'::"text");



CREATE INDEX "org_members_org_id_user_id_idx" ON "public"."org_members" USING "btree" ("org_id", "user_id");



CREATE INDEX "orgs_auth_id_idx" ON "public"."orgs" USING "btree" ("auth_id");



CREATE UNIQUE INDEX "uniq_active_merge_job" ON "public"."agent_jobs" USING "btree" ((("payload" ->> 'dataset_id'::"text"))) WHERE (("job_type" = 'merge'::"text") AND ("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text"])));



CREATE UNIQUE INDEX "uq_active_merge_job_per_dataset" ON "public"."agent_jobs" USING "btree" ("job_dataset_id") WHERE (("job_type" = 'merge'::"text") AND ("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'running'::"text"])));



CREATE UNIQUE INDEX "uq_leases_device" ON "public"."leases" USING "btree" ("device_id");



CREATE UNIQUE INDEX "uq_one_job_per_chunk" ON "public"."agent_jobs" USING "btree" ("job_chunk_id") WHERE (("job_type" = ANY (ARRAY['preprocess'::"text", 'process'::"text"])) AND ("status" <> ALL (ARRAY['completed'::"text", 'failed'::"text", 'dead'::"text"])));



CREATE UNIQUE INDEX "uq_single_active_lease_per_job" ON "public"."leases" USING "btree" ("job_id") WHERE ("status" = 'active'::"text");



CREATE OR REPLACE TRIGGER "increment_vector_count" AFTER INSERT ON "public"."vector_store" FOR EACH ROW EXECUTE FUNCTION "public"."update_vector_dataset_count"();



CREATE OR REPLACE TRIGGER "org_storage_configs_updated_at" BEFORE UPDATE ON "public"."org_storage_configs" FOR EACH ROW EXECUTE FUNCTION "public"."update_org_storage_configs_updated_at"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."alert_rules" FOR EACH ROW EXECUTE FUNCTION "public"."set_alert_rules_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."enterprise_integrations" FOR EACH ROW EXECUTE FUNCTION "public"."set_enterprise_integrations_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."execution_policies" FOR EACH ROW EXECUTE FUNCTION "public"."set_execution_policies_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."http_queue" FOR EACH ROW EXECUTE FUNCTION "public"."set_http_queue_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."plugin_execution_history" FOR EACH ROW EXECUTE FUNCTION "public"."set_plugin_execution_history_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."plugin_signing_keys" FOR EACH ROW EXECUTE FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."runtime_environments" FOR EACH ROW EXECUTE FUNCTION "public"."set_runtime_environments_org_id_trigger"();



CREATE OR REPLACE TRIGGER "set_org_id_on_insert" BEFORE INSERT ON "public"."vector_datasets" FOR EACH ROW EXECUTE FUNCTION "public"."set_vector_datasets_org_id_trigger"();



CREATE OR REPLACE TRIGGER "touch_device_vector_trigger" AFTER INSERT ON "public"."device_benchmarks" FOR EACH ROW EXECUTE FUNCTION "public"."touch_device_vector"();



CREATE OR REPLACE TRIGGER "trg_calculate_optimal_chunk_size" AFTER INSERT ON "public"."device_benchmarks" FOR EACH ROW EXECUTE FUNCTION "public"."invoke_optimal_chunk_size_calculation"();



CREATE OR REPLACE TRIGGER "trg_create_scan_job_on_insert" AFTER INSERT ON "public"."datasets" FOR EACH ROW EXECUTE FUNCTION "public"."create_scan_job_on_dataset_insert"();



CREATE OR REPLACE TRIGGER "update_pipeline_timestamp" BEFORE UPDATE ON "public"."pipeline_templates" FOR EACH ROW EXECUTE FUNCTION "public"."update_pipeline_timestamp"();



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_agent_id_fkey" FOREIGN KEY ("agent_id") REFERENCES "public"."devices"("id") ON UPDATE CASCADE ON DELETE CASCADE;



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_checkpoint_fk" FOREIGN KEY ("checkpoint_id") REFERENCES "public"."job_checkpoints"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_checkpoint_id_fkey" FOREIGN KEY ("checkpoint_id") REFERENCES "public"."job_checkpoints"("id");



ALTER TABLE ONLY "public"."agent_jobs_dead_letter"
    ADD CONSTRAINT "agent_jobs_dead_letter_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_execution_id_fkey" FOREIGN KEY ("execution_id") REFERENCES "public"."executions"("id");



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_execution_step_id_fkey" FOREIGN KEY ("execution_step_id") REFERENCES "public"."execution_steps"("id");



ALTER TABLE ONLY "public"."agent_jobs"
    ADD CONSTRAINT "agent_jobs_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."agent_metrics"
    ADD CONSTRAINT "agent_metrics_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."agent_metrics"
    ADD CONSTRAINT "agent_metrics_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."agent_worker_activity"
    ADD CONSTRAINT "agent_worker_activity_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."agent_worker_activity"
    ADD CONSTRAINT "agent_worker_activity_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."agent_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."alert_history"
    ADD CONSTRAINT "alert_history_alert_rule_id_fkey" FOREIGN KEY ("alert_rule_id") REFERENCES "public"."alert_rules"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."alert_history"
    ADD CONSTRAINT "alert_history_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."alert_rules"
    ADD CONSTRAINT "alert_rules_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."alert_rules"
    ADD CONSTRAINT "alert_rules_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."batch_chunks"
    ADD CONSTRAINT "batch_chunks_assigned_device_id_fkey" FOREIGN KEY ("assigned_device_id") REFERENCES "public"."devices"("id");



ALTER TABLE ONLY "public"."batch_chunks"
    ADD CONSTRAINT "batch_chunks_batch_id_fkey" FOREIGN KEY ("batch_id") REFERENCES "public"."vector_batches"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."batch_chunks"
    ADD CONSTRAINT "batch_chunks_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."chunk_profiles"
    ADD CONSTRAINT "chunk_profiles_chunk_id_fkey" FOREIGN KEY ("chunk_id") REFERENCES "public"."batch_chunks"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."chunk_profiles"
    ADD CONSTRAINT "chunk_profiles_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."vector_datasets"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."dataset_merge_locks"
    ADD CONSTRAINT "dataset_merge_locks_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."datasets"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."dataset_merge_locks"
    ADD CONSTRAINT "dataset_merge_locks_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."datasets"
    ADD CONSTRAINT "datasets_affinity_device_id_fkey" FOREIGN KEY ("affinity_device_id") REFERENCES "public"."devices"("id");



ALTER TABLE ONLY "public"."datasets"
    ADD CONSTRAINT "datasets_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."datasets"
    ADD CONSTRAINT "datasets_storage_config_id_fkey" FOREIGN KEY ("storage_config_id") REFERENCES "public"."org_storage_configs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."device_benchmarks"
    ADD CONSTRAINT "device_benchmarks_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_benchmarks"
    ADD CONSTRAINT "device_benchmarks_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_events"
    ADD CONSTRAINT "device_events_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_events"
    ADD CONSTRAINT "device_events_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_job_performance"
    ADD CONSTRAINT "device_job_performance_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_job_performance"
    ADD CONSTRAINT "device_job_performance_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_job_type_stats"
    ADD CONSTRAINT "device_job_type_stats_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_job_type_stats"
    ADD CONSTRAINT "device_job_type_stats_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_learning_history"
    ADD CONSTRAINT "device_learning_history_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_policies"
    ADD CONSTRAINT "device_policies_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_policies"
    ADD CONSTRAINT "device_policies_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_routing_rules"
    ADD CONSTRAINT "device_routing_rules_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."device_routing_rules"
    ADD CONSTRAINT "device_routing_rules_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_routing_rules"
    ADD CONSTRAINT "device_routing_rules_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_vectors"
    ADD CONSTRAINT "device_vectors_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."device_vectors"
    ADD CONSTRAINT "device_vectors_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."devices"
    ADD CONSTRAINT "devices_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."enterprise_integrations"
    ADD CONSTRAINT "enterprise_integrations_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."environment_cache"
    ADD CONSTRAINT "environment_cache_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."execution_policies"
    ADD CONSTRAINT "execution_policies_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."execution_steps"
    ADD CONSTRAINT "execution_steps_agent_job_id_fkey" FOREIGN KEY ("agent_job_id") REFERENCES "public"."agent_jobs"("id");



ALTER TABLE ONLY "public"."execution_steps"
    ADD CONSTRAINT "execution_steps_execution_id_fkey" FOREIGN KEY ("execution_id") REFERENCES "public"."executions"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."execution_steps"
    ADD CONSTRAINT "execution_steps_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."executions"
    ADD CONSTRAINT "executions_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."executions"
    ADD CONSTRAINT "executions_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."datasets"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."executions"
    ADD CONSTRAINT "executions_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."executions"
    ADD CONSTRAINT "executions_pipeline_template_id_fkey" FOREIGN KEY ("pipeline_template_id") REFERENCES "public"."pipeline_templates"("id");



ALTER TABLE ONLY "public"."http_queue"
    ADD CONSTRAINT "http_queue_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."job_checkpoints"
    ADD CONSTRAINT "job_checkpoints_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."agent_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."job_checkpoints"
    ADD CONSTRAINT "job_checkpoints_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."leases"
    ADD CONSTRAINT "leases_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON UPDATE CASCADE ON DELETE CASCADE;



ALTER TABLE ONLY "public"."leases"
    ADD CONSTRAINT "leases_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_plugins"
    ADD CONSTRAINT "org_plugins_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_plugins"
    ADD CONSTRAINT "org_plugins_plugin_id_fkey" FOREIGN KEY ("plugin_id") REFERENCES "public"."plugins"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_quotas"
    ADD CONSTRAINT "org_quotas_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_storage_configs"
    ADD CONSTRAINT "org_storage_configs_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_usage"
    ADD CONSTRAINT "org_usage_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."orgs"
    ADD CONSTRAINT "orgs_auth_id_fkey" FOREIGN KEY ("auth_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."pipeline_templates"
    ADD CONSTRAINT "pipeline_templates_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."pipeline_templates"
    ADD CONSTRAINT "pipeline_templates_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."datasets"("id");



ALTER TABLE ONLY "public"."pipeline_templates"
    ADD CONSTRAINT "pipeline_templates_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."plugin_execution_history"
    ADD CONSTRAINT "plugin_execution_history_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."plugin_execution_history"
    ADD CONSTRAINT "plugin_execution_history_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."agent_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."plugin_execution_history"
    ADD CONSTRAINT "plugin_execution_history_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."plugin_execution_history"
    ADD CONSTRAINT "plugin_execution_history_plugin_id_fkey" FOREIGN KEY ("plugin_id") REFERENCES "public"."plugins"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."plugin_signing_keys"
    ADD CONSTRAINT "plugin_signing_keys_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON UPDATE CASCADE ON DELETE CASCADE;



ALTER TABLE ONLY "public"."plugins"
    ADD CONSTRAINT "plugins_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."runtime_environments"
    ADD CONSTRAINT "runtime_environments_device_id_fkey" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."runtime_environments"
    ADD CONSTRAINT "runtime_environments_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."step_outputs"
    ADD CONSTRAINT "step_outputs_execution_step_id_fkey" FOREIGN KEY ("execution_step_id") REFERENCES "public"."execution_steps"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."step_outputs"
    ADD CONSTRAINT "step_outputs_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id");



ALTER TABLE ONLY "public"."vector_batches"
    ADD CONSTRAINT "vector_batches_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."vector_datasets"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."vector_batches"
    ADD CONSTRAINT "vector_batches_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."vector_datasets"
    ADD CONSTRAINT "vector_datasets_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."vector_datasets"
    ADD CONSTRAINT "vector_datasets_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."vector_store"
    ADD CONSTRAINT "vector_store_batch_id_fkey" FOREIGN KEY ("batch_id") REFERENCES "public"."vector_batches"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."vector_store"
    ADD CONSTRAINT "vector_store_chunk_id_fkey" FOREIGN KEY ("chunk_id") REFERENCES "public"."batch_chunks"("id");



ALTER TABLE ONLY "public"."vector_store"
    ADD CONSTRAINT "vector_store_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "public"."vector_datasets"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."vector_store"
    ADD CONSTRAINT "vector_store_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE CASCADE;



CREATE POLICY "Service Role Access" ON "public"."devices" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."agent_jobs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "agent_jobs_all_service" ON "public"."agent_jobs" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."agent_jobs_dead_letter" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "agent_jobs_dead_letter_service_role_all" ON "public"."agent_jobs_dead_letter" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "agent_jobs_delete" ON "public"."agent_jobs" FOR DELETE USING (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "agent_jobs_policy" ON "public"."agent_jobs" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "agent_jobs_select" ON "public"."agent_jobs" FOR SELECT TO "authenticated" USING (("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid"));



CREATE POLICY "agent_jobs_service_role_all" ON "public"."agent_jobs" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."agent_metrics" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "agent_metrics_all_service" ON "public"."agent_metrics" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "agent_metrics_select_org_admins" ON "public"."agent_metrics" FOR SELECT USING ((("org_id" IS NULL) OR ("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid") OR ("auth"."role"() = 'service_role'::"text")));



ALTER TABLE "public"."agent_worker_activity" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "agent_worker_activity_all_service" ON "public"."agent_worker_activity" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "agent_worker_activity_insert" ON "public"."agent_worker_activity" FOR INSERT WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "agent_worker_activity_select" ON "public"."agent_worker_activity" FOR SELECT TO "service_role" USING (true);



ALTER TABLE "public"."batch_chunks" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."bootstrap_rate_limits" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "bootstrap_rate_limits_service_role_all" ON "public"."bootstrap_rate_limits" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."chunk_complexity_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "chunk_complexity_cache_service_role_all" ON "public"."chunk_complexity_cache" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."chunk_profiles" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "chunk_profiles_service_role_all" ON "public"."chunk_profiles" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."dataset_merge_locks" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "dataset_merge_locks_org_members_select" ON "public"."dataset_merge_locks" FOR SELECT USING (((EXISTS ( SELECT 1
   FROM ("public"."devices" "d"
     JOIN "public"."org_members" "m" ON (("m"."org_id" = "d"."org_id")))
  WHERE (("d"."id" = "dataset_merge_locks"."device_id") AND ("m"."user_id" = "auth"."uid"())))) OR ("auth"."role"() = 'service_role'::"text")));



CREATE POLICY "dataset_merge_locks_org_modify" ON "public"."dataset_merge_locks" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "dataset_merge_locks_service_role_all" ON "public"."dataset_merge_locks" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."datasets" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "datasets_insert_own_org" ON "public"."datasets" FOR INSERT WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."org_members" "m"
  WHERE (("m"."user_id" = "auth"."uid"()) AND ("m"."org_id" = "datasets"."org_id")))));



CREATE POLICY "datasets_select_detected_columns" ON "public"."datasets" FOR SELECT USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "datasets_select_own_org" ON "public"."datasets" FOR SELECT USING ((EXISTS ( SELECT 1
   FROM "public"."org_members" "m"
  WHERE (("m"."user_id" = "auth"."uid"()) AND ("m"."org_id" = "datasets"."org_id")))));



CREATE POLICY "datasets_service_role_all" ON "public"."datasets" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "datasets_update_detected_columns" ON "public"."datasets" FOR UPDATE USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "datasets_update_service" ON "public"."datasets" FOR UPDATE TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."device_benchmarks" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_benchmarks_org_select" ON "public"."device_benchmarks" FOR SELECT USING (("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid"));



CREATE POLICY "device_benchmarks_service_role_insert" ON "public"."device_benchmarks" FOR INSERT WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."device_claims" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_claims_policy" ON "public"."device_claims" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."device_events" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_events_org_select" ON "public"."device_events" FOR SELECT USING (("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid"));



CREATE POLICY "device_events_service_role_insert" ON "public"."device_events" FOR INSERT WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "device_job_type_stats_all_service" ON "public"."device_job_type_stats" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."device_learning_history" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_learning_history_service_role_all" ON "public"."device_learning_history" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."device_policies" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_policies_read" ON "public"."device_policies" FOR SELECT USING ((("auth"."role"() = 'service_role'::"text") OR ("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid")));



CREATE POLICY "device_policies_write_admin" ON "public"."device_policies" USING (("auth"."role"() = ANY (ARRAY['service_role'::"text", 'admin'::"text"]))) WITH CHECK (("auth"."role"() = ANY (ARRAY['service_role'::"text", 'admin'::"text"])));



CREATE POLICY "device_read_by_org_members" ON "public"."devices" FOR SELECT USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



ALTER TABLE "public"."device_vectors" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "device_vectors_org_select" ON "public"."device_vectors" FOR SELECT USING ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text")));



CREATE POLICY "device_vectors_service_all" ON "public"."device_vectors" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."devices" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "devices_all_service" ON "public"."devices" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."dismissed_alerts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "dismissed_alerts_delete" ON "public"."dismissed_alerts" FOR DELETE USING (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "dismissed_alerts_insert" ON "public"."dismissed_alerts" FOR INSERT WITH CHECK ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text")));



CREATE POLICY "dismissed_alerts_select" ON "public"."dismissed_alerts" FOR SELECT USING ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text")));



ALTER TABLE "public"."enterprise_integrations" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "enterprise_integrations_admin_modify" ON "public"."enterprise_integrations" USING ((("auth"."role"() = 'service_role'::"text") OR "public"."is_org_admin"("org_id"))) WITH CHECK ((("auth"."role"() = 'service_role'::"text") OR "public"."is_org_admin"("org_id")));



CREATE POLICY "enterprise_integrations_org_members" ON "public"."enterprise_integrations" FOR SELECT USING ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text")));



CREATE POLICY "env_cache_service_role" ON "public"."environment_cache" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."environment_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "environment_cache_org_select" ON "public"."environment_cache" FOR SELECT USING ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text")));



CREATE POLICY "environment_cache_service_role" ON "public"."environment_cache" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."execution_policies" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "execution_policies_org_select" ON "public"."execution_policies" FOR SELECT USING (("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid"));



CREATE POLICY "execution_policies_service_role" ON "public"."execution_policies" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."execution_steps" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "execution_steps_select_own_org" ON "public"."execution_steps" FOR SELECT USING ((EXISTS ( SELECT 1
   FROM "public"."executions" "e"
  WHERE (("e"."id" = "execution_steps"."execution_id") AND ("e"."org_id" = (("auth"."jwt"() ->> 'org_id'::"text"))::"uuid")))));



CREATE POLICY "execution_steps_write_service_role" ON "public"."execution_steps" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."executions" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "executions_all_service" ON "public"."executions" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "executions_select_own_org" ON "public"."executions" FOR SELECT USING (("org_id" = (("auth"."jwt"() ->> 'org_id'::"text"))::"uuid"));



CREATE POLICY "executions_service_role_all" ON "public"."executions" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."http_queue" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "http_queue_service_role_all" ON "public"."http_queue" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."job_checkpoints" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "job_checkpoints_service_role" ON "public"."job_checkpoints" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."job_notification_queue" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "job_notification_queue_org_select" ON "public"."job_notification_queue" FOR SELECT USING ((("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))) OR ("auth"."role"() = 'service_role'::"text") OR ("agent_id" IN ( SELECT "d"."id"
   FROM ("public"."devices" "d"
     JOIN "public"."org_members" "m" ON (("m"."org_id" = "d"."org_id")))
  WHERE ("m"."user_id" = "auth"."uid"())))));



CREATE POLICY "job_notification_queue_service_delete" ON "public"."job_notification_queue" FOR DELETE USING (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "job_notification_queue_service_insert" ON "public"."job_notification_queue" FOR INSERT WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "job_notification_queue_service_update" ON "public"."job_notification_queue" FOR UPDATE USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."leases" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "leases_all_service" ON "public"."leases" TO "service_role" USING (true) WITH CHECK (true);



CREATE POLICY "org members can read org" ON "public"."orgs" FOR SELECT USING (("id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



ALTER TABLE "public"."org_members" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org_members_admin_delete" ON "public"."org_members" FOR DELETE USING (("public"."is_org_admin"("org_id") AND ("user_id" <> "auth"."uid"()) AND ("role" <> 'admin'::"text")));



CREATE POLICY "org_members_insert_self" ON "public"."org_members" FOR INSERT WITH CHECK (("user_id" = "auth"."uid"()));



CREATE POLICY "org_members_select_own_org" ON "public"."org_members" FOR SELECT USING ("public"."is_org_member"("org_id"));



CREATE POLICY "org_members_select_self" ON "public"."org_members" FOR SELECT USING (("user_id" = "auth"."uid"()));



ALTER TABLE "public"."org_plugins" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org_plugins_members_read" ON "public"."org_plugins" FOR SELECT USING ((("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid") AND (EXISTS ( SELECT 1
   FROM "public"."org_members"
  WHERE (("org_members"."org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid") AND ("org_members"."user_id" = ("current_setting"('jwt.claims.user_id'::"text", true))::"uuid"))))));



CREATE POLICY "org_plugins_members_update" ON "public"."org_plugins" FOR UPDATE USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"())))) WITH CHECK (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "org_plugins_service_role_all" ON "public"."org_plugins" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."org_quotas" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org_quotas_service_role" ON "public"."org_quotas" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."org_storage_configs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org_storage_configs_delete_admin" ON "public"."org_storage_configs" FOR DELETE USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE (("org_members"."user_id" = "auth"."uid"()) AND ("org_members"."role" = 'admin'::"text")))));



CREATE POLICY "org_storage_configs_insert_admin_org" ON "public"."org_storage_configs" FOR INSERT WITH CHECK ((("auth"."role"() = 'service_role'::"text") OR ("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE (("org_members"."user_id" = "auth"."uid"()) AND ("org_members"."role" = 'admin'::"text"))))));



CREATE POLICY "org_storage_configs_select_own_org" ON "public"."org_storage_configs" FOR SELECT USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "org_storage_configs_update_admin_org" ON "public"."org_storage_configs" FOR UPDATE USING ((("auth"."role"() = 'service_role'::"text") OR ("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE (("org_members"."user_id" = "auth"."uid"()) AND ("org_members"."role" = 'admin'::"text"))))));



CREATE POLICY "org_update_devices" ON "public"."devices" FOR UPDATE USING ((("auth"."role"() = 'service_role'::"text") OR ("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid"))) WITH CHECK ((("auth"."role"() = 'service_role'::"text") OR ("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid")));



ALTER TABLE "public"."org_usage" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org_usage_service_role" ON "public"."org_usage" USING (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."orgs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "orgs_admin_update" ON "public"."orgs" FOR UPDATE USING (("id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE (("org_members"."user_id" = "auth"."uid"()) AND ("org_members"."role" = 'admin'::"text"))))) WITH CHECK (("id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE (("org_members"."user_id" = "auth"."uid"()) AND ("org_members"."role" = 'admin'::"text")))));



CREATE POLICY "orgs_insert_authenticated" ON "public"."orgs" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() IS NOT NULL));



ALTER TABLE "public"."pipeline_templates" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "pipeline_templates_insert" ON "public"."pipeline_templates" FOR INSERT WITH CHECK (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "pipeline_templates_insert_own_org" ON "public"."pipeline_templates" FOR INSERT WITH CHECK (("org_id" = (("auth"."jwt"() ->> 'org_id'::"text"))::"uuid"));



CREATE POLICY "pipeline_templates_select" ON "public"."pipeline_templates" FOR SELECT USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



CREATE POLICY "pipeline_templates_select_own_org" ON "public"."pipeline_templates" FOR SELECT USING (("org_id" = (("auth"."jwt"() ->> 'org_id'::"text"))::"uuid"));



CREATE POLICY "pipeline_templates_update" ON "public"."pipeline_templates" FOR UPDATE USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



ALTER TABLE "public"."plugin_execution_history" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "plugin_execution_history_members_read" ON "public"."plugin_execution_history" FOR SELECT USING ((("org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid") AND (EXISTS ( SELECT 1
   FROM "public"."org_members"
  WHERE (("org_members"."org_id" = ("current_setting"('jwt.claims.org_id'::"text", true))::"uuid") AND ("org_members"."user_id" = ("current_setting"('jwt.claims.user_id'::"text", true))::"uuid"))))));



CREATE POLICY "plugin_execution_history_service_role_all" ON "public"."plugin_execution_history" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "plugin_signing_keys_members_read" ON "public"."plugin_signing_keys" FOR SELECT USING (("org_id" IN ( SELECT "org_members"."org_id"
   FROM "public"."org_members"
  WHERE ("org_members"."user_id" = "auth"."uid"()))));



ALTER TABLE "public"."plugins" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "plugins_members_read" ON "public"."plugins" FOR SELECT USING ((EXISTS ( SELECT 1
   FROM ("public"."org_plugins" "op"
     JOIN "public"."org_members" "om" ON (("om"."org_id" = "op"."org_id")))
  WHERE (("op"."plugin_id" = "plugins"."id") AND ("om"."user_id" = "auth"."uid"())))));



CREATE POLICY "plugins_service_role_all" ON "public"."plugins" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."runtime_environments" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "runtime_envs_service_role" ON "public"."runtime_environments" USING (("auth"."role"() = 'service_role'::"text"));



CREATE POLICY "service_role_full_access" ON "public"."device_claims" USING (true) WITH CHECK (true);



ALTER TABLE "public"."system_logs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "system_logs_audit_read" ON "public"."system_logs" FOR SELECT USING ((("auth"."role"() = 'service_role'::"text") OR ("event_type" = ANY ("public"."get_audit_event_types"()))));



CREATE POLICY "system_logs_service_role_only" ON "public"."system_logs" USING (("auth"."role"() = 'service_role'::"text")) WITH CHECK (("auth"."role"() = 'service_role'::"text"));



ALTER TABLE "public"."vector_batches" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."vector_datasets" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."vector_store" ENABLE ROW LEVEL SECURITY;




ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";






ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."agent_jobs";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."batch_chunks";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."datasets";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."device_benchmarks";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."device_vectors";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."devices";



ALTER PUBLICATION "supabase_realtime" ADD TABLE ONLY "public"."vector_datasets";



GRANT USAGE ON SCHEMA "_internal" TO "service_role";



SET SESSION AUTHORIZATION "postgres";
RESET SESSION AUTHORIZATION;






GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey16_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey16_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey16_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey16_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey16_out"("public"."gbtreekey16") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey16_out"("public"."gbtreekey16") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey16_out"("public"."gbtreekey16") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey16_out"("public"."gbtreekey16") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey2_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey2_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey2_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey2_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey2_out"("public"."gbtreekey2") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey2_out"("public"."gbtreekey2") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey2_out"("public"."gbtreekey2") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey2_out"("public"."gbtreekey2") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey32_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey32_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey32_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey32_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey32_out"("public"."gbtreekey32") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey32_out"("public"."gbtreekey32") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey32_out"("public"."gbtreekey32") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey32_out"("public"."gbtreekey32") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey4_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey4_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey4_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey4_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey4_out"("public"."gbtreekey4") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey4_out"("public"."gbtreekey4") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey4_out"("public"."gbtreekey4") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey4_out"("public"."gbtreekey4") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey8_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey8_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey8_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey8_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey8_out"("public"."gbtreekey8") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey8_out"("public"."gbtreekey8") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey8_out"("public"."gbtreekey8") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey8_out"("public"."gbtreekey8") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey_var_in"("cstring") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_in"("cstring") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_in"("cstring") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_in"("cstring") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbtreekey_var_out"("public"."gbtreekey_var") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_out"("public"."gbtreekey_var") TO "anon";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_out"("public"."gbtreekey_var") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbtreekey_var_out"("public"."gbtreekey_var") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "_internal"."list_public_functions"() TO "service_role";



GRANT ALL ON FUNCTION "_internal"."sentra_rls_table_status"() TO "service_role";




































































































































































































































GRANT ALL ON FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."acquire_dataset_merge_lock"("p_dataset_id" "uuid", "p_agent_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_duration_minutes" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."acquire_lease"("p_job_id" "uuid", "p_org_id" "uuid", "p_device_id" "uuid", "p_ttl_seconds" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."acquire_lease"("p_job_id" "uuid", "p_org_id" "uuid", "p_device_id" "uuid", "p_ttl_seconds" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."acquire_lease"("p_job_id" "uuid", "p_org_id" "uuid", "p_device_id" "uuid", "p_ttl_seconds" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."activate_pipeline"("p_pipeline_template_id" "uuid", "p_dataset_id" "uuid", "p_org_id" "uuid", "p_created_by" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."activate_pipeline"("p_pipeline_template_id" "uuid", "p_dataset_id" "uuid", "p_org_id" "uuid", "p_created_by" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."activate_pipeline"("p_pipeline_template_id" "uuid", "p_dataset_id" "uuid", "p_org_id" "uuid", "p_created_by" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."advance_pipeline_on_job_complete"() TO "anon";
GRANT ALL ON FUNCTION "public"."advance_pipeline_on_job_complete"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."advance_pipeline_on_job_complete"() TO "service_role";



GRANT ALL ON FUNCTION "public"."apply_routing_rules"("p_org_id" "uuid", "p_job_type" "text", "p_candidate_device_ids" "uuid"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."apply_routing_rules"("p_org_id" "uuid", "p_job_type" "text", "p_candidate_device_ids" "uuid"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."apply_routing_rules"("p_org_id" "uuid", "p_job_type" "text", "p_candidate_device_ids" "uuid"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid", "p_agent_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid", "p_agent_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_org_id" "uuid", "p_agent_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."assign_agent_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."assign_best_job_to_best_device"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."assign_best_job_to_best_device"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."assign_best_job_to_best_device"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."assign_chunk_job_on_insert"() TO "anon";
GRANT ALL ON FUNCTION "public"."assign_chunk_job_on_insert"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."assign_chunk_job_on_insert"() TO "service_role";



GRANT ALL ON FUNCTION "public"."auto_assign_merge_job"() TO "anon";
GRANT ALL ON FUNCTION "public"."auto_assign_merge_job"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."auto_assign_merge_job"() TO "service_role";



GRANT ALL ON FUNCTION "public"."auto_create_agent_job"() TO "anon";
GRANT ALL ON FUNCTION "public"."auto_create_agent_job"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."auto_create_agent_job"() TO "service_role";



GRANT ALL ON FUNCTION "public"."auto_progress_after_scan"() TO "anon";
GRANT ALL ON FUNCTION "public"."auto_progress_after_scan"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."auto_progress_after_scan"() TO "service_role";



GRANT ALL ON FUNCTION "public"."auto_rotate_stale_tokens"() TO "anon";
GRANT ALL ON FUNCTION "public"."auto_rotate_stale_tokens"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."auto_rotate_stale_tokens"() TO "service_role";



GRANT ALL ON FUNCTION "public"."batch_assign_jobs_atomic"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_job_type_filter" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."batch_assign_jobs_atomic"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_job_type_filter" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."batch_assign_jobs_atomic"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_job_type_filter" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."calculate_dependency_hash"("p_runtime_type" "text", "p_runtime_dependencies" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."calculate_dependency_hash"("p_runtime_type" "text", "p_runtime_dependencies" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."calculate_dependency_hash"("p_runtime_type" "text", "p_runtime_dependencies" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."calculate_retry_backoff"("p_retry_count" integer, "p_base_delay_seconds" integer, "p_max_delay_seconds" integer, "p_multiplier" numeric) TO "anon";
GRANT ALL ON FUNCTION "public"."calculate_retry_backoff"("p_retry_count" integer, "p_base_delay_seconds" integer, "p_max_delay_seconds" integer, "p_multiplier" numeric) TO "authenticated";
GRANT ALL ON FUNCTION "public"."calculate_retry_backoff"("p_retry_count" integer, "p_base_delay_seconds" integer, "p_max_delay_seconds" integer, "p_multiplier" numeric) TO "service_role";



GRANT ALL ON FUNCTION "public"."cash_dist"("money", "money") TO "postgres";
GRANT ALL ON FUNCTION "public"."cash_dist"("money", "money") TO "anon";
GRANT ALL ON FUNCTION "public"."cash_dist"("money", "money") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cash_dist"("money", "money") TO "service_role";



GRANT ALL ON FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."check_and_set_policy_cooldown"("p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."check_http_queue_depth"() TO "anon";
GRANT ALL ON FUNCTION "public"."check_http_queue_depth"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."check_http_queue_depth"() TO "service_role";



GRANT ALL ON FUNCTION "public"."check_org_quota"("p_org_id" "uuid", "p_quota_type" "text", "p_value" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."check_org_quota"("p_org_id" "uuid", "p_quota_type" "text", "p_value" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."check_org_quota"("p_org_id" "uuid", "p_quota_type" "text", "p_value" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."check_plan_limit"("p_org_id" "uuid", "p_limit_type" "text", "p_increment" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."check_plan_limit"("p_org_id" "uuid", "p_limit_type" "text", "p_increment" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."check_plan_limit"("p_org_id" "uuid", "p_limit_type" "text", "p_increment" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."check_platform_signing_configured"() TO "anon";
GRANT ALL ON FUNCTION "public"."check_platform_signing_configured"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."check_platform_signing_configured"() TO "service_role";



GRANT ALL ON FUNCTION "public"."claim_job_with_compatibility"("p_org_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."claim_job_with_compatibility"("p_org_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."claim_job_with_compatibility"("p_org_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."claim_jobs_for_device"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_lease_ttl_seconds" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."claim_jobs_for_device"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_lease_ttl_seconds" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."claim_jobs_for_device"("p_device_id" "uuid", "p_org_id" "uuid", "p_limit" integer, "p_lease_ttl_seconds" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."claim_next_job_for_device"("p_org_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."claim_next_job_for_device"("p_org_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."claim_next_job_for_device"("p_org_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_agent_worker_activity"("p_days_old" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_agent_worker_activity"("p_days_old" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_agent_worker_activity"("p_days_old" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_duplicate_cron_jobs"() TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_duplicate_cron_jobs"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_duplicate_cron_jobs"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_expired_merge_locks"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_expired_merge_locks"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_expired_merge_locks"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_job_notification_queue"("p_days_old" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_job_notification_queue"("p_days_old" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_job_notification_queue"("p_days_old" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_leases_on_offline"() TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_leases_on_offline"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_leases_on_offline"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_offline_device_leases"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_offline_device_leases"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_offline_device_leases"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_old_agent_jobs"() TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_old_agent_jobs"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_old_agent_jobs"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_old_benchmarks"() TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_old_benchmarks"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_old_benchmarks"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"() TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"("p_max_retries" integer, "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"("p_max_retries" integer, "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cleanup_stuck_jobs"("p_max_retries" integer, "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint, "p_result" "jsonb", "p_device_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint, "p_result" "jsonb", "p_device_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."complete_job_idempotent"("p_job_id" "uuid", "p_status" "text", "p_duration_ms" bigint, "p_result" "jsonb", "p_device_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."compute_agent_job_hashes"() TO "anon";
GRANT ALL ON FUNCTION "public"."compute_agent_job_hashes"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."compute_agent_job_hashes"() TO "service_role";



GRANT ALL ON FUNCTION "public"."compute_dependency_lock_hash"() TO "anon";
GRANT ALL ON FUNCTION "public"."compute_dependency_lock_hash"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."compute_dependency_lock_hash"() TO "service_role";



GRANT ALL ON FUNCTION "public"."compute_job_lock_hash"() TO "anon";
GRANT ALL ON FUNCTION "public"."compute_job_lock_hash"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."compute_job_lock_hash"() TO "service_role";



GRANT ALL ON FUNCTION "public"."compute_plugin_checksum"("p_storage_path" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."compute_plugin_checksum"("p_storage_path" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."compute_plugin_checksum"("p_storage_path" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."consolidated_dispatch"() TO "anon";
GRANT ALL ON FUNCTION "public"."consolidated_dispatch"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."consolidated_dispatch"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."create_chunks_from_plan_job"() TO "anon";
GRANT ALL ON FUNCTION "public"."create_chunks_from_plan_job"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."create_chunks_from_plan_job"() TO "service_role";



GRANT ALL ON FUNCTION "public"."create_org_with_owner"("org_name" "text", "team_size" integer, "member_name" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."create_org_with_owner"("org_name" "text", "team_size" integer, "member_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."create_org_with_owner"("org_name" "text", "team_size" integer, "member_name" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."create_scan_job_on_dataset_insert"() TO "anon";
GRANT ALL ON FUNCTION "public"."create_scan_job_on_dataset_insert"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."create_scan_job_on_dataset_insert"() TO "service_role";



GRANT ALL ON FUNCTION "public"."date_dist"("date", "date") TO "postgres";
GRANT ALL ON FUNCTION "public"."date_dist"("date", "date") TO "anon";
GRANT ALL ON FUNCTION "public"."date_dist"("date", "date") TO "authenticated";
GRANT ALL ON FUNCTION "public"."date_dist"("date", "date") TO "service_role";



GRANT ALL ON FUNCTION "public"."decrypt_vault_secret"("secret_name" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."decrypt_vault_secret"("secret_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."decrypt_vault_secret"("secret_name" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."device_has_warm_environment"("p_device_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_lock_hash" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."device_has_warm_environment"("p_device_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_lock_hash" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."device_has_warm_environment"("p_device_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_lock_hash" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."device_matches_requirements"("p_device_id" "uuid", "p_runtime_type" "text", "p_min_python_version" "text", "p_required_arch" "text", "p_required_os" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."device_matches_requirements"("p_device_id" "uuid", "p_runtime_type" "text", "p_min_python_version" "text", "p_required_arch" "text", "p_required_os" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."device_matches_requirements"("p_device_id" "uuid", "p_runtime_type" "text", "p_min_python_version" "text", "p_required_arch" "text", "p_required_os" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."device_supports_execution_mode"("p_device_id" "uuid", "p_mode" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."device_supports_execution_mode"("p_device_id" "uuid", "p_mode" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."device_supports_execution_mode"("p_device_id" "uuid", "p_mode" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."device_supports_runtime"("p_device_id" "uuid", "p_runtime_type" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."device_supports_runtime"("p_device_id" "uuid", "p_runtime_type" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."device_supports_runtime"("p_device_id" "uuid", "p_runtime_type" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."dispatch_http_jobs_secure"() TO "anon";
GRANT ALL ON FUNCTION "public"."dispatch_http_jobs_secure"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."dispatch_http_jobs_secure"() TO "service_role";



GRANT ALL ON FUNCTION "public"."elect_merge_device"("_org_id" "uuid", "_affinity_device_id" "uuid", "_preferred_network_zone" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."elect_merge_device"("_org_id" "uuid", "_affinity_device_id" "uuid", "_preferred_network_zone" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."elect_merge_device"("_org_id" "uuid", "_affinity_device_id" "uuid", "_preferred_network_zone" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."encode_plugin_signature"("sig" "bytea") TO "anon";
GRANT ALL ON FUNCTION "public"."encode_plugin_signature"("sig" "bytea") TO "authenticated";
GRANT ALL ON FUNCTION "public"."encode_plugin_signature"("sig" "bytea") TO "service_role";



GRANT ALL ON FUNCTION "public"."enqueue_device_online_event"() TO "anon";
GRANT ALL ON FUNCTION "public"."enqueue_device_online_event"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."enqueue_device_online_event"() TO "service_role";



GRANT ALL ON FUNCTION "public"."evaluate_alert_rules"() TO "anon";
GRANT ALL ON FUNCTION "public"."evaluate_alert_rules"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."evaluate_alert_rules"() TO "service_role";



GRANT ALL ON FUNCTION "public"."float4_dist"(real, real) TO "postgres";
GRANT ALL ON FUNCTION "public"."float4_dist"(real, real) TO "anon";
GRANT ALL ON FUNCTION "public"."float4_dist"(real, real) TO "authenticated";
GRANT ALL ON FUNCTION "public"."float4_dist"(real, real) TO "service_role";



GRANT ALL ON FUNCTION "public"."float8_dist"(double precision, double precision) TO "postgres";
GRANT ALL ON FUNCTION "public"."float8_dist"(double precision, double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."float8_dist"(double precision, double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."float8_dist"(double precision, double precision) TO "service_role";



GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."force_assign_job"("p_job_id" "text", "p_agent_id" "text", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_consistent"("internal", bit, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_consistent"("internal", bit, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_consistent"("internal", bit, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_consistent"("internal", bit, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bit_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bit_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bit_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bit_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_consistent"("internal", boolean, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_consistent"("internal", boolean, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_consistent"("internal", boolean, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_consistent"("internal", boolean, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_same"("public"."gbtreekey2", "public"."gbtreekey2", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_same"("public"."gbtreekey2", "public"."gbtreekey2", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_same"("public"."gbtreekey2", "public"."gbtreekey2", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_same"("public"."gbtreekey2", "public"."gbtreekey2", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bool_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bool_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bool_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bool_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bpchar_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bpchar_consistent"("internal", character, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_consistent"("internal", character, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_consistent"("internal", character, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bpchar_consistent"("internal", character, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_consistent"("internal", "bytea", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_consistent"("internal", "bytea", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_consistent"("internal", "bytea", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_consistent"("internal", "bytea", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_bytea_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_bytea_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_bytea_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_bytea_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_consistent"("internal", "money", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_consistent"("internal", "money", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_consistent"("internal", "money", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_consistent"("internal", "money", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_distance"("internal", "money", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_distance"("internal", "money", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_distance"("internal", "money", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_distance"("internal", "money", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_cash_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_cash_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_cash_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_cash_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_consistent"("internal", "date", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_consistent"("internal", "date", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_consistent"("internal", "date", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_consistent"("internal", "date", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_distance"("internal", "date", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_distance"("internal", "date", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_distance"("internal", "date", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_distance"("internal", "date", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_date_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_date_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_date_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_date_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_decompress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_decompress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_decompress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_decompress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_consistent"("internal", "anyenum", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_consistent"("internal", "anyenum", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_consistent"("internal", "anyenum", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_consistent"("internal", "anyenum", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_enum_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_enum_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_enum_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_enum_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_consistent"("internal", real, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_consistent"("internal", real, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_consistent"("internal", real, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_consistent"("internal", real, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_distance"("internal", real, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_distance"("internal", real, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_distance"("internal", real, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_distance"("internal", real, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float4_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float4_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float4_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float4_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_consistent"("internal", double precision, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_consistent"("internal", double precision, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_consistent"("internal", double precision, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_consistent"("internal", double precision, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_distance"("internal", double precision, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_distance"("internal", double precision, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_distance"("internal", double precision, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_distance"("internal", double precision, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_float8_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_float8_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_float8_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_float8_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_consistent"("internal", "inet", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_consistent"("internal", "inet", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_consistent"("internal", "inet", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_consistent"("internal", "inet", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_inet_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_inet_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_inet_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_inet_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_consistent"("internal", smallint, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_consistent"("internal", smallint, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_consistent"("internal", smallint, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_consistent"("internal", smallint, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_distance"("internal", smallint, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_distance"("internal", smallint, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_distance"("internal", smallint, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_distance"("internal", smallint, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_same"("public"."gbtreekey4", "public"."gbtreekey4", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_same"("public"."gbtreekey4", "public"."gbtreekey4", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_same"("public"."gbtreekey4", "public"."gbtreekey4", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_same"("public"."gbtreekey4", "public"."gbtreekey4", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int2_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int2_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int2_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int2_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_consistent"("internal", integer, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_consistent"("internal", integer, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_consistent"("internal", integer, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_consistent"("internal", integer, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_distance"("internal", integer, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_distance"("internal", integer, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_distance"("internal", integer, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_distance"("internal", integer, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int4_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int4_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int4_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int4_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_consistent"("internal", bigint, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_consistent"("internal", bigint, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_consistent"("internal", bigint, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_consistent"("internal", bigint, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_distance"("internal", bigint, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_distance"("internal", bigint, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_distance"("internal", bigint, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_distance"("internal", bigint, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_int8_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_int8_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_int8_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_int8_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_consistent"("internal", interval, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_consistent"("internal", interval, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_consistent"("internal", interval, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_consistent"("internal", interval, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_decompress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_decompress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_decompress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_decompress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_distance"("internal", interval, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_distance"("internal", interval, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_distance"("internal", interval, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_distance"("internal", interval, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_intv_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_intv_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_intv_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_intv_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_consistent"("internal", "macaddr8", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_consistent"("internal", "macaddr8", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_consistent"("internal", "macaddr8", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_consistent"("internal", "macaddr8", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad8_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad8_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad8_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad8_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_consistent"("internal", "macaddr", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_consistent"("internal", "macaddr", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_consistent"("internal", "macaddr", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_consistent"("internal", "macaddr", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_macad_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_macad_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_macad_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_macad_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_consistent"("internal", numeric, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_consistent"("internal", numeric, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_consistent"("internal", numeric, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_consistent"("internal", numeric, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_numeric_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_numeric_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_numeric_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_numeric_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_consistent"("internal", "oid", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_consistent"("internal", "oid", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_consistent"("internal", "oid", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_consistent"("internal", "oid", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_distance"("internal", "oid", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_distance"("internal", "oid", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_distance"("internal", "oid", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_distance"("internal", "oid", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_same"("public"."gbtreekey8", "public"."gbtreekey8", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_oid_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_oid_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_oid_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_oid_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_consistent"("internal", "text", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_consistent"("internal", "text", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_consistent"("internal", "text", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_consistent"("internal", "text", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_same"("public"."gbtreekey_var", "public"."gbtreekey_var", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_text_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_text_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_text_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_text_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_consistent"("internal", time without time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_consistent"("internal", time without time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_consistent"("internal", time without time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_consistent"("internal", time without time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_distance"("internal", time without time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_distance"("internal", time without time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_distance"("internal", time without time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_distance"("internal", time without time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_time_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_time_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_time_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_time_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_timetz_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_timetz_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_timetz_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_timetz_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_timetz_consistent"("internal", time with time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_timetz_consistent"("internal", time with time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_timetz_consistent"("internal", time with time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_timetz_consistent"("internal", time with time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_consistent"("internal", timestamp without time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_consistent"("internal", timestamp without time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_consistent"("internal", timestamp without time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_consistent"("internal", timestamp without time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_distance"("internal", timestamp without time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_distance"("internal", timestamp without time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_distance"("internal", timestamp without time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_distance"("internal", timestamp without time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_same"("public"."gbtreekey16", "public"."gbtreekey16", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_ts_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_ts_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_ts_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_ts_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_tstz_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_tstz_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_tstz_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_tstz_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_tstz_consistent"("internal", timestamp with time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_tstz_consistent"("internal", timestamp with time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_tstz_consistent"("internal", timestamp with time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_tstz_consistent"("internal", timestamp with time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_tstz_distance"("internal", timestamp with time zone, smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_tstz_distance"("internal", timestamp with time zone, smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_tstz_distance"("internal", timestamp with time zone, smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_tstz_distance"("internal", timestamp with time zone, smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_compress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_compress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_compress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_compress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_consistent"("internal", "uuid", smallint, "oid", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_consistent"("internal", "uuid", smallint, "oid", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_consistent"("internal", "uuid", smallint, "oid", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_consistent"("internal", "uuid", smallint, "oid", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_penalty"("internal", "internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_penalty"("internal", "internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_penalty"("internal", "internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_penalty"("internal", "internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_picksplit"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_picksplit"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_picksplit"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_picksplit"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_same"("public"."gbtreekey32", "public"."gbtreekey32", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_uuid_union"("internal", "internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_uuid_union"("internal", "internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_uuid_union"("internal", "internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_uuid_union"("internal", "internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_var_decompress"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_var_decompress"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_var_decompress"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_var_decompress"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."gbt_var_fetch"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."gbt_var_fetch"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."gbt_var_fetch"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gbt_var_fetch"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_advisory_lock_key"("p_uuid" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_advisory_lock_key"("p_uuid" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_advisory_lock_key"("p_uuid" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_agent_job_stats"("p_agent_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_agent_job_stats"("p_agent_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_agent_job_stats"("p_agent_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_agent_metrics_aggregate"("p_agent_id" "uuid", "p_time_range" "text", "p_interval" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_agent_metrics_aggregate"("p_agent_id" "uuid", "p_time_range" "text", "p_interval" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_agent_metrics_aggregate"("p_agent_id" "uuid", "p_time_range" "text", "p_interval" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_alert_rules"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_alert_rules"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_alert_rules"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_audit_event_types"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_audit_event_types"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_audit_event_types"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_constraints_report"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_constraints_report"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_constraints_report"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_current_org_id"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_current_org_id"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_current_org_id"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_dashboard_stats"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_dashboard_stats"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_dashboard_stats"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_dashboard_summary"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_dashboard_summary"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_dashboard_summary"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_dataset_executions"("p_dataset_id" "uuid", "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_dataset_executions"("p_dataset_id" "uuid", "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_dataset_executions"("p_dataset_id" "uuid", "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."get_device_by_id"("p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_device_by_id"("p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_device_by_id"("p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_device_job_history"("p_device_id" "uuid", "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_device_job_history"("p_device_id" "uuid", "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_device_job_history"("p_device_id" "uuid", "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."get_device_job_stats"("p_device_id" "uuid", "p_job_type" "text", "p_window_hours" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_device_job_stats"("p_device_id" "uuid", "p_job_type" "text", "p_window_hours" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_device_job_stats"("p_device_id" "uuid", "p_job_type" "text", "p_window_hours" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."get_device_rankings"("org_id" "uuid", "job_type" "text", "chunk_vector" "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."get_device_rankings"("org_id" "uuid", "job_type" "text", "chunk_vector" "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_device_rankings"("org_id" "uuid", "job_type" "text", "chunk_vector" "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_device_runtime_cache_summary"("p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_device_runtime_cache_summary"("p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_device_runtime_cache_summary"("p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_edge_url"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_edge_url"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_edge_url"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_execution_detail"("p_execution_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_execution_detail"("p_execution_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_execution_detail"("p_execution_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_fleet_health"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_fleet_health"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_fleet_health"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_functions_report"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_functions_report"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_functions_report"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_job_checkpoints"("p_job_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_job_checkpoints"("p_job_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_job_checkpoints"("p_job_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_job_detail"("p_job_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_job_detail"("p_job_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_job_detail"("p_job_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_or_create_runtime_environment"("p_org_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_hash" "text", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_or_create_runtime_environment"("p_org_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_hash" "text", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_or_create_runtime_environment"("p_org_id" "uuid", "p_runtime_type" "text", "p_runtime_version" "text", "p_dependency_hash" "text", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer, "p_event_type_filter" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer, "p_event_type_filter" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_org_audit_log"("p_org_id" "uuid", "p_limit" integer, "p_event_type_filter" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_org_plugins"("p_org_id" "uuid", "p_os" "text", "p_arch" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_org_plugins"("p_org_id" "uuid", "p_os" "text", "p_arch" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_org_plugins"("p_org_id" "uuid", "p_os" "text", "p_arch" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_org_storage_configs"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_org_storage_configs"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_org_storage_configs"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_pipeline_status"("p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_pipeline_status"("p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_pipeline_status"("p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_pipeline_template"("p_template_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_pipeline_template"("p_template_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_pipeline_template"("p_template_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_plugin_by_id"("p_plugin_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_plugin_by_id"("p_plugin_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_plugin_by_id"("p_plugin_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_recent_activity"("p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_recent_activity"("p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_recent_activity"("p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."get_step_outputs"("p_execution_step_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_step_outputs"("p_execution_step_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_step_outputs"("p_execution_step_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_triggers_report"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_triggers_report"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_triggers_report"() TO "service_role";



GRANT ALL ON FUNCTION "public"."get_user_org_role"("p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_user_org_role"("p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_user_org_role"("p_user_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."global_search"("p_org_id" "uuid", "p_query" "text", "p_type" "text", "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."global_search"("p_org_id" "uuid", "p_query" "text", "p_type" "text", "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."global_search"("p_org_id" "uuid", "p_query" "text", "p_type" "text", "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "service_role";



GRANT ALL ON FUNCTION "public"."handle_dataset_scan_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."handle_dataset_scan_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."handle_dataset_scan_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."handle_job_failure"() TO "anon";
GRANT ALL ON FUNCTION "public"."handle_job_failure"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."handle_job_failure"() TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."increment_device_job"("dev_id" "uuid", "org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."increment_device_job"("dev_id" "uuid", "org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."increment_device_job"("dev_id" "uuid", "org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"() TO "anon";
GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"() TO "service_role";



GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"("p_device_id" "uuid", "p_org_id" "uuid", "p_source" "text", "p_metrics" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"("p_device_id" "uuid", "p_org_id" "uuid", "p_source" "text", "p_metrics" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."insert_device_agent_metric"("p_device_id" "uuid", "p_org_id" "uuid", "p_source" "text", "p_metrics" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."int2_dist"(smallint, smallint) TO "postgres";
GRANT ALL ON FUNCTION "public"."int2_dist"(smallint, smallint) TO "anon";
GRANT ALL ON FUNCTION "public"."int2_dist"(smallint, smallint) TO "authenticated";
GRANT ALL ON FUNCTION "public"."int2_dist"(smallint, smallint) TO "service_role";



GRANT ALL ON FUNCTION "public"."int4_dist"(integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."int4_dist"(integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."int4_dist"(integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."int4_dist"(integer, integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."int8_dist"(bigint, bigint) TO "postgres";
GRANT ALL ON FUNCTION "public"."int8_dist"(bigint, bigint) TO "anon";
GRANT ALL ON FUNCTION "public"."int8_dist"(bigint, bigint) TO "authenticated";
GRANT ALL ON FUNCTION "public"."int8_dist"(bigint, bigint) TO "service_role";



GRANT ALL ON FUNCTION "public"."interval_dist"(interval, interval) TO "postgres";
GRANT ALL ON FUNCTION "public"."interval_dist"(interval, interval) TO "anon";
GRANT ALL ON FUNCTION "public"."interval_dist"(interval, interval) TO "authenticated";
GRANT ALL ON FUNCTION "public"."interval_dist"(interval, interval) TO "service_role";



GRANT ALL ON FUNCTION "public"."invoke_auto_assign_best_device"("p_dataset_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."invoke_auto_assign_best_device"("p_dataset_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."invoke_auto_assign_best_device"("p_dataset_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."invoke_optimal_chunk_size_calculation"() TO "anon";
GRANT ALL ON FUNCTION "public"."invoke_optimal_chunk_size_calculation"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."invoke_optimal_chunk_size_calculation"() TO "service_role";



GRANT ALL ON FUNCTION "public"."is_org_admin"("_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."is_org_admin"("_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_org_admin"("_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."is_org_member"("_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."is_org_member"("_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_org_member"("_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."lease_agent_job"("p_job_id" "uuid", "p_org_id" "uuid", "p_agent_id" "uuid", "p_ttl_secs" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."lease_agent_job"("p_job_id" "uuid", "p_org_id" "uuid", "p_agent_id" "uuid", "p_ttl_secs" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."lease_agent_job"("p_job_id" "uuid", "p_org_id" "uuid", "p_agent_id" "uuid", "p_ttl_secs" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."list_pipeline_templates"("p_org_id" "uuid", "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."list_pipeline_templates"("p_org_id" "uuid", "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."list_pipeline_templates"("p_org_id" "uuid", "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."list_public_functions"() TO "anon";
GRANT ALL ON FUNCTION "public"."list_public_functions"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."list_public_functions"() TO "service_role";



GRANT ALL ON FUNCTION "public"."log_agent_error"("p_agent_id" "text", "p_message" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."log_agent_error"("p_agent_id" "text", "p_message" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."log_agent_error"("p_agent_id" "text", "p_message" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."log_agent_error"("_device_id" "uuid", "_job_id" "uuid", "_error_message" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."manage_agent_job_state"() TO "anon";
GRANT ALL ON FUNCTION "public"."manage_agent_job_state"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."manage_agent_job_state"() TO "service_role";



GRANT ALL ON FUNCTION "public"."mark_offline_devices"() TO "anon";
GRANT ALL ON FUNCTION "public"."mark_offline_devices"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."mark_offline_devices"() TO "service_role";



GRANT ALL ON FUNCTION "public"."match_best_device"("p_job_id" "text", "p_org_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."match_best_device"("p_job_id" "text", "p_org_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_best_device"("p_job_id" "text", "p_org_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."match_best_device"("_org_id" "uuid", "_chunk_vector" "public"."vector", "_job_type" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."match_best_device"("_org_id" "uuid", "_chunk_vector" "public"."vector", "_job_type" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_best_device"("_org_id" "uuid", "_chunk_vector" "public"."vector", "_job_type" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."match_best_execution_target"("p_org_id" "uuid", "p_job_vector" "public"."vector", "p_job_type" "text", "p_runtime_type" "text", "p_execution_mode" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."match_best_execution_target"("p_org_id" "uuid", "p_job_vector" "public"."vector", "p_job_type" "text", "p_runtime_type" "text", "p_execution_mode" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_best_execution_target"("p_org_id" "uuid", "p_job_vector" "public"."vector", "p_job_type" "text", "p_runtime_type" "text", "p_execution_mode" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."migrate_enterprise_credentials_to_vault"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."migrate_enterprise_credentials_to_vault"() TO "anon";
GRANT ALL ON FUNCTION "public"."migrate_enterprise_credentials_to_vault"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."migrate_enterprise_credentials_to_vault"() TO "service_role";



GRANT ALL ON FUNCTION "public"."move_job_to_dead_letter"("p_job_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."move_job_to_dead_letter"("p_job_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."move_job_to_dead_letter"("p_job_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."notify_agent_on_dataset_register"() TO "anon";
GRANT ALL ON FUNCTION "public"."notify_agent_on_dataset_register"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."notify_agent_on_dataset_register"() TO "service_role";



GRANT ALL ON FUNCTION "public"."notify_job_queue"() TO "anon";
GRANT ALL ON FUNCTION "public"."notify_job_queue"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."notify_job_queue"() TO "service_role";



GRANT ALL ON FUNCTION "public"."notify_merge_complete"("p_dataset_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."notify_merge_complete"("p_dataset_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."notify_merge_complete"("p_dataset_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."notify_new_job"("p_org_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."notify_new_job"("p_org_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."notify_new_job"("p_org_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."oid_dist"("oid", "oid") TO "postgres";
GRANT ALL ON FUNCTION "public"."oid_dist"("oid", "oid") TO "anon";
GRANT ALL ON FUNCTION "public"."oid_dist"("oid", "oid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."oid_dist"("oid", "oid") TO "service_role";



GRANT ALL ON FUNCTION "public"."on_agent_job_failed"() TO "anon";
GRANT ALL ON FUNCTION "public"."on_agent_job_failed"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."on_agent_job_failed"() TO "service_role";



GRANT ALL ON FUNCTION "public"."on_merge_job_finished"() TO "anon";
GRANT ALL ON FUNCTION "public"."on_merge_job_finished"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."on_merge_job_finished"() TO "service_role";



GRANT ALL ON FUNCTION "public"."populate_job_runtime_info"() TO "anon";
GRANT ALL ON FUNCTION "public"."populate_job_runtime_info"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."populate_job_runtime_info"() TO "service_role";



GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_chunk_size" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_chunk_size" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_chunk_size" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pre_chunk_dataset_smart"("p_dataset_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."prevent_overassign_agent_job"() TO "anon";
GRANT ALL ON FUNCTION "public"."prevent_overassign_agent_job"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."prevent_overassign_agent_job"() TO "service_role";



GRANT ALL ON FUNCTION "public"."process_http_queue"() TO "anon";
GRANT ALL ON FUNCTION "public"."process_http_queue"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."process_http_queue"() TO "service_role";



GRANT ALL ON FUNCTION "public"."process_http_queue"("p_limit" integer, "p_max_retries" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."process_http_queue"("p_limit" integer, "p_max_retries" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."process_http_queue"("p_limit" integer, "p_max_retries" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."prune_old_agent_metrics"() TO "anon";
GRANT ALL ON FUNCTION "public"."prune_old_agent_metrics"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."prune_old_agent_metrics"() TO "service_role";



GRANT ALL ON FUNCTION "public"."prune_old_system_logs"() TO "anon";
GRANT ALL ON FUNCTION "public"."prune_old_system_logs"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."prune_old_system_logs"() TO "service_role";



GRANT ALL ON FUNCTION "public"."queue_assign_scan_job"() TO "anon";
GRANT ALL ON FUNCTION "public"."queue_assign_scan_job"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."queue_assign_scan_job"() TO "service_role";



GRANT ALL ON FUNCTION "public"."queue_job_notification"() TO "anon";
GRANT ALL ON FUNCTION "public"."queue_job_notification"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."queue_job_notification"() TO "service_role";



GRANT ALL ON FUNCTION "public"."recalculate_device_vector"("p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."recalculate_device_vector"("p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."recalculate_device_vector"("p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_device_id" "uuid", "p_job_id" "uuid", "p_chunk_count" integer, "p_strategy" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_device_id" "uuid", "p_job_id" "uuid", "p_chunk_count" integer, "p_strategy" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_device_id" "uuid", "p_job_id" "uuid", "p_chunk_count" integer, "p_strategy" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_dataset_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_dataset_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."rechunk_for_device"("p_dataset_id" "uuid", "p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."reclaim_jobs_from_device"("p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."reclaim_jobs_from_device"("p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."reclaim_jobs_from_device"("p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."reconcile_device_stale_jobs"("p_device_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."reconcile_device_stale_jobs"("p_device_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."reconcile_device_stale_jobs"("p_device_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision, "_throughput" double precision, "_output_token" "text", "_plugin_id" "text", "_plugin_version" "text", "_metrics" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision, "_throughput" double precision, "_output_token" "text", "_plugin_id" "text", "_plugin_version" "text", "_metrics" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_agent_job_result"("_job_id" "uuid", "_status" "text", "_duration_ms" double precision, "_throughput" double precision, "_output_token" "text", "_plugin_id" "text", "_plugin_version" "text", "_metrics" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."record_job_performance"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."record_job_performance"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_job_performance"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "text", "p_status" "text", "p_error" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "text", "p_status" "text", "p_error" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "text", "p_status" "text", "p_error" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "uuid", "p_status" "text", "p_error" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "uuid", "p_status" "text", "p_error" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_end"("p_execution_id" "uuid", "p_status" "text", "p_error" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "text", "p_plugin_id" "text", "p_job_id" "text", "p_device_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "text", "p_plugin_id" "text", "p_job_id" "text", "p_device_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "text", "p_plugin_id" "text", "p_job_id" "text", "p_device_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "uuid", "p_plugin_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "uuid", "p_plugin_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."record_plugin_execution_start"("p_org_id" "uuid", "p_plugin_id" "uuid", "p_job_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer, "_max_retries" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer, "_max_retries" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."recover_stuck_jobs"("_timeout_minutes" integer, "_max_retries" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."refresh_dataset_merge_lock"("p_lock_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."refresh_dataset_merge_lock"("p_lock_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."refresh_dataset_merge_lock"("p_lock_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."release_dataset_merge_lock"("p_lock_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."release_dataset_merge_lock"("p_lock_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."release_dataset_merge_lock"("p_lock_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."release_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."release_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."release_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."rotate_agent_token"("p_device_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."rotate_agent_token"("p_device_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."rotate_agent_token"("p_device_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."rotate_device_token"("p_device_id" "uuid", "p_token_hash" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."rotate_device_token"("p_device_id" "uuid", "p_token_hash" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."rotate_device_token"("p_device_id" "uuid", "p_token_hash" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."run_all_validation_tests"() TO "anon";
GRANT ALL ON FUNCTION "public"."run_all_validation_tests"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."run_all_validation_tests"() TO "service_role";



GRANT ALL ON FUNCTION "public"."safe_cast_uuid"("input" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."safe_cast_uuid"("input" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."safe_cast_uuid"("input" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."safe_http_post"("target_url" "text", "payload" "jsonb", "headers" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."safe_http_post"("target_url" "text", "payload" "jsonb", "headers" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."safe_http_post"("target_url" "text", "payload" "jsonb", "headers" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "jsonb", "p_prefer_online" boolean, "p_strict_availability" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "jsonb", "p_prefer_online" boolean, "p_strict_availability" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."select_best_device"("p_org_id" "uuid", "p_job_type" "text", "p_chunk_vector" "jsonb", "p_prefer_online" boolean, "p_strict_availability" boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sentra_rls_table_status"() TO "anon";
GRANT ALL ON FUNCTION "public"."sentra_rls_table_status"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."sentra_rls_table_status"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_alert_rules_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_alert_rules_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_alert_rules_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_default_storage_config"("p_org_id" "uuid", "p_config_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."set_default_storage_config"("p_org_id" "uuid", "p_config_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_default_storage_config"("p_org_id" "uuid", "p_config_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."set_device_vector"("p_device_id" "uuid", "p_vec_literal" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."set_device_vector"("p_device_id" "uuid", "p_vec_literal" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_device_vector"("p_device_id" "uuid", "p_vec_literal" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."set_enterprise_integrations_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_enterprise_integrations_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_enterprise_integrations_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_execution_policies_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_execution_policies_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_execution_policies_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_http_queue_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_http_queue_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_http_queue_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_job_notification_queue_org_id"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_job_notification_queue_org_id"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_job_notification_queue_org_id"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_org_id_from_record"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_org_id_from_record"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_org_id_from_record"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_plugin_execution_history_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_plugin_execution_history_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_plugin_execution_history_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_plugin_signing_keys_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_runtime_environments_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_vector_datasets_org_id_trigger"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_vector_datasets_org_id_trigger"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_vector_datasets_org_id_trigger"() TO "service_role";



GRANT ALL ON FUNCTION "public"."should_run_plugin"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."should_run_plugin"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."should_run_plugin"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."should_run_plugin_for_device"("p_device_id" "uuid", "p_rollout_percentage" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."start_job"("p_job_id" "uuid", "p_agent_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."start_job"("p_job_id" "uuid", "p_agent_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."start_job"("p_job_id" "uuid", "p_agent_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."store_s3_credentials_to_vault"("p_org_id" "uuid", "p_access_key_id" "text", "p_secret_access_key" "text", "p_provider" "text", "p_secret_name" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."store_s3_credentials_to_vault"("p_org_id" "uuid", "p_access_key_id" "text", "p_secret_access_key" "text", "p_provider" "text", "p_secret_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."store_s3_credentials_to_vault"("p_org_id" "uuid", "p_access_key_id" "text", "p_secret_access_key" "text", "p_provider" "text", "p_secret_name" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."system_health_heartbeat"() TO "anon";
GRANT ALL ON FUNCTION "public"."system_health_heartbeat"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."system_health_heartbeat"() TO "service_role";



GRANT ALL ON FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer, "_recovery_timeout" integer, "_max_retries" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer, "_recovery_timeout" integer, "_max_retries" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."system_health_heartbeat"("_stale_device_minutes" integer, "_recovery_timeout" integer, "_max_retries" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."test_case_1_device_available"() TO "anon";
GRANT ALL ON FUNCTION "public"."test_case_1_device_available"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."test_case_1_device_available"() TO "service_role";



GRANT ALL ON FUNCTION "public"."test_case_2_no_device"() TO "anon";
GRANT ALL ON FUNCTION "public"."test_case_2_no_device"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."test_case_2_no_device"() TO "service_role";



GRANT ALL ON FUNCTION "public"."test_case_3_multiple_chunks"() TO "anon";
GRANT ALL ON FUNCTION "public"."test_case_3_multiple_chunks"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."test_case_3_multiple_chunks"() TO "service_role";



GRANT ALL ON FUNCTION "public"."test_case_4_state_machine"() TO "anon";
GRANT ALL ON FUNCTION "public"."test_case_4_state_machine"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."test_case_4_state_machine"() TO "service_role";



GRANT ALL ON FUNCTION "public"."test_case_5_no_duplicates"() TO "anon";
GRANT ALL ON FUNCTION "public"."test_case_5_no_duplicates"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."test_case_5_no_duplicates"() TO "service_role";



GRANT ALL ON FUNCTION "public"."time_dist"(time without time zone, time without time zone) TO "postgres";
GRANT ALL ON FUNCTION "public"."time_dist"(time without time zone, time without time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."time_dist"(time without time zone, time without time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."time_dist"(time without time zone, time without time zone) TO "service_role";



GRANT ALL ON FUNCTION "public"."touch_device_vector"() TO "anon";
GRANT ALL ON FUNCTION "public"."touch_device_vector"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."touch_device_vector"() TO "service_role";



GRANT ALL ON FUNCTION "public"."trg_cleanup_leases_on_offline"() TO "anon";
GRANT ALL ON FUNCTION "public"."trg_cleanup_leases_on_offline"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."trg_cleanup_leases_on_offline"() TO "service_role";



GRANT ALL ON FUNCTION "public"."ts_dist"(timestamp without time zone, timestamp without time zone) TO "postgres";
GRANT ALL ON FUNCTION "public"."ts_dist"(timestamp without time zone, timestamp without time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."ts_dist"(timestamp without time zone, timestamp without time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."ts_dist"(timestamp without time zone, timestamp without time zone) TO "service_role";



GRANT ALL ON FUNCTION "public"."tstz_dist"(timestamp with time zone, timestamp with time zone) TO "postgres";
GRANT ALL ON FUNCTION "public"."tstz_dist"(timestamp with time zone, timestamp with time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."tstz_dist"(timestamp with time zone, timestamp with time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."tstz_dist"(timestamp with time zone, timestamp with time zone) TO "service_role";



GRANT ALL ON FUNCTION "public"."update_dataset_merge_metadata"("p_dataset_id" "uuid", "p_merge_time_ms" double precision, "p_merged_size_gb" double precision, "p_verified" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."update_dataset_merge_metadata"("p_dataset_id" "uuid", "p_merge_time_ms" double precision, "p_merged_size_gb" double precision, "p_verified" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_dataset_merge_metadata"("p_dataset_id" "uuid", "p_merge_time_ms" double precision, "p_merged_size_gb" double precision, "p_verified" boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."update_dataset_scan"("p_dataset_id" "uuid", "p_scan_metadata" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."update_dataset_scan"("p_dataset_id" "uuid", "p_scan_metadata" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_dataset_scan"("p_dataset_id" "uuid", "p_scan_metadata" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_dataset_status_on_merge_complete"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_dataset_status_on_merge_complete"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_dataset_status_on_merge_complete"() TO "service_role";



GRANT ALL ON FUNCTION "public"."update_dataset_status_on_scan_complete"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_dataset_status_on_scan_complete"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_dataset_status_on_scan_complete"() TO "service_role";



GRANT ALL ON FUNCTION "public"."update_dataset_storage_type"("p_dataset_id" "uuid", "p_storage_type" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."update_dataset_storage_type"("p_dataset_id" "uuid", "p_storage_type" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_dataset_storage_type"("p_dataset_id" "uuid", "p_storage_type" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_status" "text", "p_duration_ms" bigint) TO "anon";
GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_status" "text", "p_duration_ms" bigint) TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_status" "text", "p_duration_ms" bigint) TO "service_role";



GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_device_job_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."update_device_job_type_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."update_device_job_type_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_device_job_type_stats"("p_device_id" "uuid", "p_org_id" "uuid", "p_job_type" "text", "p_duration_ms" numeric, "p_throughput" numeric, "p_success" boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."update_device_vector"("device_id" "uuid", "new_vec" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."update_device_vector"("device_id" "uuid", "new_vec" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_device_vector"("device_id" "uuid", "new_vec" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid", "p_org_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid", "p_org_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_merge_lock_heartbeat"("p_lock_id" "uuid", "p_org_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."update_org_storage_configs_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_org_storage_configs_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_org_storage_configs_updated_at"() TO "service_role";



GRANT ALL ON FUNCTION "public"."update_pipeline_timestamp"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_pipeline_timestamp"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_pipeline_timestamp"() TO "service_role";



GRANT ALL ON FUNCTION "public"."update_vector_dataset_count"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_vector_dataset_count"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_vector_dataset_count"() TO "service_role";



GRANT ALL ON FUNCTION "public"."validate_claim_secret"("p_claim" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."validate_claim_secret"("p_claim" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."validate_claim_secret"("p_claim" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."validate_plugin_config"("p_config" "jsonb", "p_config_schema" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."validate_plugin_config"("p_config" "jsonb", "p_config_schema" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."validate_plugin_config"("p_config" "jsonb", "p_config_schema" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "text", "p_device_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "text", "p_device_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "text", "p_device_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."verify_job_lease"("p_job_id" "uuid", "p_device_id" "uuid") TO "service_role";












GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "service_role";



SET SESSION AUTHORIZATION "postgres";
RESET SESSION AUTHORIZATION;



SET SESSION AUTHORIZATION "postgres";
RESET SESSION AUTHORIZATION;









GRANT ALL ON TABLE "public"."agent_jobs" TO "anon";
GRANT ALL ON TABLE "public"."agent_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."agent_jobs_archive" TO "anon";
GRANT ALL ON TABLE "public"."agent_jobs_archive" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_jobs_archive" TO "service_role";



GRANT ALL ON TABLE "public"."agent_jobs_dead_letter" TO "anon";
GRANT ALL ON TABLE "public"."agent_jobs_dead_letter" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_jobs_dead_letter" TO "service_role";



GRANT ALL ON TABLE "public"."agent_metrics" TO "anon";
GRANT ALL ON TABLE "public"."agent_metrics" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_metrics" TO "service_role";



GRANT ALL ON TABLE "public"."agent_worker_activity" TO "anon";
GRANT ALL ON TABLE "public"."agent_worker_activity" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_worker_activity" TO "service_role";



GRANT ALL ON TABLE "public"."alert_history" TO "anon";
GRANT ALL ON TABLE "public"."alert_history" TO "authenticated";
GRANT ALL ON TABLE "public"."alert_history" TO "service_role";



GRANT ALL ON TABLE "public"."alert_rules" TO "anon";
GRANT ALL ON TABLE "public"."alert_rules" TO "authenticated";
GRANT ALL ON TABLE "public"."alert_rules" TO "service_role";



GRANT ALL ON TABLE "public"."batch_chunks" TO "anon";
GRANT ALL ON TABLE "public"."batch_chunks" TO "authenticated";
GRANT ALL ON TABLE "public"."batch_chunks" TO "service_role";



GRANT ALL ON TABLE "public"."bootstrap_rate_limits" TO "anon";
GRANT ALL ON TABLE "public"."bootstrap_rate_limits" TO "authenticated";
GRANT ALL ON TABLE "public"."bootstrap_rate_limits" TO "service_role";



GRANT ALL ON TABLE "public"."chunk_complexity_cache" TO "anon";
GRANT ALL ON TABLE "public"."chunk_complexity_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."chunk_complexity_cache" TO "service_role";



GRANT ALL ON TABLE "public"."chunk_profiles" TO "anon";
GRANT ALL ON TABLE "public"."chunk_profiles" TO "authenticated";
GRANT ALL ON TABLE "public"."chunk_profiles" TO "service_role";



GRANT ALL ON TABLE "public"."dataset_merge_locks" TO "anon";
GRANT ALL ON TABLE "public"."dataset_merge_locks" TO "service_role";
GRANT ALL ON TABLE "public"."dataset_merge_locks" TO "authenticated";



GRANT ALL ON TABLE "public"."datasets" TO "anon";
GRANT ALL ON TABLE "public"."datasets" TO "authenticated";
GRANT ALL ON TABLE "public"."datasets" TO "service_role";



GRANT ALL ON TABLE "public"."device_benchmarks" TO "anon";
GRANT ALL ON TABLE "public"."device_benchmarks" TO "authenticated";
GRANT ALL ON TABLE "public"."device_benchmarks" TO "service_role";



GRANT ALL ON TABLE "public"."device_claims" TO "anon";
GRANT ALL ON TABLE "public"."device_claims" TO "authenticated";
GRANT ALL ON TABLE "public"."device_claims" TO "service_role";



GRANT ALL ON TABLE "public"."device_events" TO "anon";
GRANT ALL ON TABLE "public"."device_events" TO "authenticated";
GRANT ALL ON TABLE "public"."device_events" TO "service_role";



GRANT ALL ON TABLE "public"."device_job_performance" TO "anon";
GRANT ALL ON TABLE "public"."device_job_performance" TO "authenticated";
GRANT ALL ON TABLE "public"."device_job_performance" TO "service_role";



GRANT ALL ON TABLE "public"."device_job_type_stats" TO "anon";
GRANT ALL ON TABLE "public"."device_job_type_stats" TO "authenticated";
GRANT ALL ON TABLE "public"."device_job_type_stats" TO "service_role";



GRANT ALL ON TABLE "public"."device_learning_history" TO "anon";
GRANT ALL ON TABLE "public"."device_learning_history" TO "authenticated";
GRANT ALL ON TABLE "public"."device_learning_history" TO "service_role";



GRANT ALL ON TABLE "public"."device_vectors" TO "anon";
GRANT ALL ON TABLE "public"."device_vectors" TO "authenticated";
GRANT ALL ON TABLE "public"."device_vectors" TO "service_role";



GRANT ALL ON TABLE "public"."devices" TO "anon";
GRANT ALL ON TABLE "public"."devices" TO "authenticated";
GRANT ALL ON TABLE "public"."devices" TO "service_role";



GRANT ALL ON TABLE "public"."device_learning_progress_view" TO "anon";
GRANT ALL ON TABLE "public"."device_learning_progress_view" TO "authenticated";
GRANT ALL ON TABLE "public"."device_learning_progress_view" TO "service_role";



GRANT ALL ON TABLE "public"."device_policies" TO "anon";
GRANT ALL ON TABLE "public"."device_policies" TO "authenticated";
GRANT ALL ON TABLE "public"."device_policies" TO "service_role";



GRANT ALL ON TABLE "public"."device_ranking_view" TO "anon";
GRANT ALL ON TABLE "public"."device_ranking_view" TO "authenticated";
GRANT ALL ON TABLE "public"."device_ranking_view" TO "service_role";



GRANT ALL ON TABLE "public"."device_routing_rules" TO "anon";
GRANT ALL ON TABLE "public"."device_routing_rules" TO "authenticated";
GRANT ALL ON TABLE "public"."device_routing_rules" TO "service_role";



GRANT ALL ON TABLE "public"."device_summary_view" TO "anon";
GRANT ALL ON TABLE "public"."device_summary_view" TO "authenticated";
GRANT ALL ON TABLE "public"."device_summary_view" TO "service_role";



GRANT ALL ON TABLE "public"."dismissed_alerts" TO "anon";
GRANT ALL ON TABLE "public"."dismissed_alerts" TO "authenticated";
GRANT ALL ON TABLE "public"."dismissed_alerts" TO "service_role";



GRANT ALL ON TABLE "public"."enterprise_integrations" TO "anon";
GRANT ALL ON TABLE "public"."enterprise_integrations" TO "authenticated";
GRANT ALL ON TABLE "public"."enterprise_integrations" TO "service_role";



GRANT ALL ON TABLE "public"."environment_cache" TO "anon";
GRANT ALL ON TABLE "public"."environment_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."environment_cache" TO "service_role";



GRANT ALL ON TABLE "public"."execution_policies" TO "anon";
GRANT ALL ON TABLE "public"."execution_policies" TO "authenticated";
GRANT ALL ON TABLE "public"."execution_policies" TO "service_role";



GRANT ALL ON TABLE "public"."execution_steps" TO "anon";
GRANT ALL ON TABLE "public"."execution_steps" TO "authenticated";
GRANT ALL ON TABLE "public"."execution_steps" TO "service_role";



GRANT ALL ON TABLE "public"."executions" TO "anon";
GRANT ALL ON TABLE "public"."executions" TO "authenticated";
GRANT ALL ON TABLE "public"."executions" TO "service_role";



GRANT ALL ON TABLE "public"."http_queue" TO "anon";
GRANT ALL ON TABLE "public"."http_queue" TO "authenticated";
GRANT ALL ON TABLE "public"."http_queue" TO "service_role";



GRANT ALL ON TABLE "public"."job_checkpoints" TO "anon";
GRANT ALL ON TABLE "public"."job_checkpoints" TO "authenticated";
GRANT ALL ON TABLE "public"."job_checkpoints" TO "service_role";



GRANT ALL ON TABLE "public"."job_notification_queue" TO "anon";
GRANT ALL ON TABLE "public"."job_notification_queue" TO "authenticated";
GRANT ALL ON TABLE "public"."job_notification_queue" TO "service_role";



GRANT ALL ON SEQUENCE "public"."job_notification_queue_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."job_notification_queue_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."job_notification_queue_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."leases" TO "anon";
GRANT ALL ON TABLE "public"."leases" TO "authenticated";
GRANT ALL ON TABLE "public"."leases" TO "service_role";



GRANT ALL ON TABLE "public"."org_members" TO "anon";
GRANT ALL ON TABLE "public"."org_members" TO "authenticated";
GRANT ALL ON TABLE "public"."org_members" TO "service_role";



GRANT ALL ON TABLE "public"."org_plugins" TO "anon";
GRANT ALL ON TABLE "public"."org_plugins" TO "authenticated";
GRANT ALL ON TABLE "public"."org_plugins" TO "service_role";



GRANT ALL ON TABLE "public"."org_quotas" TO "anon";
GRANT ALL ON TABLE "public"."org_quotas" TO "authenticated";
GRANT ALL ON TABLE "public"."org_quotas" TO "service_role";



GRANT ALL ON TABLE "public"."org_storage_configs" TO "anon";
GRANT ALL ON TABLE "public"."org_storage_configs" TO "authenticated";
GRANT ALL ON TABLE "public"."org_storage_configs" TO "service_role";



GRANT ALL ON TABLE "public"."org_usage" TO "anon";
GRANT ALL ON TABLE "public"."org_usage" TO "authenticated";
GRANT ALL ON TABLE "public"."org_usage" TO "service_role";



GRANT ALL ON TABLE "public"."orgs" TO "anon";
GRANT ALL ON TABLE "public"."orgs" TO "authenticated";
GRANT ALL ON TABLE "public"."orgs" TO "service_role";



GRANT ALL ON TABLE "public"."pipeline_templates" TO "anon";
GRANT ALL ON TABLE "public"."pipeline_templates" TO "authenticated";
GRANT ALL ON TABLE "public"."pipeline_templates" TO "service_role";



GRANT ALL ON TABLE "public"."plan_limits" TO "anon";
GRANT ALL ON TABLE "public"."plan_limits" TO "authenticated";
GRANT ALL ON TABLE "public"."plan_limits" TO "service_role";



GRANT ALL ON TABLE "public"."plugin_execution_history" TO "anon";
GRANT ALL ON TABLE "public"."plugin_execution_history" TO "authenticated";
GRANT ALL ON TABLE "public"."plugin_execution_history" TO "service_role";



GRANT ALL ON TABLE "public"."plugin_signing_keys" TO "anon";
GRANT ALL ON TABLE "public"."plugin_signing_keys" TO "authenticated";
GRANT ALL ON TABLE "public"."plugin_signing_keys" TO "service_role";



GRANT ALL ON TABLE "public"."plugins" TO "anon";
GRANT ALL ON TABLE "public"."plugins" TO "authenticated";
GRANT ALL ON TABLE "public"."plugins" TO "service_role";



GRANT ALL ON TABLE "public"."runtime_environments" TO "anon";
GRANT ALL ON TABLE "public"."runtime_environments" TO "authenticated";
GRANT ALL ON TABLE "public"."runtime_environments" TO "service_role";



GRANT ALL ON TABLE "public"."step_outputs" TO "anon";
GRANT ALL ON TABLE "public"."step_outputs" TO "authenticated";
GRANT ALL ON TABLE "public"."step_outputs" TO "service_role";



GRANT ALL ON TABLE "public"."system_config" TO "anon";
GRANT ALL ON TABLE "public"."system_config" TO "authenticated";
GRANT ALL ON TABLE "public"."system_config" TO "service_role";



GRANT ALL ON TABLE "public"."system_logs" TO "anon";
GRANT ALL ON TABLE "public"."system_logs" TO "authenticated";
GRANT ALL ON TABLE "public"."system_logs" TO "service_role";



GRANT ALL ON TABLE "public"."user_orgs" TO "anon";
GRANT ALL ON TABLE "public"."user_orgs" TO "authenticated";
GRANT ALL ON TABLE "public"."user_orgs" TO "service_role";



GRANT ALL ON TABLE "public"."v_active_agent_jobs" TO "anon";
GRANT ALL ON TABLE "public"."v_active_agent_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."v_active_agent_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."v_active_jobs" TO "anon";
GRANT ALL ON TABLE "public"."v_active_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."v_active_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."v_device_claim_eligible" TO "anon";
GRANT ALL ON TABLE "public"."v_device_claim_eligible" TO "authenticated";
GRANT ALL ON TABLE "public"."v_device_claim_eligible" TO "service_role";



GRANT ALL ON TABLE "public"."v_device_health" TO "anon";
GRANT ALL ON TABLE "public"."v_device_health" TO "authenticated";
GRANT ALL ON TABLE "public"."v_device_health" TO "service_role";



GRANT ALL ON TABLE "public"."v_device_runtime_capabilities" TO "anon";
GRANT ALL ON TABLE "public"."v_device_runtime_capabilities" TO "authenticated";
GRANT ALL ON TABLE "public"."v_device_runtime_capabilities" TO "service_role";



GRANT ALL ON TABLE "public"."v_job_lifecycle" TO "anon";
GRANT ALL ON TABLE "public"."v_job_lifecycle" TO "authenticated";
GRANT ALL ON TABLE "public"."v_job_lifecycle" TO "service_role";



GRANT ALL ON TABLE "public"."v_job_timeline" TO "anon";
GRANT ALL ON TABLE "public"."v_job_timeline" TO "authenticated";
GRANT ALL ON TABLE "public"."v_job_timeline" TO "service_role";



GRANT ALL ON TABLE "public"."v_pending_runtime_jobs" TO "anon";
GRANT ALL ON TABLE "public"."v_pending_runtime_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."v_pending_runtime_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."v_recent_failures" TO "anon";
GRANT ALL ON TABLE "public"."v_recent_failures" TO "authenticated";
GRANT ALL ON TABLE "public"."v_recent_failures" TO "service_role";



GRANT ALL ON TABLE "public"."v_stuck_job_summary" TO "anon";
GRANT ALL ON TABLE "public"."v_stuck_job_summary" TO "authenticated";
GRANT ALL ON TABLE "public"."v_stuck_job_summary" TO "service_role";



GRANT ALL ON TABLE "public"."v_stuck_jobs" TO "anon";
GRANT ALL ON TABLE "public"."v_stuck_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."v_stuck_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."v_system_health" TO "anon";
GRANT ALL ON TABLE "public"."v_system_health" TO "authenticated";
GRANT ALL ON TABLE "public"."v_system_health" TO "service_role";



GRANT ALL ON TABLE "public"."vector_batches" TO "anon";
GRANT ALL ON TABLE "public"."vector_batches" TO "authenticated";
GRANT ALL ON TABLE "public"."vector_batches" TO "service_role";



GRANT ALL ON TABLE "public"."vector_datasets" TO "anon";
GRANT ALL ON TABLE "public"."vector_datasets" TO "authenticated";
GRANT ALL ON TABLE "public"."vector_datasets" TO "service_role";



GRANT ALL ON TABLE "public"."vector_store" TO "anon";
GRANT ALL ON TABLE "public"."vector_store" TO "authenticated";
GRANT ALL ON TABLE "public"."vector_store" TO "service_role";









ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";































