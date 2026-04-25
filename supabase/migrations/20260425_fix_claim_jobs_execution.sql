-- Fix claim_jobs_for_device to properly create executions with dataset_id
-- The original function tried to INSERT with dataset_id=NULL but dataset_id is NOT NULL

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
BEGIN
    v_interval := (p_lease_ttl_seconds || ' seconds')::interval;
    
    FOR v_row IN
        SELECT aj.id AS j_id, aj.job_type AS j_type, aj.payload AS j_payload, aj.execution_id AS j_exec_id, aj.job_dataset_id
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
$function$
;