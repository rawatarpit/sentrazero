-- Migration: Add execution_id to claim_jobs_for_device return
-- Description: The claim_jobs_for_device function sets execution_id in the database but was not returning it to the agent.
-- This caused jobs to fail at completion because the agent had no execution_id to report back with.
-- Created: 2026-04-24
-- Updated: 2026-04-24 - Fixed ambiguous column reference by using output aliases

-- Drop the existing function
DROP FUNCTION IF EXISTS public.claim_jobs_for_device(uuid, uuid, integer, integer);

-- Create the updated function with execution_id in the return table
-- Using explicit table aliases to avoid any ambiguity between column names and variable names
CREATE OR REPLACE FUNCTION public.claim_jobs_for_device(p_device_id uuid, p_org_id uuid, p_limit integer DEFAULT 10, p_lease_ttl_seconds integer DEFAULT 1800)
 RETURNS TABLE(job_id uuid, job_type text, job_payload jsonb, exec_id uuid)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    v_interval interval;
    v_execution_id_val uuid;
    v_job_id_val uuid;
    v_rtype_val text;
    v_rpayload_val jsonb;
    v_row record;
BEGIN
    v_interval := (p_lease_ttl_seconds || ' seconds')::interval;
    
    FOR v_row IN
        SELECT aj.id AS j_id, aj.job_type AS j_type, aj.payload AS j_payload, aj.execution_id AS j_exec_id
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
        v_execution_id_val := v_row.j_exec_id;
        
        IF v_execution_id_val IS NULL THEN
            INSERT INTO public.executions (org_id, dataset_id, status, current_step_index)
            VALUES (p_org_id, NULL, 'running', 0)
            RETURNING id INTO v_execution_id_val;
        END IF;

        UPDATE public.agent_jobs
        SET status = 'assigned',
            agent_id = p_device_id,
            assigned_at = NOW(),
            lease_expires_at = NOW() + v_interval,
            updated_at = NOW(),
            last_transition_at = NOW(),
            execution_id = COALESCE(v_execution_id_val, execution_id)
        WHERE agent_jobs.id = v_job_id_val AND agent_jobs.status = 'pending';

        IF FOUND THEN
            INSERT INTO public.job_notification_queue (job_id, agent_id, org_id)
            VALUES (v_job_id_val, p_device_id, p_org_id)
            ON CONFLICT DO NOTHING;
            
            job_id := v_job_id_val;
            job_type := v_rtype_val;
            job_payload := v_rpayload_val;
            exec_id := v_execution_id_val;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$function$;