-- Migration: Add execution_id to claim_jobs_for_device return
-- Description: The claim_jobs_for_device function sets execution_id in the database but was not returning it to the agent.
-- This caused jobs to fail at completion because the agent had no execution_id to report back with.
-- Created: 2026-04-24

-- Drop the existing function
DROP FUNCTION IF EXISTS public.claim_jobs_for_device(uuid, uuid, integer, integer);

-- Create the updated function with execution_id in the return table
CREATE OR REPLACE FUNCTION public.claim_jobs_for_device(p_device_id uuid, p_org_id uuid, p_limit integer DEFAULT 10, p_lease_ttl_seconds integer DEFAULT 1800)
 RETURNS TABLE(id uuid, job_type text, payload jsonb, execution_id uuid)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    v_interval interval;
    v_execution_id uuid;
    v_rid uuid;
    v_rtype text;
    v_rpayload jsonb;
    v_row record;
BEGIN
    v_interval := (p_lease_ttl_seconds || ' seconds')::interval;
    
    FOR v_row IN
        SELECT aj.id, aj.job_type, aj.payload, aj.execution_id
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
        -- Store in variables first
        v_rid := v_row.id;
        v_rtype := v_row.job_type;
        v_rpayload := v_row.payload;
        v_execution_id := v_row.execution_id;
        
        -- If job doesn't have execution_id, create one
        IF v_execution_id IS NULL THEN
            INSERT INTO public.executions (org_id, dataset_id, status, current_step_index)
            VALUES (p_org_id, NULL, 'running', 0)
            RETURNING id INTO v_execution_id;
        END IF;

        UPDATE public.agent_jobs
        SET status = 'assigned',
            agent_id = p_device_id,
            assigned_at = NOW(),
            lease_expires_at = NOW() + v_interval,
            updated_at = NOW(),
            last_transition_at = NOW(),
            execution_id = COALESCE(v_execution_id, execution_id)
        WHERE agent_jobs.id = v_rid AND agent_jobs.status = 'pending';

        IF FOUND THEN
            INSERT INTO public.job_notification_queue (job_id, agent_id, org_id)
            VALUES (v_rid, p_device_id, p_org_id)
            ON CONFLICT DO NOTHING;
            
            -- Return all fields including execution_id
            id := v_rid;
            job_type := v_rtype;
            payload := v_rpayload;
            execution_id := v_execution_id;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$function$;