-- Migration: Fix RLS policies for device-based agent authentication
-- This fixes the 401 UNAUTHORIZED_LEGACY_JWT error when devices call edge functions

-- Fix agent_jobs table - service role full access
DROP POLICY IF EXISTS "agent_jobs_update" ON public.agent_jobs;
DROP POLICY IF EXISTS "agent_jobs_insert_policy" ON public.agent_jobs;

CREATE POLICY "agent_jobs_all_service" ON public.agent_jobs
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix leases table - service role full access  
DROP POLICY IF EXISTS "leases_service_role_only" ON public.leases;

CREATE POLICY "leases_all_service" ON public.leases
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix executions table - service role full access
DROP POLICY IF EXISTS "executions_update_service_role" ON public.executions;
DROP POLICY IF EXISTS "executions_insert_service_role" ON public.executions;

CREATE POLICY "executions_all_service" ON public.executions
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix device_job_type_stats table
DROP POLICY IF EXISTS "device_job_type_stats_insert" ON public.device_job_type_stats;

CREATE POLICY "device_job_type_stats_all_service" ON public.device_job_type_stats
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix datasets table for status updates
DROP POLICY IF EXISTS "datasets_update_for_org" ON public.datasets;

CREATE POLICY "datasets_update_service" ON public.datasets
FOR UPDATE TO service_role USING (true) WITH CHECK (true);

-- Fix device tables for service role
CREATE POLICY "devices_all_service" ON public.devices
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix agent_worker_activity
CREATE POLICY "agent_worker_activity_all_service" ON public.agent_worker_activity
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Fix agent_metrics
DROP POLICY IF EXISTS "agent_metrics_insert_service_role" ON public.agent_metrics;
CREATE POLICY "agent_metrics_all_service" ON public.agent_metrics
FOR ALL TO service_role USING (true) WITH CHECK (true);