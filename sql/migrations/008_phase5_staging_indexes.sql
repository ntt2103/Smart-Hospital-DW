-- Administrative prerequisite, not a performance tuning experiment.
-- These are the same staging bed/time indexes previously created by batch SQL.
-- Admin-owned staging requires index DDL to run in setup, before ETL execution.
CREATE INDEX IF NOT EXISTS stage_status_bed_time
    ON stg.bed_status_history(bed_id,effective_from,effective_to);
CREATE INDEX IF NOT EXISTS stage_assignment_bed_time
    ON stg.bed_assignments(bed_id,assigned_at,released_at);
