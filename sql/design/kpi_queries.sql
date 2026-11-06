-- Phase 1 KPI queries. Apply after warehouse_schema_draft.sql; no sample numbers.
-- Daily output: date × owning department; date bounds are inclusive local dates.
-- To aggregate a period use mart components, never AVG(daily rate/average).
BEGIN;
CREATE OR REPLACE FUNCTION mart.op01_visits(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.visit_count::NUMERIC
FROM mart.outpatient_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.op02_waiting_minutes(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.waiting_minutes_sum / nullif(m.waiting_valid_count, 0)
FROM mart.outpatient_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.op03_noshow_rate(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), 100.0 * m.no_show_count / nullif(m.classified_count, 0)
FROM mart.appointment_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.ip01_admissions(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.admission_count::NUMERIC
FROM mart.inpatient_admission_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.ip02_occupancy_rate(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0),
       CASE WHEN m.invalid_snapshot_count = 0 THEN 100.0 * m.occupied_beds / nullif(m.active_capacity, 0) END
FROM mart.bed_occupancy_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.ip03_los_days(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.los_days_sum / nullif(m.los_valid_count, 0)
FROM mart.inpatient_discharge_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.lab01_completed(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.completed_orders::NUMERIC
FROM mart.lab_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.lab02_tat_minutes(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), m.tat_minutes_sum / nullif(m.tat_valid_count, 0)
FROM mart.lab_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;
CREATE OR REPLACE FUNCTION mart.lab03_ontime_rate(p_start_date DATE, p_end_date DATE, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(report_date DATE, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT m.report_date, coalesce(d.department_id, 0), 100.0 * m.ontime_orders / nullif(m.ontime_valid_count, 0)
FROM mart.lab_daily m JOIN dw.dim_department d USING (department_sk)
WHERE m.report_date BETWEEN p_start_date AND p_end_date
  AND (p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id);
$$;

-- Canonical events are unique by event_id, count DISTINCT business entity as well.
-- Include inactive departments for late events and historical comparability.
-- All dim departments receive a zero on an idle interval (including SK 0).
CREATE OR REPLACE FUNCTION mart.rt01_checkins_last_15m(p_as_of TIMESTAMPTZ, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(as_of TIMESTAMPTZ, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT p_as_of, coalesce(d.department_id, 0), count(DISTINCT e.entity_id)::NUMERIC
FROM dw.dim_department d
LEFT JOIN dw.fact_operational_event e ON e.department_sk = d.department_sk
 AND e.topic = 'encounters.events' AND e.event_type = 'checked_in' AND e.is_rt_eligible
 AND e.event_time > p_as_of - INTERVAL '15 minutes' AND e.event_time <= p_as_of
 AND e.ingested_at <= p_as_of
WHERE p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id
GROUP BY d.department_sk, d.department_id;
$$;
CREATE OR REPLACE FUNCTION mart.rt02_lab_results_last_15m(p_as_of TIMESTAMPTZ, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(as_of TIMESTAMPTZ, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT p_as_of, coalesce(d.department_id, 0), count(DISTINCT e.entity_id)::NUMERIC
FROM dw.dim_department d
LEFT JOIN dw.fact_operational_event e ON e.department_sk = d.department_sk
 AND e.topic = 'laboratory.events' AND e.event_type = 'result_completed' AND e.is_rt_eligible
 AND e.event_time > p_as_of - INTERVAL '15 minutes' AND e.event_time <= p_as_of
 AND e.ingested_at <= p_as_of
WHERE p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id
GROUP BY d.department_sk, d.department_id;
$$;
COMMIT;

-- API usage examples (execute separately after loading design fixtures):
-- SELECT * FROM mart.op03_noshow_rate(DATE '2026-10-01', DATE '2026-10-31');
-- SELECT * FROM mart.rt01_checkins_last_15m(TIMESTAMPTZ '2026-10-22 10:00:00+07');
-- One period-wide waiting average: SUM(waiting_minutes_sum)/NULLIF(SUM(waiting_valid_count),0).
-- Snapshot occupancy: select one report_date. Multi-day bed-day rate must be labelled separately.
