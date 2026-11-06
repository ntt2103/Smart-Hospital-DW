-- Phase 5: canonical validated raw events are the RT input. No event fact.
-- Apply after the original warehouse, KPI and streaming helper migrations.
CREATE OR REPLACE FUNCTION mart.rt01_checkins_last_15m(p_as_of TIMESTAMPTZ, p_department_id BIGINT DEFAULT NULL)
RETURNS TABLE(as_of TIMESTAMPTZ, department_id BIGINT, value NUMERIC)
LANGUAGE sql STABLE AS $$
SELECT p_as_of, coalesce(d.department_id, 0), count(DISTINCT e.entity_id)::NUMERIC
FROM dw.dim_department d
LEFT JOIN raw.events e ON (e.payload->>'department_id')::BIGINT = d.department_id
 AND e.topic = 'encounters.events' AND e.event_type = 'checked_in'
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
LEFT JOIN raw.events e ON (e.payload->>'department_id')::BIGINT = d.department_id
 AND e.topic = 'laboratory.events' AND e.event_type = 'result_completed'
 AND e.event_time > p_as_of - INTERVAL '15 minutes' AND e.event_time <= p_as_of
 AND e.ingested_at <= p_as_of
WHERE p_department_id IS NULL OR coalesce(d.department_id, 0) = p_department_id
GROUP BY d.department_sk, d.department_id;
$$;

-- These are processing receipts, not a fact table or copies of every event.
-- Only RT signal events actually included by a committed refresh get a receipt.
CREATE TABLE ops.rt_event_receipts (
    run_id UUID NOT NULL REFERENCES ops.pipeline_runs(run_id),
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('checked_in','result_completed')),
    entity_id BIGINT NOT NULL CHECK(entity_id > 0),
    department_sk BIGINT NOT NULL REFERENCES dw.dim_department,
    event_time TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(run_id,event_id),
    CHECK(event_time > as_of - INTERVAL '15 minutes' AND event_time <= as_of AND ingested_at <= as_of)
);
CREATE INDEX rt_event_receipts_event_idx ON ops.rt_event_receipts(event_id,as_of);
CREATE OR REPLACE VIEW mart.event_visibility AS
SELECT DISTINCT ON (r.event_id) r.event_id,r.event_type,r.entity_id,
       r.published_at,r.ingested_at,r.as_of AS visible_as_of
FROM ops.rt_event_receipts r
JOIN ops.pipeline_runs p USING(run_id)
WHERE p.status='SUCCEEDED'
ORDER BY r.event_id,r.as_of;
COMMENT ON VIEW mart.event_visibility IS
 'First committed snapshot receipt for an RT signal; raw arrival alone is not visibility.';
GRANT SELECT,INSERT ON ops.rt_event_receipts TO hospital_etl;
GRANT SELECT ON mart.event_visibility TO hospital_reporter;

-- Verify the removed cache is reconstructable from immutable canonical raw.
-- Refuse cutover on missing/mismatched input; the migration transaction rolls back.
DO $$ BEGIN
 IF EXISTS (
   SELECT 1 FROM dw.fact_operational_event f
   LEFT JOIN raw.events e USING(event_id)
   LEFT JOIN dw.dim_department d ON d.department_sk=f.department_sk
   WHERE e.event_id IS NULL OR
    (f.topic,f.event_type,f.entity_id,f.event_time,f.published_at,f.ingested_at,d.department_id)
      IS DISTINCT FROM
    (e.topic,e.event_type,e.entity_id,e.event_time,e.published_at,e.ingested_at,(e.payload->>'department_id')::BIGINT)
 ) THEN
   RAISE EXCEPTION 'Event cache/canonical raw parity failed; cutover rolled back';
 END IF;
END $$;
DROP TABLE dw.fact_operational_event;
