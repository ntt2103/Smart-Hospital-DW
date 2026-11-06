CREATE VIEW mart.event_visibility AS
SELECT e.event_id,e.event_type,e.entity_id,e.published_at,e.ingested_at,s.as_of AS visible_as_of
FROM dw.fact_operational_event e
CROSS JOIN (SELECT max(as_of) as_of FROM mart.realtime_department_snapshot) s
WHERE e.is_rt_eligible AND e.event_time>s.as_of-interval '15 minutes' AND e.event_time<=s.as_of AND e.ingested_at<=s.as_of
AND e.event_type IN ('checked_in','result_completed');
