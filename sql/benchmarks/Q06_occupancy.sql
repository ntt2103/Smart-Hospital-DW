SELECT department_sk,
CASE WHEN count(*) FILTER(WHERE NOT is_snapshot_valid)=0 THEN
100.0*count(*) FILTER(WHERE is_snapshot_valid AND is_occupied)/nullif(count(*) FILTER(WHERE is_snapshot_valid AND is_active),0) END AS occupancy
FROM dw.fact_bed_snapshot_daily WHERE snapshot_date_key=%(end_key)s
GROUP BY 1 ORDER BY 1;
