SELECT result_date_key,department_sk,count(*) FILTER(WHERE is_completed_eligible) AS completed,
sum(tat_minutes)/nullif(count(*) FILTER(WHERE is_tat_eligible),0) AS tat_minutes,
100.0*count(*) FILTER(WHERE is_ontime_eligible AND tat_minutes<=target_tat_minutes_at_order)/nullif(count(*) FILTER(WHERE is_ontime_eligible),0) AS ontime_percent
FROM dw.fact_lab_order WHERE result_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY 1,2 ORDER BY 1,2;
