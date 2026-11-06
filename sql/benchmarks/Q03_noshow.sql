SELECT scheduled_date_key,department_sk,
100.0*count(*) FILTER(WHERE status='NO_SHOW')/nullif(count(*),0) AS no_show_rate
FROM dw.fact_appointment WHERE is_noshow_eligible
AND scheduled_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY 1,2 ORDER BY 1,2;
