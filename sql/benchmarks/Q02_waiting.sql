SELECT f.department_sk,sum(f.waiting_minutes)/nullif(count(*) FILTER(WHERE f.is_waiting_eligible),0) AS waiting_minutes
FROM dw.fact_encounter f WHERE f.encounter_type='OUTPATIENT'
AND f.consultation_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY f.department_sk ORDER BY 1;
