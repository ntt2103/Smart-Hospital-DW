SELECT discharge_date_key,department_sk,sum(los_days)/nullif(count(*),0) AS los_days
FROM dw.fact_encounter WHERE encounter_type='INPATIENT' AND is_los_eligible
AND discharge_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY 1,2 ORDER BY 1,2;
