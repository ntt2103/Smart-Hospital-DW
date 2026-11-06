SELECT d.calendar_date,f.department_sk,count(*) AS visits
FROM dw.fact_encounter f JOIN dw.dim_date d ON d.date_key=f.consultation_date_key
WHERE f.encounter_type='OUTPATIENT' AND f.is_visit_eligible
AND f.consultation_date_key BETWEEN %(start_key)s AND %(end_key)s
AND (%(department)s::bigint IS NULL OR f.department_sk=%(department)s)
GROUP BY d.calendar_date,f.department_sk ORDER BY 1,2;
