SELECT admission_date_key,department_sk,count(*) AS admissions
FROM dw.fact_encounter WHERE encounter_type='INPATIENT' AND is_admission_eligible
AND admission_date_key BETWEEN %(start_key)s AND %(end_key)s
GROUP BY 1,2 ORDER BY 1,2;
