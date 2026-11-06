SELECT d.doctor_id,d.doctor_sk,d.effective_from::text,d.effective_to::text,d.is_active,count(f.encounter_id) AS encounters,
count(f.encounter_id) FILTER(WHERE f.doctor_sk=0) AS historical_unknown
FROM dw.dim_doctor d LEFT JOIN dw.fact_encounter f ON f.doctor_sk=d.doctor_sk
AND (d.doctor_sk<>0 OR f.doctor_id=%(doctor)s)
WHERE d.doctor_id=%(doctor)s OR d.doctor_sk=0
GROUP BY 1,2,3,4,5 ORDER BY d.doctor_sk;
