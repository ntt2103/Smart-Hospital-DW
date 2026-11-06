SELECT f.encounter_id,f.encounter_type,f.checked_in_at,f.admitted_at,f.status
FROM dw.fact_encounter f JOIN dw.dim_patient p USING(patient_sk)
WHERE p.patient_id=%(patient)s
ORDER BY coalesce(f.checked_in_at,f.admitted_at) DESC,f.encounter_id DESC LIMIT 50;
