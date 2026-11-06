SELECT count(*) AS encounters,sum(f.waiting_minutes) AS waiting_minutes
FROM dw.fact_encounter f
WHERE EXISTS(SELECT 1 FROM dw.bridge_encounter_doctor b WHERE b.encounter_id=f.encounter_id AND b.doctor_id=%(doctor)s AND b.is_active)
AND EXISTS(SELECT 1 FROM dw.bridge_encounter_diagnosis b WHERE b.encounter_id=f.encounter_id AND b.diagnosis_id=%(diagnosis)s AND b.is_active);
