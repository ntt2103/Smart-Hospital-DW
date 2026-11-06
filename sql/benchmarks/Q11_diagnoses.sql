SELECT b.is_primary,count(DISTINCT f.encounter_id) AS encounters
FROM dw.fact_encounter f JOIN dw.bridge_encounter_diagnosis b USING(encounter_id)
WHERE b.diagnosis_id=%(diagnosis)s AND b.is_active
GROUP BY 1 ORDER BY 1;
