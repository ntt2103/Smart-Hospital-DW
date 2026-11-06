SELECT b.role,count(DISTINCT f.encounter_id) AS encounters,count(*) AS participations
FROM dw.fact_encounter f JOIN dw.bridge_encounter_doctor b USING(encounter_id)
WHERE b.doctor_id=%(doctor)s AND b.is_active
GROUP BY b.role ORDER BY b.role;
