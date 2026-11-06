-- Restore the exact measured optional indexes after the control baseline.
CREATE INDEX phase5_doctor_participation_lookup
ON dw.bridge_encounter_doctor(doctor_id,encounter_id) INCLUDE(role) WHERE is_active;
CREATE INDEX phase5_diagnosis_lookup
ON dw.bridge_encounter_diagnosis(diagnosis_id,encounter_id) INCLUDE(is_primary) WHERE is_active;
CREATE INDEX phase5_patient_history_lookup
ON dw.fact_encounter(patient_sk,(coalesce(checked_in_at,admitted_at)) DESC,encounter_id DESC);
