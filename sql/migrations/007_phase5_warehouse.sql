-- Phase 5: observed SCD2, one encounter grain, two multi-valued bridges.
-- Applied transactionally after immutable 003/004/005. Back up before cutover.
ALTER TABLE raw.source_rows DROP CONSTRAINT source_rows_source_table_check;
ALTER TABLE raw.source_rows ADD CONSTRAINT source_rows_source_table_check CHECK(source_table IN (
 'patients','departments','doctors','appointments','encounters','beds','bed_status_history',
 'bed_assignments','lab_test_types','lab_orders','diagnoses','encounter_doctors','encounter_diagnoses'));
CREATE TABLE dw.fact_encounter (
    encounter_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    encounter_id BIGINT NOT NULL UNIQUE CHECK(encounter_id>0),
    encounter_type TEXT NOT NULL CHECK(encounter_type IN ('OUTPATIENT','INPATIENT')),
    appointment_id BIGINT,
    parent_encounter_id BIGINT,
    patient_sk BIGINT NOT NULL REFERENCES dw.dim_patient,
    department_sk BIGINT NOT NULL REFERENCES dw.dim_department,
    doctor_sk BIGINT NOT NULL REFERENCES dw.dim_doctor,
    doctor_id BIGINT,
    checked_in_date_key INTEGER NOT NULL REFERENCES dw.dim_date,
    consultation_date_key INTEGER NOT NULL REFERENCES dw.dim_date,
    admission_date_key INTEGER NOT NULL REFERENCES dw.dim_date,
    discharge_date_key INTEGER NOT NULL REFERENCES dw.dim_date,
    checked_in_at TIMESTAMPTZ,
    consultation_started_at TIMESTAMPTZ,
    consultation_completed_at TIMESTAMPTZ,
    admitted_at TIMESTAMPTZ,
    discharged_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    is_visit_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    is_waiting_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    is_admission_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    is_los_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    waiting_minutes NUMERIC GENERATED ALWAYS AS
      (CASE WHEN is_waiting_eligible THEN extract(epoch FROM consultation_started_at-checked_in_at)/60.0 END) STORED,
    los_days NUMERIC GENERATED ALWAYS AS
      (CASE WHEN is_los_eligible THEN extract(epoch FROM discharged_at-admitted_at)/86400.0 END) STORED,
    source_updated_at TIMESTAMPTZ NOT NULL,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK((encounter_type='OUTPATIENT' AND admitted_at IS NULL AND discharged_at IS NULL
      AND admission_date_key=0 AND discharge_date_key=0 AND NOT is_admission_eligible AND NOT is_los_eligible)
      OR (encounter_type='INPATIENT' AND checked_in_at IS NULL AND consultation_started_at IS NULL
      AND consultation_completed_at IS NULL AND appointment_id IS NULL AND checked_in_date_key=0
      AND consultation_date_key=0 AND NOT is_visit_eligible AND NOT is_waiting_eligible)),
    CHECK(NOT is_visit_eligible OR (consultation_started_at IS NOT NULL AND consultation_date_key<>0)),
    CHECK(NOT is_waiting_eligible OR (is_visit_eligible AND checked_in_at IS NOT NULL AND consultation_started_at>=checked_in_at)),
    CHECK(NOT is_admission_eligible OR (admitted_at IS NOT NULL AND admission_date_key<>0)),
    CHECK(NOT is_los_eligible OR (is_admission_eligible AND status='DISCHARGED' AND discharged_at>admitted_at AND discharge_date_key<>0))
);

INSERT INTO dw.fact_encounter(encounter_id,encounter_type,appointment_id,patient_sk,department_sk,doctor_sk,doctor_id,
 checked_in_date_key,consultation_date_key,admission_date_key,discharge_date_key,checked_in_at,
 consultation_started_at,consultation_completed_at,status,is_visit_eligible,is_waiting_eligible,source_updated_at,batch_id,loaded_at)
SELECT encounter_id,'OUTPATIENT',appointment_id,patient_sk,department_sk,doctor_sk,doctor_id,
 checked_in_date_key,consultation_date_key,0,0,checked_in_at,consultation_started_at,
 consultation_completed_at,status,is_visit_eligible,is_waiting_eligible,source_updated_at,batch_id,loaded_at
FROM dw.fact_outpatient_encounter;
INSERT INTO dw.fact_encounter(encounter_id,encounter_type,parent_encounter_id,patient_sk,department_sk,doctor_sk,doctor_id,
 checked_in_date_key,consultation_date_key,admission_date_key,discharge_date_key,admitted_at,
 discharged_at,status,is_admission_eligible,is_los_eligible,source_updated_at,batch_id,loaded_at)
SELECT encounter_id,'INPATIENT',parent_encounter_id,patient_sk,department_sk,doctor_sk,doctor_id,
 0,0,admission_date_key,discharge_date_key,admitted_at,discharged_at,status,
 is_admission_eligible,is_los_eligible,source_updated_at,batch_id,loaded_at
FROM dw.fact_inpatient_encounter;

-- Equivalent indexes carried over from the old physical facts; baseline has no new tuning.
CREATE INDEX ix_encounter_op_date_dept ON dw.fact_encounter(consultation_date_key,department_sk)
 WHERE encounter_type='OUTPATIENT';
CREATE INDEX ix_encounter_ip_admission_dept ON dw.fact_encounter(admission_date_key,department_sk)
 WHERE encounter_type='INPATIENT';
CREATE INDEX ix_encounter_ip_discharge_dept ON dw.fact_encounter(discharge_date_key,department_sk)
 WHERE encounter_type='INPATIENT';

-- Bidirectional equality of every persisted business value, including eligibility and measures.
DO $$ BEGIN
 IF EXISTS(
  (SELECT to_jsonb(o)-'outpatient_encounter_sk' FROM dw.fact_outpatient_encounter o
   EXCEPT SELECT to_jsonb(n)-ARRAY['encounter_sk','encounter_type','parent_encounter_id','admission_date_key','discharge_date_key','admitted_at','discharged_at','is_admission_eligible','is_los_eligible','los_days']
    FROM dw.fact_encounter n WHERE encounter_type='OUTPATIENT')
  UNION ALL
  (SELECT to_jsonb(n)-ARRAY['encounter_sk','encounter_type','parent_encounter_id','admission_date_key','discharge_date_key','admitted_at','discharged_at','is_admission_eligible','is_los_eligible','los_days']
    FROM dw.fact_encounter n WHERE encounter_type='OUTPATIENT'
   EXCEPT SELECT to_jsonb(o)-'outpatient_encounter_sk' FROM dw.fact_outpatient_encounter o)
 ) THEN RAISE EXCEPTION 'OP migration parity failed'; END IF;
 IF EXISTS(
  (SELECT to_jsonb(o)-'inpatient_encounter_sk' FROM dw.fact_inpatient_encounter o
   EXCEPT SELECT to_jsonb(n)-ARRAY['encounter_sk','encounter_type','appointment_id','checked_in_date_key','consultation_date_key','checked_in_at','consultation_started_at','consultation_completed_at','is_visit_eligible','is_waiting_eligible','waiting_minutes']
    FROM dw.fact_encounter n WHERE encounter_type='INPATIENT')
  UNION ALL
  (SELECT to_jsonb(n)-ARRAY['encounter_sk','encounter_type','appointment_id','checked_in_date_key','consultation_date_key','checked_in_at','consultation_started_at','consultation_completed_at','is_visit_eligible','is_waiting_eligible','waiting_minutes']
    FROM dw.fact_encounter n WHERE encounter_type='INPATIENT'
   EXCEPT SELECT to_jsonb(o)-'inpatient_encounter_sk' FROM dw.fact_inpatient_encounter o)
 ) THEN RAISE EXCEPTION 'IP migration parity failed'; END IF;
END $$;

CREATE OR REPLACE VIEW mart.outpatient_daily AS
SELECT d.calendar_date report_date,f.department_sk,
 count(*) FILTER(WHERE is_visit_eligible) visit_count,
 count(*) FILTER(WHERE is_waiting_eligible) waiting_valid_count,
 sum(waiting_minutes) FILTER(WHERE is_waiting_eligible) waiting_minutes_sum
FROM dw.fact_encounter f JOIN dw.dim_date d ON d.date_key=f.consultation_date_key
WHERE f.encounter_type='OUTPATIENT' AND d.calendar_date IS NOT NULL GROUP BY d.calendar_date,f.department_sk;
CREATE OR REPLACE VIEW mart.inpatient_admission_daily AS
SELECT d.calendar_date report_date,f.department_sk,count(*) FILTER(WHERE is_admission_eligible) admission_count
FROM dw.fact_encounter f JOIN dw.dim_date d ON d.date_key=f.admission_date_key
WHERE f.encounter_type='INPATIENT' AND d.calendar_date IS NOT NULL GROUP BY d.calendar_date,f.department_sk;
CREATE OR REPLACE VIEW mart.inpatient_discharge_daily AS
SELECT d.calendar_date report_date,f.department_sk,count(*) FILTER(WHERE is_los_eligible) los_valid_count,
 sum(los_days) FILTER(WHERE is_los_eligible) los_days_sum
FROM dw.fact_encounter f JOIN dw.dim_date d ON d.date_key=f.discharge_date_key
WHERE f.encounter_type='INPATIENT' AND d.calendar_date IS NOT NULL GROUP BY d.calendar_date,f.department_sk;

-- No CASCADE: unhandled external dependencies stop this transaction safely.
DROP TABLE dw.fact_outpatient_encounter;
DROP TABLE dw.fact_inpatient_encounter;
-- Temporary compatibility views for existing audits; never counted as physical facts.
CREATE VIEW dw.fact_outpatient_encounter AS
SELECT encounter_sk outpatient_encounter_sk,encounter_id,appointment_id,patient_sk,department_sk,doctor_sk,doctor_id,
 checked_in_date_key,consultation_date_key,checked_in_at,consultation_started_at,consultation_completed_at,status,
 is_visit_eligible,is_waiting_eligible,waiting_minutes,source_updated_at,batch_id,loaded_at
FROM dw.fact_encounter WHERE encounter_type='OUTPATIENT';
CREATE VIEW dw.fact_inpatient_encounter AS
SELECT encounter_sk inpatient_encounter_sk,encounter_id,parent_encounter_id,patient_sk,department_sk,doctor_sk,doctor_id,
 admission_date_key,discharge_date_key,admitted_at,discharged_at,status,is_admission_eligible,is_los_eligible,
 los_days,source_updated_at,batch_id,loaded_at
FROM dw.fact_encounter WHERE encounter_type='INPATIENT';
COMMENT ON VIEW dw.fact_outpatient_encounter IS 'Temporary Phase 5 compatibility view; physical grain is dw.fact_encounter.';
COMMENT ON VIEW dw.fact_inpatient_encounter IS 'Temporary Phase 5 compatibility view; physical grain is dw.fact_encounter.';

CREATE TABLE dw.dim_diagnosis (
 diagnosis_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
 diagnosis_id BIGINT UNIQUE CHECK(diagnosis_id>0),
 diagnosis_code TEXT UNIQUE,
 diagnosis_name TEXT,
 is_active BOOLEAN,
 source_updated_at TIMESTAMPTZ,
 loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO dw.dim_diagnosis(diagnosis_sk,diagnosis_code,diagnosis_name) VALUES(0,'UNKNOWN','Không xác định');
CREATE TABLE dw.bridge_encounter_doctor (
 association_id BIGINT PRIMARY KEY CHECK(association_id>0),
 encounter_id BIGINT NOT NULL REFERENCES dw.fact_encounter(encounter_id),
 doctor_id BIGINT NOT NULL,
 doctor_sk BIGINT NOT NULL REFERENCES dw.dim_doctor,
 role TEXT NOT NULL CHECK(role IN ('PRIMARY','CONSULTING')),
 joined_at TIMESTAMPTZ NOT NULL,
 left_at TIMESTAMPTZ,
 is_active BOOLEAN NOT NULL,
 source_updated_at TIMESTAMPTZ NOT NULL,
 batch_id UUID NOT NULL,
 loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 CHECK(left_at IS NULL OR left_at>joined_at)
);
CREATE TABLE dw.bridge_encounter_diagnosis (
 association_id BIGINT PRIMARY KEY CHECK(association_id>0),
 encounter_id BIGINT NOT NULL REFERENCES dw.fact_encounter(encounter_id),
 diagnosis_id BIGINT NOT NULL,
 diagnosis_sk BIGINT NOT NULL REFERENCES dw.dim_diagnosis,
 is_primary BOOLEAN NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_updated_at TIMESTAMPTZ NOT NULL,
 batch_id UUID NOT NULL,
 loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE VIEW mart.encounter_participations AS
SELECT b.association_id,b.encounter_id,b.doctor_id,b.doctor_sk,b.role,b.joined_at,b.left_at,b.is_active,
 f.encounter_type,f.department_sk FROM dw.bridge_encounter_doctor b JOIN dw.fact_encounter f USING(encounter_id);
CREATE VIEW mart.encounter_diagnoses AS
SELECT b.association_id,b.encounter_id,b.diagnosis_id,d.diagnosis_code,d.diagnosis_name,
 b.is_primary,b.recorded_at,b.is_active,f.encounter_type,f.department_sk
FROM dw.bridge_encounter_diagnosis b JOIN dw.dim_diagnosis d USING(diagnosis_sk) JOIN dw.fact_encounter f USING(encounter_id);
-- Filtering multi-valued relations before aggregating preserves one encounter even for 2 doctors x 3 diagnoses.
CREATE FUNCTION mart.encounters_by_doctor_diagnosis(p_doctor_id BIGINT DEFAULT NULL,p_diagnosis_id BIGINT DEFAULT NULL)
RETURNS TABLE(encounter_id BIGINT,encounter_type TEXT,department_sk BIGINT) LANGUAGE sql STABLE AS $$
 SELECT f.encounter_id,f.encounter_type,f.department_sk FROM dw.fact_encounter f
 WHERE (p_doctor_id IS NULL OR EXISTS(SELECT 1 FROM dw.bridge_encounter_doctor b WHERE b.encounter_id=f.encounter_id AND b.doctor_id=p_doctor_id AND b.is_active))
 AND (p_diagnosis_id IS NULL OR EXISTS(SELECT 1 FROM dw.bridge_encounter_diagnosis b WHERE b.encounter_id=f.encounter_id AND b.diagnosis_id=p_diagnosis_id AND b.is_active))
$$;
