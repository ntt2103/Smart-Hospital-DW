-- Transactional SQL restatement; bounded Python extraction/reconciliation. All statements share the batch transaction.
INSERT INTO dw.dim_date(date_key,calendar_date,calendar_year,calendar_quarter,calendar_month,day_of_month,iso_day_of_week,is_weekend)
SELECT to_char(d,'YYYYMMDD')::int,d::date,extract(year from d),extract(quarter from d),extract(month from d),extract(day from d),extract(isodow from d),extract(isodow from d)>=6
FROM generate_series(
 (SELECT min(business_date) FROM (SELECT (commissioned_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date business_date FROM stg.beds UNION ALL SELECT (scheduled_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM stg.appointments UNION ALL SELECT (ordered_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM stg.lab_orders UNION ALL SELECT (coalesce(admitted_at,checked_in_at,consultation_started_at) AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM stg.encounters) x),
 (SELECT greatest((as_of AT TIME ZONE 'Asia/Ho_Chi_Minh')::date+1,
                  (SELECT max((scheduled_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) FROM stg.appointments)) FROM run_context),interval '1 day') d
ON CONFLICT(date_key) DO NOTHING;

CREATE TEMP TABLE next_patient ON COMMIT DROP AS SELECT patient_id,patient_code,date_of_birth,gender,is_active,updated_at source_updated_at FROM stg.patients;
SELECT dw.merge_current('dw.dim_patient','next_patient',ARRAY['patient_id']);
CREATE TEMP TABLE next_department ON COMMIT DROP AS SELECT department_id,department_code,department_name,is_active,updated_at source_updated_at FROM stg.departments;
SELECT dw.merge_current('dw.dim_department','next_department',ARRAY['department_id']);
UPDATE dw.dim_doctor d SET effective_to=r.observed_at FROM stg.doctors s JOIN dw.dim_department dp ON dp.department_id=s.department_id CROSS JOIN run_context r
WHERE d.doctor_id=s.doctor_id AND d.effective_to IS NULL AND (d.doctor_code,d.department_sk,d.is_active) IS DISTINCT FROM (s.doctor_code,dp.department_sk,s.is_active);
INSERT INTO dw.dim_doctor(doctor_id,doctor_code,department_sk,is_active,effective_from,source_updated_at)
SELECT s.doctor_id,s.doctor_code,dp.department_sk,s.is_active,r.observed_at,s.updated_at
FROM stg.doctors s JOIN dw.dim_department dp USING(department_id) CROSS JOIN run_context r
WHERE NOT EXISTS(SELECT 1 FROM dw.dim_doctor d WHERE d.doctor_id=s.doctor_id AND d.effective_to IS NULL);
CREATE TEMP TABLE next_bed ON COMMIT DROP AS SELECT b.bed_id,b.bed_code,b.bed_type,b.commissioned_at,d.department_sk,b.updated_at source_updated_at FROM stg.beds b JOIN dw.dim_department d USING(department_id);
SELECT dw.merge_current('dw.dim_bed','next_bed',ARRAY['bed_id']);
CREATE TEMP TABLE next_test ON COMMIT DROP AS SELECT lab_test_type_id,test_code,test_name,default_tat_minutes,is_active,updated_at source_updated_at FROM stg.lab_test_types;
SELECT dw.merge_current('dw.dim_lab_test_type','next_test',ARRAY['lab_test_type_id']);

CREATE TEMP TABLE next_appointment ON COMMIT DROP AS
SELECT a.appointment_id,a.rescheduled_from_appointment_id,p.patient_sk,dp.department_sk,coalesce(d.doctor_sk,0) doctor_sk,a.doctor_id,
dw.local_date_key(a.scheduled_at) scheduled_date_key,a.scheduled_at,a.status,a.status_changed_at,r.as_of eligibility_as_of,
coalesce(a.scheduled_at<=r.as_of AND a.status IN('ATTENDED','NO_SHOW') AND a.status_changed_at<=r.as_of AND (a.status<>'NO_SHOW' OR a.status_changed_at>=a.scheduled_at+interval '30 minutes'),false) is_noshow_eligible,
a.updated_at source_updated_at,r.run_id batch_id
FROM stg.appointments a JOIN dw.dim_patient p USING(patient_id) JOIN dw.dim_department dp USING(department_id) CROSS JOIN run_context r
LEFT JOIN dw.dim_doctor d ON d.doctor_id=a.doctor_id AND a.scheduled_at>=d.effective_from AND (d.effective_to IS NULL OR a.scheduled_at<d.effective_to);
SELECT dw.merge_current('dw.fact_appointment','next_appointment',ARRAY['appointment_id']);

CREATE TEMP TABLE next_encounter ON COMMIT DROP AS
SELECT e.encounter_id,e.encounter_type,e.appointment_id,e.parent_encounter_id,p.patient_sk,dp.department_sk,
coalesce(d.doctor_sk,0) doctor_sk,e.doctor_id,
dw.local_date_key(e.checked_in_at) checked_in_date_key,dw.local_date_key(e.consultation_started_at) consultation_date_key,
dw.local_date_key(e.admitted_at) admission_date_key,dw.local_date_key(e.discharged_at) discharge_date_key,
e.checked_in_at,e.consultation_started_at,e.consultation_completed_at,e.admitted_at,e.discharged_at,e.status,
coalesce(e.encounter_type='OUTPATIENT' AND e.consultation_started_at<=r.as_of,false) is_visit_eligible,
coalesce(e.encounter_type='OUTPATIENT' AND e.consultation_started_at<=r.as_of AND e.checked_in_at<=e.consultation_started_at,false) is_waiting_eligible,
coalesce(e.encounter_type='INPATIENT' AND e.admitted_at<=r.as_of,false) is_admission_eligible,
coalesce(e.encounter_type='INPATIENT' AND e.admitted_at<=r.as_of AND e.status='DISCHARGED' AND e.discharged_at>e.admitted_at AND e.discharged_at<=r.as_of,false) is_los_eligible,
e.updated_at source_updated_at,r.run_id batch_id
FROM stg.encounters e JOIN dw.dim_patient p USING(patient_id) JOIN dw.dim_department dp USING(department_id) CROSS JOIN run_context r
LEFT JOIN dw.dim_doctor d ON d.doctor_id=e.doctor_id
AND coalesce(e.consultation_started_at,e.checked_in_at,e.admitted_at)>=d.effective_from
AND (d.effective_to IS NULL OR coalesce(e.consultation_started_at,e.checked_in_at,e.admitted_at)<d.effective_to);
SELECT dw.merge_current('dw.fact_encounter','next_encounter',ARRAY['encounter_id']);

CREATE TEMP TABLE next_diagnosis ON COMMIT DROP AS
SELECT diagnosis_id,diagnosis_code,diagnosis_name,is_active,updated_at source_updated_at FROM stg.diagnoses;
SELECT dw.merge_current('dw.dim_diagnosis','next_diagnosis',ARRAY['diagnosis_id']);
CREATE TEMP TABLE next_doctor_bridge ON COMMIT DROP AS
SELECT b.association_id,b.encounter_id,b.doctor_id,coalesce(d.doctor_sk,0) doctor_sk,b.role,
b.joined_at,b.left_at,b.is_active,b.updated_at source_updated_at,r.run_id batch_id
FROM stg.encounter_doctors b CROSS JOIN run_context r
LEFT JOIN dw.dim_doctor d ON d.doctor_id=b.doctor_id AND b.joined_at>=d.effective_from
AND (d.effective_to IS NULL OR b.joined_at<d.effective_to);
SELECT dw.merge_current('dw.bridge_encounter_doctor','next_doctor_bridge',ARRAY['association_id']);
CREATE TEMP TABLE next_diagnosis_bridge ON COMMIT DROP AS
SELECT b.association_id,b.encounter_id,b.diagnosis_id,coalesce(d.diagnosis_sk,0) diagnosis_sk,
b.is_primary,b.recorded_at,b.is_active,b.updated_at source_updated_at,r.run_id batch_id
FROM stg.encounter_diagnoses b CROSS JOIN run_context r LEFT JOIN dw.dim_diagnosis d USING(diagnosis_id);
SELECT dw.merge_current('dw.bridge_encounter_diagnosis','next_diagnosis_bridge',ARRAY['association_id']);
CREATE TEMP TABLE next_assignment ON COMMIT DROP AS
SELECT a.bed_assignment_id,a.encounter_id,p.patient_sk,b.department_sk,b.bed_sk,
dw.local_date_key(a.assigned_at) assigned_date_key,dw.local_date_key(a.released_at) released_date_key,
a.assigned_at,a.released_at,true is_assignment_valid,a.updated_at source_updated_at,r.run_id batch_id
FROM stg.bed_assignments a JOIN stg.encounters e USING(encounter_id) JOIN dw.dim_patient p USING(patient_id) JOIN dw.dim_bed b USING(bed_id) CROSS JOIN run_context r;
SELECT dw.merge_current('dw.fact_bed_assignment','next_assignment',ARRAY['bed_assignment_id']);

CREATE TEMP TABLE next_lab ON COMMIT DROP AS
SELECT l.lab_order_id,l.encounter_id,p.patient_sk,dp.department_sk,coalesce(d.doctor_sk,0) doctor_sk,e.doctor_id,t.lab_test_type_sk,
dw.local_date_key(l.ordered_at) ordered_date_key,dw.local_date_key(l.result_completed_at) result_date_key,
l.ordered_at,l.collected_at,l.processing_started_at,l.result_completed_at,l.target_tat_minutes_at_order,l.status,
coalesce(l.status='COMPLETED' AND l.result_completed_at<=r.as_of,false) is_completed_eligible,
coalesce(l.status='COMPLETED' AND l.result_completed_at<=r.as_of AND l.result_completed_at>=l.ordered_at,false) is_tat_eligible,
coalesce(l.status='COMPLETED' AND l.result_completed_at<=r.as_of AND l.result_completed_at>=l.ordered_at AND l.target_tat_minutes_at_order>0,false) is_ontime_eligible,
l.updated_at source_updated_at,r.run_id batch_id
FROM stg.lab_orders l JOIN stg.encounters e USING(encounter_id) JOIN dw.dim_patient p USING(patient_id) JOIN dw.dim_department dp ON dp.department_id=l.department_id JOIN dw.dim_lab_test_type t USING(lab_test_type_id) CROSS JOIN run_context r
LEFT JOIN dw.dim_doctor d ON d.doctor_id=e.doctor_id AND l.ordered_at>=d.effective_from AND (d.effective_to IS NULL OR l.ordered_at<d.effective_to);
SELECT dw.merge_current('dw.fact_lab_order','next_lab',ARRAY['lab_order_id']);

CREATE TEMP TABLE next_snapshot ON COMMIT DROP AS
SELECT b.bed_sk,b.department_sk,d.date_key snapshot_date_key,
d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh' snapshot_at,h.status bed_status,
coalesce(h.status='ACTIVE',false) is_active,(a.bed_assignment_id IS NOT NULL) is_occupied,
(h.bed_status_history_id IS NOT NULL AND (a.bed_assignment_id IS NULL OR h.status='ACTIVE')) is_snapshot_valid,
a.bed_assignment_id,a.encounter_id,r.run_id batch_id
FROM dw.dim_bed b CROSS JOIN dw.dim_date d CROSS JOIN run_context r
LEFT JOIN stg.bed_status_history h ON h.bed_id=b.bed_id AND (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')>=h.effective_from AND (h.effective_to IS NULL OR (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')<h.effective_to)
LEFT JOIN stg.bed_assignments a ON a.bed_id=b.bed_id AND (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')>=a.assigned_at AND (a.released_at IS NULL OR (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')<a.released_at)
WHERE b.bed_sk<>0 AND d.calendar_date IS NOT NULL AND (r.snapshot_start IS NULL OR d.calendar_date>=r.snapshot_start) AND (r.snapshot_end IS NULL OR d.calendar_date<=r.snapshot_end)
AND (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')>=b.commissioned_at AND (d.calendar_date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')<=r.as_of;
DELETE FROM dw.fact_bed_snapshot_daily f USING dw.dim_date d,run_context r WHERE d.date_key=f.snapshot_date_key
AND (r.snapshot_start IS NULL OR d.calendar_date>=r.snapshot_start) AND (r.snapshot_end IS NULL OR d.calendar_date<=r.snapshot_end)
AND NOT EXISTS(SELECT 1 FROM next_snapshot n WHERE (n.bed_sk,n.snapshot_date_key)=(f.bed_sk,f.snapshot_date_key));
SELECT dw.merge_current('dw.fact_bed_snapshot_daily','next_snapshot',ARRAY['bed_sk','snapshot_date_key']);
