"""Full set-based source DQ. Every row is checked; only error examples are bounded."""
import re
import hashlib
import time

from data_sources.generator.historical import TABLE_KEYS


def validate_database(conn, as_of, schema='public', evidence=None):
    if not re.fullmatch(r'[a-z_][a-z_0-9]*', schema):
        raise ValueError('Invalid source schema')
    s = schema
    issues = []
    checks_executed=[]

    def check(rule, query):
        started=time.monotonic()
        row = conn.execute(f'SELECT count(*),min(k::text) FROM ({query}) failures', {'as_of': as_of}).fetchone()
        checks_executed.append({'rule':rule,'query_sha256':hashlib.sha256(query.encode()).hexdigest(),
                                'failed_count':row[0],'elapsed_ms':round((time.monotonic()-started)*1000,3)})
        if row[0]:
            key = int(row[1]) if row[1] and row[1].isdigit() else row[1]
            issues.append({'rule': rule, 'key': key, 'failed_count': row[0]})

    refs = {
        'doctors': [('department_id','departments')], 'beds': [('department_id','departments')],
        'appointments': [('patient_id','patients'),('department_id','departments'),('doctor_id','doctors'),('rescheduled_from_appointment_id','appointments')],
        'encounters': [('patient_id','patients'),('department_id','departments'),('doctor_id','doctors'),('appointment_id','appointments'),('parent_encounter_id','encounters')],
        'bed_status_history': [('bed_id','beds')], 'bed_assignments': [('bed_id','beds'),('encounter_id','encounters')],
        'lab_orders': [('encounter_id','encounters'),('department_id','departments'),('lab_test_type_id','lab_test_types')],
        'encounter_doctors': [('encounter_id','encounters'),('doctor_id','doctors')],
        'encounter_diagnoses': [('encounter_id','encounters'),('diagnosis_id','diagnoses')],
    }
    for table, pk in TABLE_KEYS.items():
        check('AUDIT_TIME', f'SELECT {pk} k FROM {s}.{table} WHERE updated_at<created_at')
        for field, target in refs.get(table, []):
            check('FK', f'SELECT t.{pk} k FROM {s}.{table} t LEFT JOIN {s}.{target} p ON p.{TABLE_KEYS[target]}=t.{field} WHERE t.{field} IS NOT NULL AND p.{TABLE_KEYS[target]} IS NULL')
    checks = {
        'ATTENDANCE_LINK': f"""SELECT e.encounter_id k FROM {s}.encounters e JOIN {s}.appointments a USING(appointment_id)
          WHERE e.encounter_type<>'OUTPATIENT' OR a.status<>'ATTENDED' OR e.patient_id<>a.patient_id OR e.department_id<>a.department_id""",
        'ATTENDED_WITHOUT_ENCOUNTER': f"SELECT a.appointment_id k FROM {s}.appointments a LEFT JOIN {s}.encounters e USING(appointment_id) WHERE a.status='ATTENDED' GROUP BY a.appointment_id HAVING count(e.encounter_id)<>1",
        'OP_TO_IP': f"""SELECT e.encounter_id k FROM {s}.encounters e JOIN {s}.encounters p ON p.encounter_id=e.parent_encounter_id
          WHERE e.encounter_type<>'INPATIENT' OR p.encounter_type<>'OUTPATIENT' OR e.patient_id<>p.patient_id OR p.consultation_started_at IS NULL OR e.admitted_at IS NULL OR e.admitted_at<p.consultation_started_at""",
        'PARENT_UNIQUE': f'SELECT parent_encounter_id k FROM {s}.encounters WHERE parent_encounter_id IS NOT NULL GROUP BY 1 HAVING count(*)>1',
        'FUTURE_MILESTONE': f"SELECT encounter_id k FROM {s}.encounters WHERE GREATEST(checked_in_at,consultation_started_at,consultation_completed_at,admitted_at,discharged_at)>=%(as_of)s::timestamptz",
        'ENCOUNTER_TIME': f"""SELECT encounter_id k FROM {s}.encounters WHERE
          (consultation_started_at IS NOT NULL AND (checked_in_at IS NULL OR consultation_started_at<checked_in_at)) OR
          (consultation_completed_at IS NOT NULL AND (consultation_started_at IS NULL OR consultation_completed_at<consultation_started_at)) OR
          (discharged_at IS NOT NULL AND (admitted_at IS NULL OR discharged_at<=admitted_at))""",
        'NO_SHOW_GRACE': f"SELECT appointment_id k FROM {s}.appointments WHERE status='NO_SHOW' AND status_changed_at<scheduled_at+interval '30 minutes'",
        'RESCHEDULE_CHAIN': f"""SELECT a.appointment_id k FROM {s}.appointments a JOIN {s}.appointments p ON p.appointment_id=a.rescheduled_from_appointment_id
          WHERE p.status<>'RESCHEDULED' OR p.patient_id<>a.patient_id OR p.appointment_id>=a.appointment_id OR a.scheduled_at<p.scheduled_at""",
        'RESCHEDULE_SUCCESSOR': f"SELECT p.appointment_id k FROM {s}.appointments p LEFT JOIN {s}.appointments a ON a.rescheduled_from_appointment_id=p.appointment_id WHERE p.status='RESCHEDULED' GROUP BY 1 HAVING count(a.appointment_id)<>1",
        'BED_HISTORY_COVERAGE': f"""SELECT b.bed_id k FROM {s}.beds b LEFT JOIN {s}.bed_status_history h USING(bed_id)
          GROUP BY b.bed_id,b.commissioned_at HAVING min(h.effective_from) IS DISTINCT FROM b.commissioned_at OR count(*) FILTER(WHERE h.effective_to IS NULL)<>1""",
        'BED_HISTORY_CONTINUITY': f"""SELECT bed_id k FROM (SELECT *,lag(effective_to) OVER(PARTITION BY bed_id ORDER BY effective_from) previous,row_number() OVER(PARTITION BY bed_id ORDER BY effective_from) rn FROM {s}.bed_status_history) q WHERE rn>1 AND previous IS DISTINCT FROM effective_from""",
        'ASSIGNMENT_CONTAINMENT': f"""SELECT a.bed_assignment_id k FROM {s}.bed_assignments a JOIN {s}.encounters e USING(encounter_id)
          WHERE e.encounter_type<>'INPATIENT' OR e.admitted_at IS NULL OR a.assigned_at<e.admitted_at OR a.released_at<=a.assigned_at OR (e.discharged_at IS NOT NULL AND (a.released_at IS NULL OR a.released_at>e.discharged_at))""",
        'ASSIGNMENT_ACTIVE_COVERAGE': f"""SELECT a.bed_assignment_id k FROM {s}.bed_assignments a LEFT JOIN
          (SELECT bed_id,range_agg(tstzrange(effective_from,effective_to,'[)')) ranges FROM {s}.bed_status_history WHERE status='ACTIVE' GROUP BY bed_id) h USING(bed_id)
          WHERE h.ranges IS NULL OR NOT h.ranges @> tstzrange(a.assigned_at,a.released_at,'[)')""",
        'LAB_CONTEXT': f"""SELECT l.lab_order_id k FROM {s}.lab_orders l JOIN {s}.encounters e USING(encounter_id)
          WHERE l.department_id<>e.department_id OR COALESCE(e.admitted_at,e.consultation_started_at) IS NULL OR l.ordered_at<COALESCE(e.admitted_at,e.consultation_started_at) OR l.ordered_at>e.discharged_at""",
        'LAB_TIMELINE': f"""SELECT lab_order_id k FROM {s}.lab_orders WHERE collected_at<ordered_at OR processing_started_at<collected_at OR result_completed_at<processing_started_at OR GREATEST(ordered_at,collected_at,processing_started_at,result_completed_at)>=%(as_of)s::timestamptz""",
        'LAB_TARGET': f'SELECT lab_order_id k FROM {s}.lab_orders WHERE target_tat_minutes_at_order<=0',
        'PRIMARY_DOCTOR_MATCH': f"SELECT a.association_id k FROM {s}.encounter_doctors a JOIN {s}.encounters e USING(encounter_id) WHERE a.is_active AND a.role='PRIMARY' AND a.doctor_id IS DISTINCT FROM e.doctor_id",
        'DOCTOR_PARTICIPATION_TIME': f"""SELECT a.association_id k FROM {s}.encounter_doctors a JOIN {s}.encounters e USING(encounter_id) WHERE COALESCE(e.checked_in_at,e.admitted_at) IS NULL OR a.joined_at<COALESCE(e.checked_in_at,e.admitted_at) OR a.left_at<=a.joined_at OR
          (COALESCE(e.consultation_completed_at,e.discharged_at) IS NOT NULL AND (a.left_at IS NULL OR a.left_at>COALESCE(e.consultation_completed_at,e.discharged_at)))""",
        'DIAGNOSIS_TIME': f"""SELECT a.association_id k FROM {s}.encounter_diagnoses a JOIN {s}.encounters e USING(encounter_id) WHERE COALESCE(e.consultation_started_at,e.admitted_at) IS NULL OR a.recorded_at<COALESCE(e.consultation_started_at,e.admitted_at) OR a.recorded_at>=%(as_of)s::timestamptz""",
        'DIAGNOSIS_PRIMARY': f'SELECT encounter_id k FROM {s}.encounter_diagnoses WHERE is_active GROUP BY encounter_id HAVING count(*) FILTER(WHERE is_primary)<>1',
        'DIAGNOSIS_DUPLICATE': f'SELECT encounter_id k FROM {s}.encounter_diagnoses WHERE is_active GROUP BY encounter_id,diagnosis_id HAVING count(*)>1',
    }
    for group in ('bed_id','encounter_id'):
        checks[f'OVERLAP_{group}'] = f"SELECT {group} k FROM (SELECT {group},assigned_at,max(COALESCE(released_at,'infinity'::timestamptz)) OVER(PARTITION BY {group} ORDER BY assigned_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previous FROM {s}.bed_assignments) q WHERE assigned_at<previous"
    checks['PRIMARY_DOCTOR_OVERLAP'] = f"SELECT encounter_id k FROM (SELECT encounter_id,joined_at,max(COALESCE(left_at,'infinity'::timestamptz)) OVER(PARTITION BY encounter_id ORDER BY joined_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previous FROM {s}.encounter_doctors WHERE is_active AND role='PRIMARY') q WHERE joined_at<previous"
    for rule, query in checks.items():
        check(rule, query)
    if evidence is not None:
        evidence.update({'mode':'full_set_based_no_sampling','as_of':str(as_of),'schema':schema,
                         'check_count':len(checks_executed),'checks':checks_executed,
                         'failed_rows_across_checks':sum(c['failed_count'] for c in checks_executed)})
    return issues
