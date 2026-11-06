"""Deterministic synthetic snapshots. No network, credentials or patient PII."""
import argparse
import calendar
import hashlib
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

TABLE_KEYS = {
    'patients': 'patient_id', 'departments': 'department_id', 'doctors': 'doctor_id',
    'appointments': 'appointment_id', 'encounters': 'encounter_id', 'beds': 'bed_id',
    'bed_status_history': 'bed_status_history_id', 'bed_assignments': 'bed_assignment_id',
    'lab_test_types': 'lab_test_type_id', 'lab_orders': 'lab_order_id',
    'diagnoses': 'diagnosis_id', 'encounter_doctors': 'association_id',
    'encounter_diagnoses': 'association_id',
}
LOAD_ORDER = ['departments', 'patients', 'doctors', 'beds', 'bed_status_history',
              'lab_test_types', 'diagnoses', 'appointments', 'encounters', 'bed_assignments',
              'lab_orders', 'encounter_doctors', 'encounter_diagnoses']
CORE_TABLES = tuple(k for k in TABLE_KEYS if k not in ('diagnoses','encounter_doctors','encounter_diagnoses'))
VN = timezone(timedelta(hours=7))


def iso(value):
    return value.astimezone(timezone.utc).isoformat()


def moment(value):
    return datetime.fromisoformat(value)


def generate(scale='small', seed=20261022, as_of='2026-10-22T00:00:00+07:00'):
    rng = random.Random(seed)
    parsed_end = moment(as_of)
    if parsed_end.utcoffset() is None:
        raise ValueError('as_of requires an explicit timezone')
    end = parsed_end.astimezone(VN)
    start = end.replace(year=end.year - 1, day=min(end.day, calendar.monthrange(end.year-1, end.month)[1]))
    count = {'baseline': (10000, 50000, 50000, 4000, 32000),
             'small': (100, 500, 500, 40, 320)}[scale]
    np, na, no, ni, nl = count
    data = {name: [] for name in TABLE_KEYS}

    def add(table, created=None, updated=None, **row):
        created = created or start - timedelta(days=365*91)
        updated = updated or created
        row.update(created_at=iso(created), updated_at=iso(updated))
        data[table].append(row)
        return row

    for dept in range(1, 11):
        add('departments', department_id=dept, department_code=f'DEP-{dept:02}',
            department_name=f'Synthetic Department {dept:02}', is_active=True)
    for i in range(1, np+1):
        # Every patient is born before the earliest encounter.
        dob = (start - timedelta(days=rng.randint(1, 90*365))).date()
        add('patients', patient_id=i, patient_code=f'SYN-{i:06}', date_of_birth=str(dob),
            gender=rng.choice(['FEMALE', 'MALE', 'OTHER', 'UNKNOWN']), is_active=True)
    for i in range(1, 101):
        add('doctors', doctor_id=i, doctor_code=f'DOC-{i:03}', department_id=(i-1)//10+1, is_active=True)
    for i in range(1, 241):
        add('beds', bed_id=i, bed_code=f'BED-{i:03}', department_id=(i-1)//24+1,
            bed_type='ICU' if i % 12 == 0 else 'REGULAR', commissioned_at=iso(start))
        intervals = [(start, None, 'ACTIVE')]
        if i % 24 >= 23 or i % 24 == 0:
            mid = start + timedelta(days=180)
            intervals = [(start, mid, 'ACTIVE'), (mid, mid+timedelta(days=7), 'MAINTENANCE'),
                         (mid+timedelta(days=7), None, 'ACTIVE')]
        for left, right, state in intervals:
            add('bed_status_history', created=start, updated=right or left,
                bed_status_history_id=len(data['bed_status_history'])+1, bed_id=i,
                status=state, effective_from=iso(left), effective_to=iso(right) if right else None,
                reason='Synthetic schedule')
    targets = [30, 45, 60, 90, 120, 180, 240, 360]
    for i, target in enumerate(targets, 1):
        add('lab_test_types', lab_test_type_id=i, test_code=f'TEST-{i:02}',
            test_name=f'Synthetic Test {i:02}', default_tat_minutes=target, is_active=True)

    def slot():
        while True:
            day = start + timedelta(days=rng.randrange((end-start).days))
            if day.weekday() < 5 or rng.random() < .6:
                return day.replace(hour=rng.choices([8, 9, 10, 13, 15], [3, 3, 2, 1, 1])[0],
                                   minute=rng.randrange(60), second=0, microsecond=0)

    def department():
        return rng.choices(range(1, 11), weights=[3, 3, 2, 2, 1, 1, 1, 1, 1, 1])[0]

    pending = None
    for i in range(1, na+1):
        scheduled = slot()
        patient, dept = rng.randint(1, np), department()
        predecessor = None
        if pending:
            predecessor, patient, scheduled = pending
            pending = None
        state = rng.choices(['ATTENDED', 'NO_SHOW', 'CANCELLED', 'RESCHEDULED'], [76, 10, 7, 7])[0]
        if i == na or predecessor:
            state = 'ATTENDED'
        if scheduled > end-timedelta(hours=24) and state in ('NO_SHOW', 'RESCHEDULED'):
            state = 'SCHEDULED'
        if state == 'RESCHEDULED':
            # Successor remains before cutoff and keeps the same patient.
            successor = min(scheduled+timedelta(hours=2), end-timedelta(minutes=1))
            pending = (i, patient, successor)
        changed = scheduled + timedelta(hours=24) if state == 'NO_SHOW' else scheduled
        if state == 'SCHEDULED':
            changed = scheduled-timedelta(days=1)
        add('appointments', created=scheduled-timedelta(days=1), updated=changed,
            appointment_id=i, patient_id=patient, department_id=dept,
            doctor_id=(dept-1)*10+rng.randint(1, 10), scheduled_at=iso(scheduled), status=state,
            status_changed_at=iso(changed), rescheduled_from_appointment_id=predecessor)

    attended = [a for a in data['appointments'] if a['status'] == 'ATTENDED']
    for i in range(1, no+1):
        appointment = attended[i-1] if i <= len(attended) else None
        dept = appointment['department_id'] if appointment else department()
        checked = moment(appointment['scheduled_at']) if appointment else slot()
        began = checked + timedelta(minutes=min(180, max(1, int(rng.lognormvariate(3, .55)))))
        completed = began + timedelta(minutes=rng.randint(8, 30))
        state = 'COMPLETED' if completed < end else ('IN_CONSULTATION' if began < end else 'CHECKED_IN')
        began = began if began < end else None
        completed = completed if completed < end else None
        add('encounters', created=checked, updated=completed or began or checked,
            encounter_id=i, patient_id=appointment['patient_id'] if appointment else rng.randint(1, np),
            department_id=dept, doctor_id=appointment['doctor_id'] if appointment else (dept-1)*10+1,
            appointment_id=appointment['appointment_id'] if appointment else None, parent_encounter_id=None,
            encounter_type='OUTPATIENT', status=state, status_changed_at=iso(completed or began or checked),
            checked_in_at=iso(checked), consultation_started_at=iso(began) if began else None,
            consultation_completed_at=iso(completed) if completed else None,
            admitted_at=None, discharged_at=None)

    parents = sorted([e for e in data['encounters'] if e['consultation_started_at']],
                     key=lambda e: e['consultation_started_at'])
    admissions = []
    for i in range(ni):
        parent = parents[min(len(parents)-1, i*len(parents)//ni)] if i % 5 < 3 else None
        admitted = moment(parent['consultation_started_at'])+timedelta(hours=1) if parent else slot()
        if admitted >= end:
            admitted = end-timedelta(minutes=1)
        admissions.append((admitted, i, parent))
    pairs = [(dept, (dept-1)*24+k, (dept-1)*24+k+1) for dept in range(1, 11) for k in range(1, 22, 2)]
    available = [start]*len(pairs)
    for proposed, i, parent in sorted(admissions, key=lambda x: (x[0], x[1])):
        index = min(range(len(pairs)), key=lambda j: available[j])
        dept, bed_a, bed_b = pairs[index]
        admitted = max(proposed, available[index])
        if admitted >= end:
            raise ValueError('Capacity exhausted; reduce admissions or LOS, never overbook')
        discharge = admitted+timedelta(hours=rng.randint(12, 180))
        closed = discharge < end
        available[index] = discharge+timedelta(minutes=30)
        encounter_id = no+i+1
        add('encounters', created=admitted, updated=discharge if closed else admitted,
            encounter_id=encounter_id, patient_id=parent['patient_id'] if parent else rng.randint(1, np),
            department_id=dept, doctor_id=(dept-1)*10+1, appointment_id=None,
            parent_encounter_id=parent['encounter_id'] if parent else None,
            encounter_type='INPATIENT', status='DISCHARGED' if closed else 'ADMITTED',
            status_changed_at=iso(discharge if closed else admitted),
            checked_in_at=None, consultation_started_at=None, consultation_completed_at=None,
            admitted_at=iso(admitted), discharged_at=iso(discharge) if closed else None)
        split = admitted+(discharge-admitted)/2
        segments = [(bed_a, admitted, discharge if closed else None)]
        if i % 5 == 0 and split < end:
            segments = [(bed_a, admitted, split), (bed_b, split, discharge if closed else None)]
        for bed, left, right in segments:
            add('bed_assignments', created=left, updated=right or left,
                bed_assignment_id=len(data['bed_assignments'])+1, bed_id=bed, encounter_id=encounter_id,
                assigned_at=iso(left), released_at=iso(right) if right else None)
    data['encounters'].sort(key=lambda e: e['encounter_id'])

    contexts = [e for e in data['encounters'] if e['admitted_at'] or e['consultation_started_at']]
    for i in range(1, nl+1):
        e = rng.choice(contexts)
        ordered = moment(e['admitted_at'] or e['consultation_started_at'])+timedelta(minutes=2)
        if ordered >= end:
            ordered = moment(e['admitted_at'] or e['consultation_started_at'])
        test = rng.randint(1, 8)
        target = targets[test-1]
        collected = ordered+timedelta(minutes=3)
        processing = collected+timedelta(minutes=2)
        result = ordered+timedelta(minutes=max(6, int(target*rng.uniform(.3, 1.3))))
        cancelled = i % 20 == 0
        state = 'CANCELLED' if cancelled else ('COMPLETED' if result < end else
                'PROCESSING' if processing < end else 'COLLECTED' if collected < end else 'ORDERED')
        c = collected if collected < end and not cancelled else None
        p = processing if processing < end and not cancelled else None
        r = result if state == 'COMPLETED' else None
        changed = r or p or c or ordered
        add('lab_orders', created=ordered, updated=changed, lab_order_id=i, encounter_id=e['encounter_id'],
            department_id=e['department_id'], lab_test_type_id=test, status=state,
            status_changed_at=iso(changed), ordered_at=iso(ordered), collected_at=iso(c) if c else None,
            processing_started_at=iso(p) if p else None, result_completed_at=iso(r) if r else None,
            target_tat_minutes_at_order=target)

    for i in range(1, 101):
        add('diagnoses', diagnosis_id=i, diagnosis_code=f'SYN-DX-{i:03}',
            diagnosis_name=f'Synthetic diagnosis {i:03}', is_active=True)
    for e in data['encounters']:
        began = e['consultation_started_at'] or e['admitted_at']
        if not began:
            continue
        left = e['consultation_completed_at'] or e['discharged_at']
        for j in range(2 if e['encounter_id'] % 5 == 1 else 1):
            add('encounter_doctors', created=moment(began), updated=moment(left or began),
                association_id=len(data['encounter_doctors'])+1, encounter_id=e['encounter_id'],
                doctor_id=e['doctor_id'] if j == 0 else e['doctor_id'] % 100+1,
                role='PRIMARY' if j == 0 else 'CONSULTING', joined_at=began, left_at=left, is_active=True)
        for j in range(3 if e['encounter_id'] % 5 == 1 else 1):
            add('encounter_diagnoses', created=moment(began), association_id=len(data['encounter_diagnoses'])+1,
                encounter_id=e['encounter_id'], diagnosis_id=(e['encounter_id']+j) % 100+1,
                is_primary=j == 0, recorded_at=began, is_active=True)
    manifest = {'generator_version': 2, 'seed': seed, 'scale': scale,
                'history_start': iso(start), 'as_of': iso(end), 'history_months': 12,
                'counts': {k: len(v) for k, v in data.items()}, 'synthetic_only': True}
    canonical = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
    manifest['business_sha256'] = hashlib.sha256(canonical).hexdigest()
    return {'manifest': manifest, 'tables': data}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale', choices=['small', 'baseline'], default='small')
    parser.add_argument('--seed', type=int, default=20261022)
    parser.add_argument('--as-of', default='2026-10-22T00:00:00+07:00')
    parser.add_argument('--output', type=Path, default=Path('artifacts/generated/historical.json'))
    args = parser.parse_args()
    result = generate(args.scale, args.seed, args.as_of)
    from data_quality.source_checks import validate
    issues = validate(result)
    if issues:
        raise RuntimeError(f'Clean generation rejected: {issues[:10]}')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(',', ':')), encoding='utf-8')
    print(json.dumps(result['manifest'], indent=2))


if __name__ == '__main__':
    main()
