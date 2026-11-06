"""Cross-table guards not covered by source FK/CHECK/EXCLUDE constraints."""
from collections import defaultdict
from datetime import timedelta
from data_sources.generator.historical import TABLE_KEYS, moment

# Imported here to preserve the public DQ API used by batch and seed commands.
from data_quality.database_checks import validate_database


def validate(snapshot):
    tables = snapshot['tables']
    cutoff = moment(snapshot['manifest']['as_of'])
    errors = []

    def fail(rule, key):
        errors.append({'rule': rule, 'key': key})

    index = {}
    for name, pk in TABLE_KEYS.items():
        rows = tables[name]
        index[name] = {r[pk]: r for r in rows}
        if len(index[name]) != len(rows) or any(not isinstance(r[pk], int) or r[pk] <= 0 for r in rows):
            fail('PK', name)
        for r in rows:
            if moment(r['created_at']) > moment(r['updated_at']):
                fail('AUDIT_TIME', f'{name}:{r[pk]}')
    refs = {
        'doctors': [('department_id', 'departments')],
        'appointments': [('patient_id', 'patients'), ('department_id', 'departments'), ('doctor_id', 'doctors'),
                         ('rescheduled_from_appointment_id', 'appointments')],
        'encounters': [('patient_id', 'patients'), ('department_id', 'departments'), ('doctor_id', 'doctors'),
                       ('appointment_id', 'appointments'), ('parent_encounter_id', 'encounters')],
        'beds': [('department_id', 'departments')], 'bed_status_history': [('bed_id', 'beds')],
        'bed_assignments': [('bed_id', 'beds'), ('encounter_id', 'encounters')],
        'lab_orders': [('encounter_id', 'encounters'), ('department_id', 'departments'), ('lab_test_type_id', 'lab_test_types')],
        'encounter_doctors': [('encounter_id', 'encounters'), ('doctor_id', 'doctors')],
        'encounter_diagnoses': [('encounter_id', 'encounters'), ('diagnosis_id', 'diagnoses')],
    }
    for table, keys in refs.items():
        for row in tables[table]:
            for field, target in keys:
                if row[field] is not None and row[field] not in index[target]:
                    fail('FK', f'{table}:{row[TABLE_KEYS[table]]}:{field}')
    if errors:
        return errors  # Avoid masking broken references with a Python KeyError.
    appointment_encounters = defaultdict(list)
    parent_children = defaultdict(list)
    for e in tables['encounters']:
        key = e['encounter_id']
        if e['appointment_id']:
            a = index['appointments'][e['appointment_id']]
            appointment_encounters[e['appointment_id']].append(e)
            if e['encounter_type'] != 'OUTPATIENT' or a['status'] != 'ATTENDED' or any(
                    e[k] != a[k] for k in ('patient_id', 'department_id')):
                fail('ATTENDANCE_LINK', key)
        if e['parent_encounter_id']:
            p = index['encounters'][e['parent_encounter_id']]
            parent_children[e['parent_encounter_id']].append(e)
            if e['encounter_type'] != 'INPATIENT' or p['encounter_type'] != 'OUTPATIENT' or p['patient_id'] != e['patient_id'] or not p['consultation_started_at'] or not e['admitted_at'] or moment(e['admitted_at']) < moment(p['consultation_started_at']):
                fail('OP_TO_IP', key)
        for left, right in [('checked_in_at', 'consultation_started_at'),
                            ('consultation_started_at', 'consultation_completed_at'), ('admitted_at', 'discharged_at')]:
            if e[right] and (not e[left] or moment(e[left]) > moment(e[right])):
                fail('ENCOUNTER_TIME', key)
        for field in ('checked_in_at', 'consultation_started_at', 'consultation_completed_at', 'admitted_at', 'discharged_at'):
            if e[field] and moment(e[field]) >= cutoff:
                fail('FUTURE_MILESTONE', key)
    if any(len(v) > 1 for v in parent_children.values()):
        fail('PARENT_UNIQUE', 'encounters')
    successors = defaultdict(list)
    for a in tables['appointments']:
        k = a['appointment_id']
        if a['status'] not in ('SCHEDULED', 'ATTENDED', 'NO_SHOW', 'CANCELLED', 'RESCHEDULED'):
            fail('APPOINTMENT_STATUS', k)
        if a['status'] == 'ATTENDED' and len(appointment_encounters[k]) != 1:
            fail('ATTENDED_WITHOUT_ENCOUNTER', k)
        if a['status'] == 'NO_SHOW' and moment(a['status_changed_at']) < moment(a['scheduled_at'])+timedelta(minutes=30):
            fail('NO_SHOW_GRACE', k)
        predecessor = a['rescheduled_from_appointment_id']
        if predecessor:
            p = index['appointments'][predecessor]
            successors[predecessor].append(k)
            if p['status'] != 'RESCHEDULED' or p['patient_id'] != a['patient_id'] or predecessor >= k:
                fail('RESCHEDULE_CHAIN', k)
    for a in tables['appointments']:
        if (a['status'] == 'RESCHEDULED' and len(successors[a['appointment_id']]) != 1):
            fail('RESCHEDULE_SUCCESSOR', a['appointment_id'])
    histories = defaultdict(list)
    for h in tables['bed_status_history']:
        histories[h['bed_id']].append(h)
    infinity = cutoff+timedelta(days=36500)
    for bed in tables['beds']:
        rows = sorted(histories[bed['bed_id']], key=lambda r: r['effective_from'])
        if not rows or rows[0]['effective_from'] != bed['commissioned_at'] or rows[-1]['effective_to'] is not None:
            fail('BED_HISTORY_COVERAGE', bed['bed_id'])
        for i, h in enumerate(rows):
            if h['status'] not in ('ACTIVE', 'MAINTENANCE', 'INACTIVE') or (h['effective_to'] and moment(h['effective_to']) <= moment(h['effective_from'])):
                fail('BED_STATUS', h['bed_status_history_id'])
            if i and rows[i-1]['effective_to'] != h['effective_from']:
                fail('BED_HISTORY_CONTINUITY', bed['bed_id'])
    grouped = {'bed_id': defaultdict(list), 'encounter_id': defaultdict(list)}
    for a in tables['bed_assignments']:
        e = index['encounters'][a['encounter_id']]
        left, right = moment(a['assigned_at']), moment(a['released_at']) if a['released_at'] else infinity
        if e['encounter_type'] != 'INPATIENT' or not e['admitted_at'] or left < moment(e['admitted_at']) or right <= left or (e['discharged_at'] and (not a['released_at'] or right > moment(e['discharged_at']))):
            fail('ASSIGNMENT_CONTAINMENT', a['bed_assignment_id'])
        cursor = left
        for h in sorted(histories[a['bed_id']], key=lambda r: r['effective_from']):
            hleft = moment(h['effective_from'])
            hright = moment(h['effective_to']) if h['effective_to'] else infinity
            if hright <= cursor or hleft >= right:
                continue
            if hleft > cursor or h['status'] != 'ACTIVE':
                break
            cursor = max(cursor, hright)
            if cursor >= right:
                break
        if cursor < right:
            fail('ASSIGNMENT_ACTIVE_COVERAGE', a['bed_assignment_id'])
        for group, values in grouped.items():
            values[a[group]].append((left, right))
    for group, values in grouped.items():
        for key, spans in values.items():
            last_end = None
            for left, right in sorted(spans):
                if last_end and left < last_end:
                    fail(f'OVERLAP_{group}', key)
                last_end = max(last_end or right, right)
    for l in tables['lab_orders']:
        e = index['encounters'][l['encounter_id']]
        start = e['admitted_at'] or e['consultation_started_at']
        if l['department_id'] != e['department_id'] or not start or moment(l['ordered_at']) < moment(start) or (e['discharged_at'] and moment(l['ordered_at']) > moment(e['discharged_at'])):
            fail('LAB_CONTEXT', l['lab_order_id'])
        previous = moment(l['ordered_at'])
        for field in ('collected_at', 'processing_started_at', 'result_completed_at'):
            if l[field]:
                current = moment(l[field])
                if current < previous or current >= cutoff:
                    fail('LAB_TIMELINE', l['lab_order_id'])
                previous = current
        if l['target_tat_minutes_at_order'] <= 0:
            fail('LAB_TARGET', l['lab_order_id'])
        allowed = {'ORDERED': (False, False, False), 'COLLECTED': (True, False, False),
                   'PROCESSING': (True, True, False), 'COMPLETED': (True, True, True)}
        actual = tuple(bool(l[f]) for f in ('collected_at', 'processing_started_at', 'result_completed_at'))
        if l['status'] == 'CANCELLED':
            if actual[2]:
                fail('LAB_STATE', l['lab_order_id'])
        elif allowed.get(l['status']) != actual:
            fail('LAB_STATE', l['lab_order_id'])
    primary_doctors = defaultdict(list)
    primary_diagnoses = defaultdict(list)
    active_diagnoses = defaultdict(set)
    for a in tables['encounter_doctors']:
        e = index['encounters'][a['encounter_id']]
        started = e['checked_in_at'] or e['admitted_at']
        ended = e['consultation_completed_at'] or e['discharged_at']
        if (a['role'] not in ('PRIMARY', 'CONSULTING') or not started or moment(a['joined_at']) < moment(started)
            or (a['left_at'] and moment(a['left_at']) <= moment(a['joined_at']))
            or (ended and (not a['left_at'] or moment(a['left_at']) > moment(ended)))):
            fail('DOCTOR_PARTICIPATION_TIME', a['association_id'])
        if a['is_active'] and a['role'] == 'PRIMARY':
            if a['doctor_id'] != e['doctor_id']:
                fail('PRIMARY_DOCTOR_MATCH', a['association_id'])
            primary_doctors[a['encounter_id']].append((moment(a['joined_at']), moment(a['left_at']) if a['left_at'] else infinity))
    for key, spans in primary_doctors.items():
        previous = None
        for left, right in sorted(spans):
            if previous and left < previous:
                fail('PRIMARY_DOCTOR_OVERLAP', key)
            previous = max(previous or right, right)
    for a in tables['encounter_diagnoses']:
        e = index['encounters'][a['encounter_id']]
        started = e['consultation_started_at'] or e['admitted_at']
        if not started or moment(a['recorded_at']) < moment(started) or moment(a['recorded_at']) >= cutoff:
            fail('DIAGNOSIS_TIME', a['association_id'])
        if a['is_active']:
            if a['diagnosis_id'] in active_diagnoses[a['encounter_id']]:
                fail('DIAGNOSIS_DUPLICATE', a['association_id'])
            active_diagnoses[a['encounter_id']].add(a['diagnosis_id'])
            if a['is_primary']:
                primary_diagnoses[a['encounter_id']].append(a)
    for key in active_diagnoses:
        if len(primary_diagnoses[key]) != 1:
            fail('DIAGNOSIS_PRIMARY', key)
    return errors
