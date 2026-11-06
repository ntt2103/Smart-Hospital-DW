"""PostgreSQL rejection tests; all attempted writes rolled back."""
import json
from pathlib import Path
from operations.foundation import connect, ROOT
from psycopg import errors


def run():
    results = []
    with connect('oltp', 'admin') as conn:
        cases = [
            ('fk_orphan', "UPDATE public.lab_orders SET encounter_id=9223372036854770000 WHERE lab_order_id=1", errors.ForeignKeyViolation),
            ('invalid_status', "UPDATE public.appointments SET status='INVALID' WHERE appointment_id=1", errors.CheckViolation),
            ('negative_wait', "UPDATE public.encounters SET consultation_started_at=checked_in_at-interval '1 minute' WHERE encounter_id=1", errors.CheckViolation),
            ('bed_overlap', "INSERT INTO public.bed_assignments(bed_id,encounter_id,assigned_at,released_at) SELECT bed_id,encounter_id,assigned_at,released_at FROM public.bed_assignments LIMIT 1", errors.ExclusionViolation),
            ('lab_target_frozen', "UPDATE public.lab_orders SET target_tat_minutes_at_order=target_tat_minutes_at_order+1 WHERE lab_order_id=1", errors.RaiseException),
        ]
        for name, statement, expected in cases:
            try:
                with conn.transaction():
                    conn.execute(statement)
            except expected:
                results.append({'name': name, 'passed': True})
            else:
                raise AssertionError(f'{name} was accepted')
    output = ROOT/'artifacts/phase2/source_database_tests.json'
    output.write_text(json.dumps({'passed':len(results),'failed':0,'checks':results}, indent=2), encoding='utf-8')
    print(json.dumps({'source_database_tests_passed':len(results)}))


if __name__ == '__main__':
    run()
