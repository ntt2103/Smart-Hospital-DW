-- DWH: align the fact guard with the live reception no-show grace period.
BEGIN;
DO $$
DECLARE item RECORD;
BEGIN
 FOR item IN SELECT conname FROM pg_constraint
   WHERE conrelid='dw.fact_appointment'::regclass AND contype='c'
     AND pg_get_constraintdef(oid) LIKE '%is_noshow_eligible%'
 LOOP
   EXECUTE format('ALTER TABLE dw.fact_appointment DROP CONSTRAINT %I',item.conname);
 END LOOP;
END $$;
ALTER TABLE dw.fact_appointment ADD CONSTRAINT appointment_noshow_30m CHECK (
 NOT is_noshow_eligible OR
 (scheduled_at IS NOT NULL AND scheduled_date_key<>0 AND scheduled_at<=eligibility_as_of
 AND status IN ('ATTENDED','NO_SHOW') AND status_changed_at IS NOT NULL
 AND status_changed_at<=eligibility_as_of
 AND (status<>'NO_SHOW' OR status_changed_at>=scheduled_at+interval '30 minutes'))
);
COMMIT;
