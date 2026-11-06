-- OLTP: first-visit registration is held separately until reception check-in.
BEGIN;
ALTER TABLE public.patients ALTER COLUMN date_of_birth DROP NOT NULL;
CREATE TABLE public.booking_registrations (
 request_id UUID PRIMARY KEY,
 request JSONB NOT NULL,
 patient_id BIGINT REFERENCES public.patients(patient_id),
 appointment_id BIGINT UNIQUE REFERENCES public.appointments(appointment_id),
 encounter_id BIGINT UNIQUE REFERENCES public.encounters(encounter_id),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE public.booking_event_outbox (
 event_id UUID PRIMARY KEY, event_body JSONB NOT NULL,
 published BOOLEAN NOT NULL DEFAULT FALSE
);
GRANT SELECT ON public.booking_registrations,public.booking_event_outbox TO hospital_reader;
GRANT SELECT,INSERT,UPDATE ON public.booking_registrations,public.booking_event_outbox TO hospital_writer;
COMMIT;
