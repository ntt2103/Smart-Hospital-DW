CREATE OR REPLACE FUNCTION dw.local_date_key(t TIMESTAMPTZ) RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(to_char(t AT TIME ZONE 'Asia/Ho_Chi_Minh','YYYYMMDD')::INTEGER,0) $$;
CREATE VIEW mart.departments AS SELECT department_sk,department_id,department_code,department_name FROM dw.dim_department;
CREATE VIEW mart.pipeline_health AS
SELECT pipeline_name,max(finished_at) FILTER(WHERE status='SUCCEEDED') last_success_at,
count(*) FILTER(WHERE status='FAILED') failed_runs FROM ops.pipeline_runs GROUP BY pipeline_name;
CREATE OR REPLACE FUNCTION dw.merge_current(target_table REGCLASS,source_table REGCLASS,keys TEXT[]) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE fields TEXT; assignments TEXT; key_list TEXT;
BEGIN
 SELECT string_agg(format('%I',attname),',' ORDER BY attnum),
 string_agg(format('%I=EXCLUDED.%I',attname,attname),',' ORDER BY attnum) FILTER(WHERE NOT attname=ANY(keys))
 INTO fields,assignments FROM pg_attribute WHERE attrelid=source_table AND attnum>0 AND NOT attisdropped;
 SELECT string_agg(format('%I',k),',') INTO key_list FROM unnest(keys) k;
 EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s',target_table,fields,fields,source_table,key_list,assignments);
END $$;
