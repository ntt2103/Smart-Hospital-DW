-- Phase 1 design only. Execute against an isolated DWH database in Phase 2.
-- UUIDs are supplied by the caller; no extension or implicit live deployment.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.pipeline_runs (
    run_id UUID PRIMARY KEY,
    parent_run_id UUID REFERENCES ops.pipeline_runs(run_id),
    pipeline_name TEXT NOT NULL,
    run_kind TEXT NOT NULL CHECK (run_kind IN ('BATCH','STREAM_REFRESH','BACKFILL','RECONCILIATION')),
    status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    input_bounds JSONB NOT NULL DEFAULT '{}'::jsonb,
    extracted_count BIGINT NOT NULL DEFAULT 0 CHECK (extracted_count >= 0),
    raw_count BIGINT NOT NULL DEFAULT 0 CHECK (raw_count >= 0),
    staged_count BIGINT NOT NULL DEFAULT 0 CHECK (staged_count >= 0),
    upserted_count BIGINT NOT NULL DEFAULT 0 CHECK (upserted_count >= 0),
    quarantined_count BIGINT NOT NULL DEFAULT 0 CHECK (quarantined_count >= 0),
    error_code TEXT,
    error_message TEXT,
    CHECK ((status='RUNNING' AND finished_at IS NULL) OR (status<>'RUNNING' AND finished_at IS NOT NULL)),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE ops.watermarks (
    pipeline_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    high_water_updated_at TIMESTAMPTZ,
    high_water_pk BIGINT,
    last_run_id UUID REFERENCES ops.pipeline_runs(run_id),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_name,source_name),
    CHECK ((high_water_updated_at IS NULL) = (high_water_pk IS NULL)),
    CHECK (high_water_pk IS NULL OR high_water_pk > 0)
);

CREATE TABLE ops.dq_results (
    dq_result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ops.pipeline_runs(run_id),
    rule_id TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('WARN','ERROR')),
    result TEXT NOT NULL CHECK (result IN ('PASS','WARN','FAIL')),
    checked_count BIGINT NOT NULL CHECK (checked_count >= 0),
    failed_count BIGINT NOT NULL CHECK (failed_count >= 0 AND failed_count <= checked_count),
    sample_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    query_version TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id,rule_id,scope_key)
);

CREATE TABLE ops.quarantined_records (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID REFERENCES ops.pipeline_runs(run_id),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('BATCH','KAFKA','TRANSFORM')),
    source_name TEXT NOT NULL,
    source_key TEXT,
    rule_id TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('WARN','ERROR')),
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    resolution TEXT,
    CHECK (resolved_at IS NULL OR resolved_at >= quarantined_at)
);

CREATE TABLE ops.reconciliation_results (
    reconciliation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ops.pipeline_runs(run_id),
    scope_key TEXT NOT NULL,
    source_name TEXT NOT NULL,
    target_name TEXT NOT NULL,
    extraction_cut TIMESTAMPTZ NOT NULL,
    source_count BIGINT NOT NULL CHECK (source_count >= 0),
    target_count BIGINT NOT NULL CHECK (target_count >= 0),
    missing_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    divergent_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    result TEXT NOT NULL CHECK (result IN ('PASS','WARN','FAIL')),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id,scope_key,source_name,target_name)
);

CREATE TABLE ops.mart_refreshes (
    refresh_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ops.pipeline_runs(run_id),
    mart_name TEXT NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    input_cutoff TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    CHECK (published_at >= as_of),
    UNIQUE (mart_name,as_of)
);
