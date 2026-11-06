#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=af_password="$AIRFLOW_META_PASSWORD" --set=mb_password="$METABASE_META_PASSWORD" <<'SQL'
CREATE ROLE airflow_meta_owner LOGIN PASSWORD :'af_password';
CREATE ROLE metabase_meta_owner LOGIN PASSWORD :'mb_password';
CREATE DATABASE airflow_meta OWNER airflow_meta_owner;
CREATE DATABASE metabase_app OWNER metabase_meta_owner;
REVOKE ALL ON DATABASE airflow_meta FROM PUBLIC;
REVOKE ALL ON DATABASE metabase_app FROM PUBLIC;
SQL
