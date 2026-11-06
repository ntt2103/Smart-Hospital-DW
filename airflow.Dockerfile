ARG AIRFLOW_IMAGE=apache/airflow:3.1.8-python3.12
FROM ${AIRFLOW_IMAGE}
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "psycopg[binary]==3.2.3" && pip check
ENV PYTHONPATH=/app/src:/app PYTHONDONTWRITEBYTECODE=1
