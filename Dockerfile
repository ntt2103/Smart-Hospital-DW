ARG PYTHON_IMAGE=python:3.12.15-slim-bookworm
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt && pip check
ENV PYTHONPATH=/app/src:/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY src ./src
COPY sql ./sql
COPY tests ./tests
CMD ["python", "-m", "operations.bootstrap", "database"]
