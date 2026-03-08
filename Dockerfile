# ── Football Fam Data Pipeline ────────────────────────────────────────────
# Multi-stage build: slim Python image with only runtime deps.
#
#   docker build -t football-fam-data .
#   docker run --env-file .env football-fam-data python scripts/run_all.py
# ──────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps needed by psycopg2-binary and lxml
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev gcc curl && \
    rm -rf /var/lib/apt/lists/*

# ── Dependencies ─────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ─────────────────────────────────────────────────────
COPY . .

# Ensure data/logs directory exists for pipeline output
RUN mkdir -p data/logs

# ── Default: run the API server ──────────────────────────────────────────
# Override CMD for pipeline runs:
#   docker run football-fam-data python scripts/run_all.py
EXPOSE 8000
CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
