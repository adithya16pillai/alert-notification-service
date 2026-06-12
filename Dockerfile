# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for layer caching.
COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && pip install .

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

# Non-root runtime user.
RUN useradd --uid 10001 --no-create-home appuser
USER appuser

EXPOSE 8000

# Default command runs the API; override to `ans-worker` for the dispatcher.
CMD ["ans-api"]
