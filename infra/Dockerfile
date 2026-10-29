FROM python:3.14-slim@sha256:35caba83af2fe6da9a5d922f9b0156f21d1246a48278c58a08bd253303ba42e8 AS dependencies

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
RUN useradd --create-home --uid 10001 workflow
COPY requirements.lock /app/
RUN pip install --no-cache-dir -r requirements.lock

FROM dependencies AS runtime
COPY --chown=workflow:workflow api /app/api
COPY --chown=workflow:workflow orchestrator /app/orchestrator
COPY --chown=workflow:workflow worker /app/worker
COPY --chown=workflow:workflow shared /app/shared
USER workflow
EXPOSE 8000
CMD ["python", "-m", "api"]

FROM runtime AS test
USER root
RUN apt-get update && apt-get install --yes --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*
ENV PYTEST_ADDOPTS="-o cache_dir=/tmp/pytest-cache" \
    RUFF_CACHE_DIR=/tmp/ruff-cache \
    MYPY_CACHE_DIR=/tmp/mypy-cache
COPY requirements-dev.lock pyproject.toml /app/
RUN pip install --no-cache-dir -r requirements-dev.lock
COPY --chown=workflow:workflow tests /app/tests
COPY --chown=workflow:workflow db /app/db
USER workflow
CMD ["pytest", "-q", "tests/unit", "tests/component", "tests/integration"]
