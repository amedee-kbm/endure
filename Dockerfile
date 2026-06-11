FROM python:3.12-slim AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=endure.settings
ENV PATH="/app/.venv/bin:$PATH"

# helps with bind mounts + dev ergonomics
ENV PYTHONDONTWRITEBYTECODE=1

# Evaluation runner: dev dependencies (pytest etc.) on top of base.
# git lets helpers.git_commit() record the real hash in result metadata;
# safe.directory is needed because /app is a bind mount owned by the host.
FROM base AS runner
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system --add safe.directory /app
RUN uv sync --frozen