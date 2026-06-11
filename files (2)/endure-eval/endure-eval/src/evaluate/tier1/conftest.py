"""
Tier 1 conftest — deterministic, in-process evaluation against real PostgreSQL.

Requires ONLY the postgres service:  docker compose up -d postgres
(or run inside the runner container, where ENDURE_DATABASE_HOST=postgres).

WARNING: tests truncate Endure tables between cases. Point them at a scratch
database (ENDURE_DATABASE_NAME), never at one whose contents you care about.
"""

from __future__ import annotations

import os

# Must precede django.setup(). Defaults match docker-compose.yml's postgres
# service; override via environment when running inside the compose network.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "endure.settings")
os.environ.setdefault("ENDURE_DATABASE_NAME", "endure")
os.environ.setdefault("ENDURE_DATABASE_USER", "endure")
os.environ.setdefault("ENDURE_DATABASE_PASSWORD", "endure")
os.environ.setdefault("ENDURE_DATABASE_HOST", "localhost")
os.environ.setdefault("ENDURE_DATABASE_PORT", "5432")

import django  # noqa: E402

django.setup()

import pytest  # noqa: E402

from src.models import (  # noqa: E402
    Checkpoint,
    DeadLetterJob,
    Job,
    JobEvent,
    SchedulerLeader,
    SourceFile,
    StepOutput,
    Tenant,
    Worker,
)


@pytest.fixture(autouse=True)
async def clean_db():
    """Truncate all mutable Endure tables before each test (FK-safe order)."""
    for model in (
        JobEvent,
        StepOutput,
        Checkpoint,
        DeadLetterJob,
        SourceFile,
        Job,
        Worker,
        SchedulerLeader,
    ):
        await model.objects.all().adelete()
    yield


@pytest.fixture
async def tenant():
    t, _ = await Tenant.objects.aget_or_create(name="tier1-tenant")
    return t


@pytest.fixture
async def make_job(tenant):
    """Factory for Job rows in arbitrary states (state injection)."""

    async def _make(**overrides):
        from src.constants import JobState

        fields = dict(
            tenant=tenant,
            name="tier1-job",
            job_type="src.evaluate.tier1.jobs:GateJob",
            payload={},
            state=JobState.RUNNING,
            attempt=1,
            max_retries=3,
            timeout_seconds=300,
        )
        fields.update(overrides)
        return await Job.objects.acreate(**fields)

    return _make
