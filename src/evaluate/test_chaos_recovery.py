"""RQ2: fault tolerance — worker crashes and scheduler failover.

Requires a running Docker Compose stack and ENDURE_CHAOS=1.

Override container names if the defaults don't match your environment:
  ENDURE_WORKER_CONTAINER    (default: endure-worker-1)
  ENDURE_SCHEDULER_CONTAINER (default: endure-scheduler)
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time

import pytest

from src.evaluate.helpers import (
    SYNTHETIC_JOB,
    create_tenant,
    submit_job,
    unique_name,
    wait_for_jobs,
    wait_for_running_count,
)

pytestmark = [pytest.mark.e2e, pytest.mark.chaos, pytest.mark.evaluate, pytest.mark.asyncio]

WORKER_CONTAINER = os.environ.get("ENDURE_WORKER_CONTAINER", "endure-worker-1")
SCHEDULER_CONTAINER = os.environ.get("ENDURE_SCHEDULER_CONTAINER", "endure-scheduler")


def _chaos_enabled() -> bool:
    return os.environ.get("ENDURE_CHAOS", "").strip() in ("1", "true", "yes")


def _docker_stop(name: str) -> None:
    subprocess.run(["docker", "stop", name], check=True, capture_output=True, timeout=30)


def _docker_start(name: str) -> None:
    subprocess.run(["docker", "start", name], check=True, capture_output=True, timeout=30)


@pytest.fixture(autouse=True)
def require_chaos():
    if not _chaos_enabled():
        pytest.skip("Set ENDURE_CHAOS=1 to run container fault-injection tests")


async def test_worker_crash_jobs_still_complete(client):
    tenant_id = await create_tenant(client, name=unique_name("chaos-worker"))
    job_ids = [
        await submit_job(
            client,
            tenant_id=tenant_id,
            job_type=SYNTHETIC_JOB,
            payload={"stage_duration": 0.4, "stages": 5},
            max_retries=3,
            name=unique_name("crash-job"),
        )
        for _ in range(5)
    ]

    await wait_for_running_count(client, job_ids, min_running=3, timeout=30.0)
    _docker_stop(WORKER_CONTAINER)
    try:
        final = await wait_for_jobs(client, job_ids, timeout=180.0)
        completed = sum(1 for j in final if j["state"] == "COMPLETED")
        assert completed == len(job_ids), (
            f"Only {completed}/{len(job_ids)} jobs completed after worker crash"
        )
    finally:
        _docker_start(WORKER_CONTAINER)
        await asyncio.sleep(5)


async def test_scheduler_failover_jobs_complete(client):
    tenant_id = await create_tenant(client, name=unique_name("chaos-failover"))
    job_ids: list[str] = []

    # Stagger submission so some are RUNNING before we kill the scheduler
    for i in range(4):
        job_ids.append(
            await submit_job(
                client,
                tenant_id=tenant_id,
                job_type=SYNTHETIC_JOB,
                payload={"stage_duration": 0.4, "stages": 5},
                max_retries=3,
                name=unique_name(f"failover-{i}"),
            )
        )
        await asyncio.sleep(0.5)

    await wait_for_running_count(client, job_ids, min_running=2, timeout=30.0)

    for i in range(4, 8):
        job_ids.append(
            await submit_job(
                client,
                tenant_id=tenant_id,
                job_type=SYNTHETIC_JOB,
                payload={"stage_duration": 0.4, "stages": 5},
                max_retries=3,
                name=unique_name(f"failover-{i}"),
            )
        )
        await asyncio.sleep(0.5)

    leader_before = (await client.get("/api/v1/admin/leader")).json().get("leader")
    holder_before = (leader_before or {}).get("holder_id")
    # holder_id equals the container name when ENDURE_SCHEDULER_INSTANCE_ID is set
    leader_container = holder_before or SCHEDULER_CONTAINER
    killed_at = time.time()
    _docker_stop(leader_container)

    try:
        # Lease TTL=15 s, standby loop=0.1 s → failover bound ≤ 16 s.
        # Allow 20 s to absorb scheduling jitter.
        deadline = killed_at + 20.0
        new_leader = None
        elapsed = 0.0
        while time.time() < deadline:
            info = (await client.get("/api/v1/admin/leader")).json().get("leader")
            if info and info.get("holder_id") != holder_before:
                elapsed = time.time() - killed_at
                new_leader = info
                break
            await asyncio.sleep(1.0)
        assert new_leader is not None, (
            f"Standby scheduler did not acquire leadership within 20 s "
            f"(TTL=15 s + 0.1 s loop bound, killed {leader_container!r})"
        )

        final = await wait_for_jobs(client, job_ids, timeout=120.0)
        assert all(j["state"] == "COMPLETED" for j in final), (
            f"States after failover: {[j['state'] for j in final]}"
        )
    finally:
        _docker_start(leader_container)
        await asyncio.sleep(5)


