"""Shared HTTP helpers for evaluation tests."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import httpx

TERMINAL_STATES = frozenset(
    {"COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED", "TIMED_OUT"}
)

SYNTHETIC_JOB = "src.evaluate.jobs:SyntheticJob"


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def create_tenant(
    client: httpx.AsyncClient,
    *,
    name: str,
    max_concurrent_jobs: int = 32,
    max_workers: int = 8,
) -> str:
    """Create a tenant and return its UUID string. Handles 409 by looking up existing."""
    r = await client.post(
        "/api/v1/admin/tenants",
        json={
            "name": name,
            "max_concurrent_jobs": max_concurrent_jobs,
            "max_workers": max_workers,
        },
    )
    if r.status_code == 409:
        r2 = await client.get("/api/v1/admin/tenants")
        r2.raise_for_status()
        for t in r2.json():
            if t["name"] == name:
                return str(t["id"])
        raise RuntimeError(f"Tenant {name!r} reported conflict but not found")
    r.raise_for_status()
    return str(r.json()["id"])


async def submit_job(
    client: httpx.AsyncClient,
    *,
    tenant_id: str,
    name: str | None = None,
    job_type: str = SYNTHETIC_JOB,
    payload: dict[str, Any] | None = None,
    max_retries: int = 3,
    timeout_seconds: int = 30,
) -> str:
    """Submit a job and return its UUID string."""
    r = await client.post(
        "/api/v1/jobs",
        json={
            "name": name or unique_name("eval-job"),
            "tenant_id": tenant_id,
            "job_type": job_type,
            "payload": payload or {},
            "max_retries": max_retries,
            "timeout_seconds": timeout_seconds,
        },
    )
    r.raise_for_status()
    return r.json()["id"]


async def get_job(client: httpx.AsyncClient, job_id: str) -> dict:
    r = await client.get(f"/api/v1/jobs/{job_id}")
    r.raise_for_status()
    return r.json()


async def get_events(client: httpx.AsyncClient, job_id: str) -> list[dict]:
    r = await client.get(f"/api/v1/jobs/{job_id}/events")
    r.raise_for_status()
    return r.json()


async def wait_for_job(
    client: httpx.AsyncClient,
    job_id: str,
    *,
    target_states: frozenset[str] = TERMINAL_STATES,
    timeout: float = 120.0,
    poll_interval: float = 1.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = await get_job(client, job_id)
        if job["state"] in target_states:
            return job
        await asyncio.sleep(poll_interval)
    job = await get_job(client, job_id)
    raise TimeoutError(
        f"Job {job_id} stuck in {job['state']!r}, expected one of {target_states}"
    )


async def wait_for_jobs(
    client: httpx.AsyncClient,
    job_ids: list[str],
    *,
    timeout: float = 300.0,
    poll_interval: float = 2.0,
) -> list[dict]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = await asyncio.gather(*(get_job(client, jid) for jid in job_ids))
        if all(j["state"] in TERMINAL_STATES for j in jobs):
            return list(jobs)
        await asyncio.sleep(poll_interval)
    raise TimeoutError(f"Not all {len(job_ids)} jobs finished within {timeout}s")


async def wait_for_running_count(
    client: httpx.AsyncClient,
    job_ids: list[str],
    min_running: int,
    *,
    timeout: float = 90.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = await asyncio.gather(*(get_job(client, jid) for jid in job_ids))
        if sum(1 for j in jobs if j["state"] == "RUNNING") >= min_running:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError(f"Fewer than {min_running} jobs reached RUNNING within {timeout}s")


async def wait_for_checkpoint(
    client: httpx.AsyncClient,
    job_id: str,
    *,
    min_total: int = 1,
    timeout: float = 120.0,
    poll_interval: float = 0.5,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = await client.get(f"/api/v1/jobs/{job_id}/checkpoints")
        r.raise_for_status()
        data = r.json()
        if int(data.get("total", 0)) >= min_total:
            return data
        await asyncio.sleep(poll_interval)
    raise TimeoutError(
        f"Job {job_id} did not reach {min_total} checkpoint(s) within {timeout}s"
    )


async def get_assigned_worker_hostname(
    client: httpx.AsyncClient,
    job_id: str,
    *,
    timeout: float = 60.0,
    poll_interval: float = 0.5,
) -> str:
    """Return the hostname of the worker assigned to this job (usable with docker stop)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = await get_job(client, job_id)
        wid = job.get("assigned_worker_id")
        if wid is not None:
            r = await client.get("/api/v1/workers")
            r.raise_for_status()
            for w in r.json().get("workers", []):
                if str(w.get("id")) == str(wid):
                    host = w.get("hostname", "")
                    if host:
                        return host
        await asyncio.sleep(poll_interval)
    raise TimeoutError(
        f"Could not resolve assigned worker hostname for job {job_id} within {timeout}s"
    )


