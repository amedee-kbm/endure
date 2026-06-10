"""
Locust load test for endure — RQ1 scalability evaluation.

Run headlessly (worker-sweep mode):
  locust --headless -u 10 -r 2 --run-time 60s --csv results/w4 \
         -f src/evaluate/load/locustfile.py --host http://localhost:8000

Run with web UI:
  locust -f src/evaluate/load/locustfile.py --host http://localhost:8000
  # open http://localhost:8089

Environment variables:
  ENDURE_TENANT_NAME    tenant name to use (default: locust-tenant)
  ENDURE_STAGE_DURATION seconds per synthetic job stage (default: 0.05)
  ENDURE_NUM_STAGES     number of stages per job (default: 5)
"""

import os
import time
import uuid

import requests as _requests
from locust import HttpUser, between, events, task

TENANT_NAME = os.environ.get("ENDURE_TENANT_NAME", "locust-tenant")
STAGE_DURATION = float(os.environ.get("ENDURE_STAGE_DURATION", "0.4"))
NUM_STAGES = int(os.environ.get("ENDURE_NUM_STAGES", "5"))
JOB_TYPE = "src.evaluate.jobs:SyntheticJob"
TERMINAL_STATES = {"COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED", "TIMED_OUT"}

_tenant_id: str | None = None


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Resolve (or create) the load-test tenant using plain requests so that
    setup calls never appear in Locust's stats and a 409 on an existing tenant
    is handled silently instead of triggering an error report."""
    global _tenant_id
    base = environment.host.rstrip("/")
    sess = _requests.Session()

    r = sess.post(
        f"{base}/api/v1/admin/tenants",
        json={
            "name": TENANT_NAME,
            "max_concurrent_jobs": 1000,
            "max_workers": 100,
        },
        timeout=10,
    )
    if r.status_code == 201:
        _tenant_id = str(r.json()["id"])
        return
    if r.status_code == 409:
        # Tenant already exists from a previous run — look it up by name.
        r2 = sess.get(f"{base}/api/v1/admin/tenants", timeout=10)
        for t in r2.json():
            if t["name"] == TENANT_NAME:
                _tenant_id = str(t["id"])
                return
    # Any other error: leave _tenant_id as None; tasks will no-op.
    print(f"[setup] WARNING: could not resolve tenant (status={r.status_code})")


class EndureUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(10)
    def submit_and_poll_job(self):
        if _tenant_id is None:
            return

        job_name = f"locust-{uuid.uuid4().hex[:8]}"
        r = self.client.post(
            "/api/v1/jobs",
            json={
                "name": job_name,
                "tenant_id": _tenant_id,
                "job_type": JOB_TYPE,
                "payload": {
                    "stage_duration": STAGE_DURATION,
                    "stages": NUM_STAGES,
                },
                "max_retries": 0,
                "timeout_seconds": 300,
            },
            name="/api/v1/jobs [submit]",
        )
        if not r.ok:
            return

        job_id = r.json()["id"]
        start = time.monotonic()
        deadline = start + 120.0

        while time.monotonic() < deadline:
            poll = self.client.get(
                f"/api/v1/jobs/{job_id}",
                name="/api/v1/jobs/{id} [poll]",
            )
            if not poll.ok:
                break
            state = poll.json().get("state", "")
            if state in TERMINAL_STATES:
                elapsed_ms = (time.monotonic() - start) * 1000
                self.environment.events.request.fire(
                    request_type="JOB",
                    name="job_completed_latency",
                    response_time=elapsed_ms,
                    response_length=0,
                    exception=None if state == "COMPLETED" else Exception(state),
                    context={},
                )
                break
            time.sleep(0.5)

    @task(1)
    def snapshot_metrics(self):
        self.client.get("/api/v1/metrics", name="/api/v1/metrics [snapshot]")
