"""Shared utilities for the Endure evaluation suite."""

from __future__ import annotations

import csv as csv_mod
import datetime
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = os.environ.get("ENDURE_API_URL", "http://localhost:8000")
RESULTS_DIR = Path(os.environ.get("ENDURE_RESULTS_DIR", "/app/loadtest-results"))

_SETTING_VARS: dict[str, tuple[str, str]] = {
    "worker_heartbeat_interval": ("ENDURE_WORKER_HEARTBEAT_INTERVAL", "2.0"),
    "worker_heartbeat_timeout": ("ENDURE_WORKER_HEARTBEAT_TIMEOUT", "15.0"),
    "scheduler_loop_interval": ("ENDURE_SCHEDULER_LOOP_INTERVAL", "0.1"),
    "worker_poll_interval": ("ENDURE_WORKER_POLL_INTERVAL", "0.5"),
    "leader_lock_ttl": ("ENDURE_LEADER_LOCK_TTL", "15.0"),
    "leader_heartbeat_interval": ("ENDURE_LEADER_HEARTBEAT_INTERVAL", "5.0"),
    "retry_base_delay": ("ENDURE_RETRY_BASE_DELAY", "2.0"),
}

TERMINAL_STATES = {"COMPLETED", "DEAD_LETTER", "CANCELLED"}


def capture_settings() -> dict[str, float]:
    return {k: float(os.getenv(ev, dv)) for k, (ev, dv) in _SETTING_VARS.items()}


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd="/app",
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def result_metadata() -> dict:
    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_commit": git_commit(),
        "settings": capture_settings(),
    }


def ts_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    print(f"[result] {path}")


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv_mod.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"[result] {path}")


# ---------------------------------------------------------------------------
# Database (psycopg2 — raw SQL for D4 arbitration and source_files count)
# ---------------------------------------------------------------------------

def _db_kwargs() -> dict:
    return dict(
        host=os.environ.get("ENDURE_DATABASE_HOST", "postgres"),
        port=int(os.environ.get("ENDURE_DATABASE_PORT", "5432")),
        dbname=os.environ.get("ENDURE_DATABASE_NAME", "endure"),
        user=os.environ.get("ENDURE_DATABASE_USER", "endure"),
        password=os.environ.get("ENDURE_DATABASE_PASSWORD", "endure"),
    )


def db_conn():
    import psycopg2  # optional; only needed for live-stack tests
    return psycopg2.connect(**_db_kwargs())


def db_fetchall(sql: str, params=None) -> list[tuple]:
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                return cur.fetchall()
    finally:
        conn.close()


def db_fetchone(sql: str, params=None) -> tuple | None:
    rows = db_fetchall(sql, params)
    return rows[0] if rows else None


def db_execute(sql: str, params=None) -> int:
    """Execute DML; return rowcount."""
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                return cur.rowcount
    finally:
        conn.close()


def source_file_count(tenant_id: str) -> int:
    row = db_fetchone("SELECT COUNT(*) FROM source_files WHERE tenant_id=%s", (tenant_id,))
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

def _api(method: str, path: str, **kwargs) -> Any:
    import requests  # optional; only needed for live-stack tests
    r = requests.request(method, f"{API_BASE}/api/v1/{path.lstrip('/')}", timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()


def ensure_tenant(name: str) -> dict:
    try:
        return _api("POST", "admin/tenants", json={
            "name": name,
            "max_concurrent_jobs": 100,
            "max_workers": 20,
        })
    except requests.HTTPError as exc:
        if exc.response.status_code == 409:
            tenants = _api("GET", "admin/tenants")
            return next(t for t in tenants if t["name"] == name)
        raise


def submit_report(
    tenant_id: str,
    payload: dict,
    *,
    report_type: str = "daily_import",
    max_retries: int = 3,
    timeout_seconds: int = 600,
) -> dict:
    return _api("POST", "reports", json={
        "tenant_id": str(tenant_id),
        "report_type": report_type,
        "payload": payload,
        "max_retries": max_retries,
        "timeout_seconds": timeout_seconds,
    })


def get_job(job_id: str) -> dict:
    return _api("GET", f"jobs/{job_id}")


def get_report(job_id: str) -> dict:
    return _api("GET", f"reports/{job_id}")


def get_events(job_id: str) -> list[dict]:
    return _api("GET", f"jobs/{job_id}/events")


def get_step_outputs(job_id: str) -> dict:
    return _api("GET", f"jobs/{job_id}/step-outputs")


def get_checkpoints(job_id: str) -> dict:
    return _api("GET", f"jobs/{job_id}/checkpoints")


def get_leader() -> dict | None:
    return _api("GET", "admin/leader")["leader"]


def get_workers(state: str | None = None) -> list[dict]:
    params = {"state": state} if state else {}
    return _api("GET", "workers", params=params)["workers"]


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------

def wait_for_state(
    job_id: str,
    states: str | set[str],
    timeout: float = 300,
    interval: float = 2.0,
) -> dict:
    if isinstance(states, str):
        states = {states}
    deadline = time.monotonic() + timeout
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_job(job_id)
        if job["state"] in states:
            return job
        if job["state"] in TERMINAL_STATES - states:
            raise AssertionError(
                f"Job {job_id} hit unexpected terminal state {job['state']!r}; "
                f"expected {states}"
            )
        time.sleep(interval)
    raise TimeoutError(
        f"Job {job_id} did not reach {states} within {timeout}s; "
        f"last state={job.get('state')}"
    )


def wait_for_event(
    job_id: str,
    event_type: str,
    timeout: float = 90,
    interval: float = 1.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for ev in get_events(job_id):
            if ev["event"] == event_type:
                return ev
        time.sleep(interval)
    raise TimeoutError(
        f"Event {event_type!r} never appeared for job {job_id} in {timeout}s"
    )


def wait_for_step_count(
    job_id: str,
    min_count: int,
    timeout: float = 120,
    interval: float = 1.0,
) -> dict:
    deadline = time.monotonic() + timeout
    data: dict = {}
    while time.monotonic() < deadline:
        data = get_step_outputs(job_id)
        if data["count"] >= min_count:
            return data
        time.sleep(interval)
    raise TimeoutError(
        f"step_outputs for {job_id} never reached {min_count} "
        f"(got {data.get('count', 0)}) in {timeout}s"
    )


def wait_for_checkpoint(
    job_id: str,
    min_count: int = 1,
    timeout: float = 180,
    interval: float = 2.0,
) -> dict:
    """Wait until at least min_count checkpoints exist for the job."""
    deadline = time.monotonic() + timeout
    data: dict = {}
    while time.monotonic() < deadline:
        data = get_checkpoints(job_id)
        if len(data.get("checkpoints", [])) >= min_count:
            return data
        time.sleep(interval)
    raise TimeoutError(
        f"Checkpoint count>={min_count} never reached for job {job_id} in {timeout}s"
    )


def wait_for_worker_offline(timeout: float = 60, interval: float = 2.0) -> dict:
    """Poll until any worker transitions to OFFLINE; return that worker record."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for w in get_workers():
            if w["state"] == "OFFLINE":
                return w
        time.sleep(interval)
    raise TimeoutError(f"No worker went OFFLINE within {timeout}s")


def wait_for_leader_change(
    current_holder: str,
    timeout: float = 90,
    interval: float = 2.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = get_leader()
        if info and info.get("holder_id") != current_holder:
            return info
        time.sleep(interval)
    raise TimeoutError(
        f"Leader did not change from {current_holder!r} in {timeout}s"
    )


# ---------------------------------------------------------------------------
# Container control (Docker SDK via /var/run/docker.sock)
# ---------------------------------------------------------------------------

def _docker():
    import docker as docker_sdk  # optional; only needed for live-stack tests
    return docker_sdk.from_env()


def find_service_containers(service_name: str) -> list:
    return _docker().containers.list(
        filters={
            "label": f"com.docker.compose.service={service_name}",
            "status": "running",
        }
    )


def kill_one(service_name: str) -> tuple[str, float]:
    """SIGKILL one running container of the named service.

    Returns (container_name, kill_time_utc_epoch).
    """
    containers = find_service_containers(service_name)
    if not containers:
        raise RuntimeError(f"No running containers for service={service_name!r}")
    target = containers[0]
    name = target.name
    t = time.time()
    target.kill()
    return name, t


def kill_named(container_name: str) -> float:
    """SIGKILL a container by exact name. Returns kill_time_utc_epoch."""
    t = time.time()
    _docker().containers.get(container_name).kill()
    return t


def holder_to_container(holder_id: str) -> str:
    """Map ENDURE_SCHEDULER_INSTANCE_ID to container_name (set in base compose)."""
    mapping = {
        "endure-scheduler": "endure-scheduler",
        "endure-scheduler-standby": "endure-scheduler-standby",
    }
    if holder_id not in mapping:
        raise ValueError(f"Unknown holder_id {holder_id!r}; expected one of {list(mapping)}")
    return mapping[holder_id]


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def parse_iso(ts: str) -> datetime.datetime:
    """Parse an ISO-8601 timestamp string to a UTC-aware datetime."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(ts).astimezone(datetime.timezone.utc)


def epoch(ts: str) -> float:
    return parse_iso(ts).timestamp()


# ---------------------------------------------------------------------------
# DrainSampler — queue-depth time-series (used by E4a and E4b)
# ---------------------------------------------------------------------------

class DrainSampler:
    """Samples job-state counts every `interval` seconds in a background thread."""

    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self.rows: list[dict] = []
        self._stop = threading.Event()
        self._t0 = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self._stop.is_set():
            counts = dict(
                db_fetchall("SELECT state, COUNT(*) FROM jobs GROUP BY state")
            )
            self.rows.append({
                "t_s": round(time.time() - self._t0, 1),
                "queued": counts.get("QUEUED", 0),
                "scheduled": counts.get("SCHEDULED", 0),
                "running": counts.get("RUNNING", 0),
                "completed": counts.get("COMPLETED", 0),
            })
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)
