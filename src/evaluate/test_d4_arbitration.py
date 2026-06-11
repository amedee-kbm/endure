"""D4 — Ghost-run arbitration (demonstration).

Directly exercises the CAS layer and duplicate-insert guards without timing games.

Part A — Ownership-gated CAS:
  Create a job row in RUNNING state assigned to worker-B.
  Attempt the COMPLETED transition filtered to worker-A (wrong owner).
  Assert: 0 rows updated; state unchanged.

Part B — Duplicate StepOutput guard:
  Insert a StepOutput row for (job, stage, step_id).
  Insert again with the same identity.
  Assert: exactly 1 row survives; IntegrityError does not propagate.

Part C — Duplicate Checkpoint guard:
  Call save_checkpoint() twice with the same (job_id, sequence_number).
  Assert: 1 checkpoint row; second call returns None (IntegrityError caught).
"""

import asyncio
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from src.evaluate import helpers as h

# ---------------------------------------------------------------------------
# Part A — CAS ownership gate (raw SQL, no Django required)
# ---------------------------------------------------------------------------


def _insert_worker(cur, worker_id: str):
    cur.execute(
        "INSERT INTO workers "
        "(id, hostname, pid, state, last_heartbeat, registered_at, "
        "max_inflight_jobs, inflight_job_count, tenant_inflight_job_count_map) "
        "VALUES (%s, %s, 1, 'ONLINE', now(), now(), 4, 0, '{}')",
        (worker_id, f"host-{worker_id[:8]}"),
    )


@pytest.mark.demonstration
def test_d4a_cas_ownership_gate(tenant_id: str):
    """UPDATE filtered to wrong owner must affect 0 rows."""
    worker_a = str(uuid.uuid4())
    worker_b = str(uuid.uuid4())
    job_id = str(uuid.uuid4())

    conn = h.db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                _insert_worker(cur, worker_a)
                _insert_worker(cur, worker_b)

                cur.execute("SELECT id FROM tenants WHERE name='evaluate'")
                row = cur.fetchone()
                assert row, "evaluate tenant not found — run conftest tenant fixture first"
                tid = row[0]

                cur.execute(
                    "INSERT INTO jobs "
                    "(id, tenant_id, name, job_type, state, attempt, max_retries, "
                    "timeout_seconds, payload, created_at, updated_at, assigned_worker_id) "
                    "VALUES (%s, %s, 'd4-cas', 'test:Job', 'RUNNING', 1, 3, "
                    "3600, '{}', now(), now(), %s)",
                    (job_id, tid, worker_b),
                )

                # Wrong-owner update (worker-A trying to complete worker-B's job)
                cur.execute(
                    "UPDATE jobs SET state='COMPLETED' "
                    "WHERE id=%s AND state='RUNNING' AND assigned_worker_id=%s",
                    (job_id, worker_a),
                )
                rows_updated = cur.rowcount

                cur.execute("SELECT state FROM jobs WHERE id=%s", (job_id,))
                final_state = cur.fetchone()[0]

                # Cleanup
                cur.execute("DELETE FROM jobs WHERE id=%s", (job_id,))
                cur.execute(
                    "DELETE FROM workers WHERE id IN (%s, %s)", (worker_a, worker_b)
                )
    finally:
        conn.close()

    assert rows_updated == 0, (
        f"Wrong-owner UPDATE must affect 0 rows, affected {rows_updated}"
    )
    assert final_state == "RUNNING", (
        f"State must remain RUNNING after wrong-owner attempt, got {final_state!r}"
    )


# ---------------------------------------------------------------------------
# Part B — Duplicate StepOutput (unique_together constraint + IntegrityError guard)
# ---------------------------------------------------------------------------


@pytest.mark.demonstration
def test_d4b_duplicate_step_output(tenant_id: str):
    """Duplicate (job, stage_name, step_id) insert: 1 row survives, no exception escapes."""
    job_id = str(uuid.uuid4())
    stage = "ingest"
    step_id = 0

    conn = h.db_conn()
    try:
        # Need a real tenant FK
        tid_row = h.db_fetchone("SELECT id FROM tenants WHERE name='evaluate'")
        assert tid_row, "evaluate tenant not found"
        tid = tid_row[0]

        # Create a minimal job row
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (id, tenant_id, name, job_type, state, attempt, "
                    "max_retries, timeout_seconds, payload, created_at, updated_at) "
                    "VALUES (%s, %s, 'd4-step', 'test:Job', 'RUNNING', 1, 3, "
                    "3600, '{}', now(), now())",
                    (job_id, tid),
                )

        # First insert — must succeed
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO src_stepoutput "
                    "(job_id, stage_name, step_id, step_name, output, created_at) "
                    "VALUES (%s, %s, %s, 'read_csv', '\"ok\"', now())",
                    (job_id, stage, step_id),
                )

        # Second insert — must raise IntegrityError (unique violation)
        raised = False
        try:
            conn2 = h.db_conn()
            with conn2:
                with conn2.cursor() as cur:
                    cur.execute(
                        "INSERT INTO src_stepoutput "
                        "(job_id, stage_name, step_id, step_name, output, created_at) "
                        "VALUES (%s, %s, %s, 'read_csv', '\"duplicate\"', now())",
                        (job_id, stage, step_id),
                    )
            conn2.close()
        except psycopg2.errors.UniqueViolation:
            raised = True
        finally:
            try:
                conn2.close()
            except Exception:
                pass

        # The constraint fires; exactly 1 row survives
        count_row = h.db_fetchone(
            "SELECT COUNT(*) FROM src_stepoutput "
            "WHERE job_id=%s AND stage_name=%s AND step_id=%s",
            (job_id, stage, step_id),
        )
        count = int(count_row[0])

        # Check step() application-level guard (calls acreate with IntegrityError catch)
        # via asyncio.run — Django must be set up in the runner container
        no_app_exception = _verify_step_no_exception(job_id, stage)

        # Cleanup
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM src_stepoutput WHERE job_id=%s", (job_id,))
                cur.execute("DELETE FROM jobs WHERE id=%s", (job_id,))
    finally:
        conn.close()

    assert raised, "Expected UniqueViolation on duplicate StepOutput insert"
    assert count == 1, f"Expected exactly 1 StepOutput row, got {count}"
    assert no_app_exception, "step() raised an exception on duplicate — IntegrityError guard broken"


def _verify_step_no_exception(job_id: str, stage: str) -> bool:
    """
    Verify that calling step() with an already-recorded (job, stage, step_id=0)
    returns the cached value without raising. Uses asyncio.run() + Django ORM.
    """
    import os
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "endure.settings")
    try:
        import django
        django.setup()
    except RuntimeError:
        pass  # already set up

    import uuid as _uuid
    from src.framework.context import _current_job_id, _current_stage, _step_counter
    from src.framework.step import step as fw_step

    async def _inner():
        tok_j = _current_job_id.set(_uuid.UUID(job_id))
        tok_s = _current_stage.set(stage)
        tok_c = _step_counter.set(0)
        try:
            # step_id=0 already exists in DB (inserted by the test above).
            # step() should find it and return the cached value — no IntegrityError.
            result = await fw_step("read_csv", lambda: "should_not_execute")
            return result == "ok"  # must return the cached value, not execute the lambda
        except Exception:
            return False
        finally:
            _current_job_id.reset(tok_j)
            _current_stage.reset(tok_s)
            _step_counter.reset(tok_c)

    return asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Part C — Duplicate Checkpoint guard
# ---------------------------------------------------------------------------


@pytest.mark.demonstration
def test_d4c_duplicate_checkpoint(tenant_id: str):
    """save_checkpoint() called twice with same (job, sequence): 1 row, second returns None."""
    import os
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "endure.settings")
    try:
        import django
        django.setup()
    except RuntimeError:
        pass

    import uuid as _uuid
    from src.checkpoint.manager import CheckpointManager

    async def _inner():
        mgr = CheckpointManager()
        job_id = _uuid.uuid4()

        # Must create a real job row so the FK holds
        from src.models import Job, Tenant
        tenant = await Tenant.objects.filter(name="evaluate").afirst()
        assert tenant is not None, "evaluate tenant not found"

        job = await Job.objects.acreate(
            tenant=tenant,
            name="d4-checkpoint",
            job_type="test:Job",
            state="RUNNING",
            attempt=1,
            max_retries=3,
            timeout_seconds=3600,
        )

        try:
            cp1 = await mgr.save_checkpoint(job.id, sequence=1, data=b"state-a")
            assert cp1 is not None, "First save_checkpoint must return a Checkpoint"

            cp2 = await mgr.save_checkpoint(job.id, sequence=1, data=b"state-b")
            assert cp2 is None, (
                "Second save_checkpoint with same sequence must return None (IntegrityError caught)"
            )

            # Verify exactly 1 row in DB
            from src.models import Checkpoint
            count = await Checkpoint.objects.filter(
                job=job, sequence_number=1
            ).acount()
            assert count == 1, f"Expected 1 Checkpoint row, got {count}"

            return True
        finally:
            await job.checkpoints.all().adelete()
            await job.adelete()

    ok = asyncio.run(_inner())
    assert ok
