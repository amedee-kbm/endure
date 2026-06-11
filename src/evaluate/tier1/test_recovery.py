"""
RQ1 correctness, deterministically: abandon-and-resume fail-stop injection.

Each test: run the executor as a task, hold it at a gate, cancel (fail-stop:
no further writes), then resume with a fresh executor. Assertions are over
database evidence plus exact execution counts.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from src.evaluate.tier1 import jobs as J
from src.models import Checkpoint, StepOutput
from src.worker.executor import JobExecutor

pytestmark = pytest.mark.asyncio

JOB_GATE = "src.evaluate.tier1.jobs:GateJob"
JOB_STEP = "src.evaluate.tier1.jobs:StepLoopJob"
JOB_2STAGE = "src.evaluate.tier1.jobs:TwoStageStepJob"


async def _run_until_gated_then_kill(job_type, payload, job_id, *, checkpoints):
    """Start execution, wait until `checkpoints` snapshots exist, fail-stop it."""
    task = asyncio.create_task(JobExecutor().execute(job_type, payload, job_id=job_id))
    for _ in range(400):  # ≤ 20 s safety ceiling; normally milliseconds
        if await Checkpoint.objects.filter(job_id=job_id).acount() >= checkpoints:
            break
        await asyncio.sleep(0.05)
    else:
        task.cancel()
        pytest.fail(f"never reached {checkpoints} checkpoints")
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_stage_resume_skips_completed(make_job):
    J.reset()
    job = await make_job(job_type=JOB_GATE)
    payload = {"gate_s3": "g3"}

    await _run_until_gated_then_kill(JOB_GATE, payload, job.id, checkpoints=2)
    assert J.CALLS["s1"] == 1 and J.CALLS["s2"] == 1
    s3_first = J.CALLS["s3"]  # 0 or 1 depending on cancel landing before/at entry

    J.gate("g3").set()  # resumed run must not block
    result = await JobExecutor().execute(JOB_GATE, payload, job_id=job.id)

    assert result["success"] is True
    assert result["result"]["completed_stages"] == ["s1", "s2", "s3"]
    # Checkpointed stages never re-execute; the interrupted stage re-executes
    # (at-least-once) exactly once more.
    assert J.CALLS["s1"] == 1
    assert J.CALLS["s2"] == 1
    assert J.CALLS["s3"] == s3_first + 1


async def test_step_resume_replays_recorded(make_job):
    J.reset()
    job = await make_job(job_type=JOB_STEP)
    payload = {"n_items": 20, "block_after": 10, "gate": "gs"}

    task = asyncio.create_task(JobExecutor().execute(JOB_STEP, payload, job_id=job.id))
    for _ in range(400):
        if await StepOutput.objects.filter(job_id=job.id).acount() >= 10:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert await StepOutput.objects.filter(job_id=job.id).acount() == 10
    assert J.CALLS["fn"] == 10

    J.gate("gs").set()
    result = await JobExecutor().execute(JOB_STEP, payload, job_id=job.id)

    assert result["success"] is True
    assert await StepOutput.objects.filter(job_id=job.id).acount() == 20
    # 10 originals + 10 post-resume; the recorded 10 were replayed, not re-run.
    assert J.CALLS["fn"] == 20
    assert result["result"]["items"] == [i * 2 for i in range(20)]


async def test_double_crash_resume(make_job):
    """Crash in s2, resume; crash in s3, resume. Catches the snapshot
    state-merge bug (PATCHES.md): without the patch, the post-resume
    checkpoint carries the stale completed list and s2 runs a third time."""
    import json as _json

    J.reset()
    job = await make_job(job_type=JOB_GATE)
    payload = {"gate_s2": "g2", "gate_s3": "g3"}

    await _run_until_gated_then_kill(JOB_GATE, payload, job.id, checkpoints=1)

    J.gate("g2").set()
    await _run_until_gated_then_kill(JOB_GATE, payload, job.id, checkpoints=2)

    # After resume-1 completes s2 and is killed at s3's gate, the latest
    # checkpoint must carry the FULL completed list (s1 + s2). Without the
    # patch, it only contains ["s1"] and a second crash re-executes s2.
    latest = await Checkpoint.objects.filter(job_id=job.id).order_by(
        "-sequence_number").afirst()
    completed = _json.loads(bytes(latest.data))["completed_stages"]
    assert "s2" in completed, (
        "checkpoint written after a resume must carry the full completed-stage "
        "list (resumed stages included), or a second crash re-executes them"
    )

    J.gate("g3").set()
    result = await JobExecutor().execute(JOB_GATE, payload, job_id=job.id)

    assert result["success"] is True
    assert J.CALLS["s1"] == 1, "s1 re-executed after a later resume"
    assert J.CALLS["s2"] <= 2, "s2 re-executed: stale completed_stages snapshot"


async def test_stage_namespaced_step_identity(make_job):
    """Crash before s2's steps after s1's are recorded; the resume must
    produce s2's own outputs, never replay s1's rows under s2's counters."""
    J.reset()
    job = await make_job(job_type=JOB_2STAGE)
    payload = {"gate_s2": "g2"}

    await _run_until_gated_then_kill(JOB_2STAGE, payload, job.id, checkpoints=1)
    assert J.CALLS["fn_s1"] == 3

    J.gate("g2").set()
    result = await JobExecutor().execute(JOB_2STAGE, payload, job_id=job.id)

    assert result["success"] is True
    assert result["result"]["s2_items"] == ["s2-0", "s2-1", "s2-2"]
    assert J.CALLS["fn_s1"] == 3 and J.CALLS["fn_s2"] == 3
    rows = [
        (s.stage_name, s.step_id)
        async for s in StepOutput.objects.filter(job_id=job.id).order_by(
            "stage_name", "step_id"
        )
    ]
    assert rows == [("s1", 0), ("s1", 1), ("s1", 2), ("s2", 0), ("s2", 1), ("s2", 2)]
