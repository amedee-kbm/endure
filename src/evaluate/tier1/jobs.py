"""
Controlled pipelines for deterministic fail-stop injection.

Fail-stop, from the database's perspective, is the absence of further writes.
We inject it faithfully in-process: run the executor as an asyncio task, hold
execution at a gate (an asyncio.Event), cancel the task, then run a fresh
executor for the same job_id. CALLS counts real stage/function executions
across both runs — the evidence that skip/replay worked.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

from src.framework.pipeline import Pipeline
from src.framework.step import step

CALLS: dict[str, int] = defaultdict(int)
GATES: dict[str, asyncio.Event] = {}


def reset() -> None:
    CALLS.clear()
    GATES.clear()


def gate(key: str) -> asyncio.Event:
    if key not in GATES:
        GATES[key] = asyncio.Event()
    return GATES[key]


async def _maybe_block(payload: dict, key_field: str) -> None:
    key = payload.get(key_field)
    if key:
        ev = gate(key)
        if not ev.is_set():
            await ev.wait()


class GateJob(Pipeline):
    """Three stages; s2 and s3 can each block at entry on a payload-named gate."""

    stages = ["s1", "s2", "s3"]

    async def s1(self, payload: dict, state: dict) -> dict:
        CALLS["s1"] += 1
        return {"out_s1": 1}

    async def s2(self, payload: dict, state: dict) -> dict:
        await _maybe_block(payload, "gate_s2")
        CALLS["s2"] += 1
        return {"out_s2": 2}

    async def s3(self, payload: dict, state: dict) -> dict:
        await _maybe_block(payload, "gate_s3")
        CALLS["s3"] += 1
        return {"out_s3": 3}


async def _work(i: int) -> int:
    CALLS["fn"] += 1
    return i * 2


class StepLoopJob(Pipeline):
    """One stage looping step() over n_items; blocks before item `block_after`."""

    stages = ["ingest"]

    async def ingest(self, payload: dict, state: dict) -> dict:
        n = payload["n_items"]
        block_after = payload.get("block_after")
        results = []
        for i in range(n):
            if block_after is not None and i == block_after:
                await _maybe_block(payload, "gate")
            results.append(await step(f"item_{i}", _work, i))
        return {"items": results}


async def _tagged(tag: str, i: int) -> str:
    CALLS[f"fn_{tag}"] += 1
    return f"{tag}-{i}"


class TwoStageStepJob(Pipeline):
    """Both stages use step() — regression guard for stage-namespaced identity."""

    stages = ["s1", "s2"]

    async def s1(self, payload: dict, state: dict) -> dict:
        return {"s1_items": [await step(f"a{i}", _tagged, "s1", i) for i in range(3)]}

    async def s2(self, payload: dict, state: dict) -> dict:
        await _maybe_block(payload, "gate_s2")
        return {"s2_items": [await step(f"b{i}", _tagged, "s2", i) for i in range(3)]}
