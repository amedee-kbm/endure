"""SleepJob — incompressible-duration workload for scheduler-scaling sweeps.

The single stage sleeps for payload["duration_s"] (default 10). Because the
work cannot be sped up or contended, any deviation from ideal scaling in a
worker-count sweep is attributable to the scheduler itself.
"""

from __future__ import annotations

import asyncio

from src.framework.pipeline import Pipeline


class SleepJob(Pipeline):
    stages = ["sleep"]
    timeout = 600

    async def sleep(self, payload: dict, state: dict) -> dict:
        duration = float(payload.get("duration_s", 10.0))
        await asyncio.sleep(duration)
        return {"slept_s": duration}
