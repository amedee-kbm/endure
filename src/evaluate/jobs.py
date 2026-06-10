"""
Synthetic job implementation used exclusively in evaluation tests.

These jobs are executed by the real worker process, so they must be importable
from inside the Docker container (PYTHONPATH=/app covers src/evaluate/).

job_type to use in API calls:
  "src.evaluate.jobs:SyntheticJob"
"""

import asyncio
import json


class SyntheticJob:
    """
    Multi-stage job with configurable duration, optional failure, and checkpointing.

    Payload fields:
      stage_duration (float)  seconds per stage (default 0.3)
      stages (int)            number of stages (default 5)
      fail_at_stage (int)     0-based stage index to raise on (default None)
    """

    STAGE_NAMES = ["extract_data", "aggregate_metrics", "generate_charts", "render_report", "upload_artifact"]

    def __init__(self):
        self._checkpoint_sequence = 0
        self._completed_stages: list[str] = []

    def supports_checkpointing(self):
        return True

    def get_checkpoint_data(self, state: dict) -> bytes:
        return json.dumps(state).encode()

    def parse_checkpoint_data(self, data: bytes) -> dict:
        return json.loads(data)

    async def save_state(self) -> dict:
        return {"completed_stages": self._completed_stages}

    async def run(self, payload: dict, resume_state=None, checkpoint_callback=None):
        stage_duration = float(payload.get("stage_duration", 0.3))
        num_stages = int(payload.get("stages", 5))
        fail_at_stage = payload.get("fail_at_stage")  # 0-based index or None

        stage_names = self.STAGE_NAMES[:num_stages]
        previously_completed = list((resume_state or {}).get("completed_stages", []))
        self._completed_stages = list(previously_completed)
        completed = set(previously_completed)

        for i, stage in enumerate(stage_names):
            if stage in completed:
                continue

            await asyncio.sleep(stage_duration)

            if fail_at_stage is not None and i == int(fail_at_stage):
                raise RuntimeError(f"Intentional failure at stage {i} ({stage!r})")

            self._completed_stages.append(stage)
            self._checkpoint_sequence += 1

            if checkpoint_callback:
                state = {"completed_stages": self._completed_stages[:]}
                await checkpoint_callback(
                    sequence=self._checkpoint_sequence,
                    data=json.dumps(state).encode(),
                )

        return {"completed_stages": self._completed_stages}
