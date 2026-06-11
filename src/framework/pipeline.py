import json
import logging

from src.framework.context import _current_stage, _step_counter

logger = logging.getLogger("endure.framework.pipeline")


class Pipeline:
    stages: list[str] = []
    schedule: str | None = None
    timeout: int = 3600

    def __init__(self):
        self._checkpoint_sequence: int = 0
        self._completed_stages: list[str] = []

    def supports_checkpointing(self) -> bool:
        return True

    def get_checkpoint_data(self, state: dict) -> bytes:
        return json.dumps(state).encode()

    def parse_checkpoint_data(self, data: bytes) -> dict:
        return json.loads(data)

    async def save_state(self) -> dict:
        return {"completed_stages": list(self._completed_stages)}

    async def run(self, payload: dict, resume_state=None, checkpoint_callback=None):
        completed = set((resume_state or {}).get("completed_stages", []))
        state: dict = (resume_state or {}).copy()
        # Seed from resume state so post-resume checkpoints carry the full list
        # (preserve declared stage order).
        self._completed_stages = [s for s in self.stages if s in completed]

        for stage_name in self.stages:
            if stage_name in completed:
                logger.debug(f"Skipping completed stage: {stage_name}")
                continue

            _current_stage.set(stage_name)
            _step_counter.set(0)

            logger.info(f"Running stage: {stage_name}")
            stage_fn = getattr(self, stage_name)
            update = await stage_fn(payload, state)
            state.update(update or {})

            self._completed_stages.append(stage_name)
            self._checkpoint_sequence += 1

            if checkpoint_callback:
                snap = {**state, "completed_stages": list(self._completed_stages)}
                await checkpoint_callback(
                    sequence=self._checkpoint_sequence,
                    data=json.dumps(snap).encode(),
                )

        return {**state, "completed_stages": list(self._completed_stages)}
