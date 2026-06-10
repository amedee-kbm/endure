import json
import logging

logger = logging.getLogger("endure.framework.pipeline")


class Pipeline:
    stages: list[str] = []
    schedule: str | None = None
    timeout: int = 3600

    def __init__(self):
        self._checkpoint_sequence: int = 0
        self._completed_stages: list[str] = []

    # ------------------------------------------------------------------
    # Scheduler job protocol
    # ------------------------------------------------------------------

    def supports_checkpointing(self) -> bool:
        return True

    def get_checkpoint_data(self, state: dict) -> bytes:
        return json.dumps(state).encode()

    def parse_checkpoint_data(self, data: bytes) -> dict:
        return json.loads(data)

    async def save_state(self) -> dict:
        return {"completed_stages": list(self._completed_stages)}

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self, payload: dict, resume_state=None, checkpoint_callback=None):
        completed = set((resume_state or {}).get("completed_stages", []))
        state: dict = (resume_state or {}).copy()

        for stage_name in self.stages:
            if stage_name in completed:
                logger.debug(f"Skipping completed stage: {stage_name}")
                continue

            logger.info(f"Running stage: {stage_name}")
            stage_fn = getattr(self, stage_name)
            update = await stage_fn(payload, state)
            state.update(update or {})

            self._completed_stages.append(stage_name)
            self._checkpoint_sequence += 1

            if checkpoint_callback:
                snap = {"completed_stages": list(self._completed_stages), **state}
                await checkpoint_callback(
                    sequence=self._checkpoint_sequence,
                    data=json.dumps(snap).encode(),
                )

        return {"completed_stages": list(self._completed_stages), **state}


# Backward compatibility alias
BaseReportJob = Pipeline
