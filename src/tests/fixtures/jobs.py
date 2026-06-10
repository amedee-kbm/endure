"""
Minimal job stubs for use in unit tests only.
No endure base class required — just the interface the executor expects.
"""

import json


class SuccessJob:
    def supports_checkpointing(self):
        return False

    async def run(self, payload, resume_state=None):
        return {"output": payload.get("x", 0) * 2}


class FailingJob:
    def supports_checkpointing(self):
        return False

    async def run(self, payload, resume_state=None):
        raise RuntimeError("intentional failure")


class CheckpointingJob:
    def __init__(self):
        self._checkpoint_sequence = 0

    def supports_checkpointing(self):
        return True

    def get_checkpoint_data(self, state: dict) -> bytes:
        return json.dumps(state).encode()

    def parse_checkpoint_data(self, data: bytes) -> dict:
        return json.loads(data)

    async def save_state(self) -> dict:
        return {"checkpoint_sequence": self._checkpoint_sequence}

    async def run(self, payload, resume_state=None, checkpoint_callback=None):
        stages = payload.get("stages", 3)
        for i in range(stages):
            if checkpoint_callback:
                await checkpoint_callback(
                    sequence=i + 1,
                    data=f"stage-{i}".encode(),
                )
        return {"stages_completed": stages}
