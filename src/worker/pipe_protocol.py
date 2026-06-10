"""
Typed message protocol for multiprocessing.Pipe communication between
IsolatedExecutor (parent) and the job subprocess (child).

Two message types:
  checkpoint — child sends serialized job state; parent persists it via checkpoint_manager
  result     — child sends final job outcome (success or failure)

Bytes are base64-encoded so the dict is inspectable in logs and forward-compatible
with non-pickle transports. The Pipe uses pickle internally, so the encoding is
defensive rather than strictly necessary.
"""

import base64
from dataclasses import dataclass

MESSAGE_TYPE_CHECKPOINT = "checkpoint"
MESSAGE_TYPE_RESULT = "result"


@dataclass
class CheckpointMessage:
    sequence: int
    data: bytes


@dataclass
class ResultMessage:
    success: bool
    result: object
    error: str | None
    traceback: str | None


def build_checkpoint_message(sequence: int, data: bytes) -> dict:
    return {
        "type": MESSAGE_TYPE_CHECKPOINT,
        "sequence": sequence,
        "data": base64.b64encode(data).decode("ascii"),
    }


def build_result_message(
    success: bool,
    result=None,
    error: str | None = None,
    traceback_str: str | None = None,
) -> dict:
    return {
        "type": MESSAGE_TYPE_RESULT,
        "success": success,
        "result": result,
        "error": error,
        "traceback": traceback_str,
    }


def parse_pipe_message(raw: dict) -> CheckpointMessage | ResultMessage:
    t = raw.get("type")
    if t == MESSAGE_TYPE_CHECKPOINT:
        return CheckpointMessage(
            sequence=raw["sequence"],
            data=base64.b64decode(raw["data"]),
        )
    elif t == MESSAGE_TYPE_RESULT:
        return ResultMessage(
            success=raw["success"],
            result=raw.get("result"),
            error=raw.get("error"),
            traceback=raw.get("traceback"),
        )
    else:
        raise ValueError(f"Unknown message type: {t!r}")
