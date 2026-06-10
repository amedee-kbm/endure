from __future__ import annotations
import json
from typing import Any, Callable, Awaitable
from src.framework.context import _current_job_id, _step_counter


async def step(name: str, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
    """
    Checkpoint an individual sub-operation within a pipeline stage.

    Before executing fn, checks whether a StepOutput with this position already
    exists for the current job (crash recovery). If yes, returns the cached result.
    If no, executes fn, records the result, and returns it.

    The job context (job_id, step counter) is managed automatically via contextvars
    set by the worker before job execution begins.
    """
    # Lazy import — avoids Django ORM startup at module import time
    from src.models import StepOutput  # noqa: PLC0415

    job_id = _current_job_id.get()
    counter = _step_counter.get()
    _step_counter.set(counter + 1)

    if job_id is not None:
        existing = await StepOutput.objects.filter(
            job_id=job_id, step_id=counter
        ).afirst()
        if existing is not None:
            return json.loads(existing.output)

    result = await fn(*args, **kwargs)

    if job_id is not None:
        await StepOutput.objects.acreate(
            job_id=job_id,
            step_id=counter,
            step_name=name,
            output=json.dumps(result, default=str),
        )

    return result
