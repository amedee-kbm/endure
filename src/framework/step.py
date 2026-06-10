from __future__ import annotations
import json
from typing import Any, Callable, Awaitable

from django.db import IntegrityError

from src.framework.context import _current_job_id, _step_counter


async def step(name: str, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
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
        try:
            await StepOutput.objects.acreate(
                job_id=job_id,
                step_id=counter,
                step_name=name,
                output=json.dumps(result, default=str),
            )
        except IntegrityError:
            # A concurrent execution of the same job already recorded this
            # step (false-suspect re-dispatch race).  The unique_together
            # constraint on (job, step_id) guarantees exactly one row exists;
            # swallowing the error here is safe because the result we computed
            # is identical to the one already stored (idempotent fn).
            pass

    return result
