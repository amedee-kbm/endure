from contextvars import ContextVar
import uuid

_current_job_id: ContextVar[uuid.UUID | None] = ContextVar('_current_job_id', default=None)
_step_counter: ContextVar[int] = ContextVar('_step_counter', default=0)
