# PATCHES — apply before integrating the test suite

## Patch 1: `src/framework/pipeline.py` — resume state-merge bug (CORRECTNESS)

**Bug.** On resume, `self._completed_stages` starts empty (fresh instance) and the
checkpoint snapshot is built as `{"completed_stages": new_list, **state}`. Since
`state` was copied from `resume_state`, it still contains the OLD
`completed_stages` key, and the `**state` spread comes second — so every
post-resume checkpoint records the STALE completed list. A job that crashes,
resumes, and crashes again re-executes stages it completed after the first
resume. `tier1/test_recovery.py::test_double_crash_resume` fails until this is
applied.

**Change A** — seed the instance list from resume state. Replace:

```python
        completed = set((resume_state or {}).get("completed_stages", []))
        state: dict = (resume_state or {}).copy()
```

with:

```python
        completed = set((resume_state or {}).get("completed_stages", []))
        state: dict = (resume_state or {}).copy()
        # Seed from resume state so post-resume checkpoints carry the full list
        # (preserve declared stage order).
        self._completed_stages = [s for s in self.stages if s in completed]
```

**Change B** — make the fresh list win in the snapshot. Replace:

```python
                snap = {"completed_stages": list(self._completed_stages), **state}
```

with:

```python
                snap = {**state, "completed_stages": list(self._completed_stages)}
```

**Change C** — same ordering for the final return. Replace:

```python
        return {"completed_stages": list(self._completed_stages), **state}
```

with:

```python
        return {**state, "completed_stages": list(self._completed_stages)}
```
