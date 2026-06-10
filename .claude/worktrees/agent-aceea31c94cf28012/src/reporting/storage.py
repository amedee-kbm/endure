"""
Report artifact storage — saves report files to REPORT_OUTPUT_DIR.
"""

from pathlib import Path

from django.conf import settings


def save_artifact(
    tenant_id: str,
    report_type: str,
    name: str,
    content: str | bytes,
    ext: str | None = None,
) -> str:
    """Write a report artifact to disk and return the absolute path as a string.

    The ``ext`` parameter controls the file extension:
    - If provided explicitly it is used as-is.
    - If *content* is ``bytes`` and *ext* is not given, defaults to ``"xlsx"``.
    - If *content* is ``str`` and *ext* is not given, defaults to ``"html"``.

    The write is unconditional: if the file already exists (e.g. after a
    checkpoint resume that re-runs this stage), it is overwritten.  Callers
    that produce deterministic content for a given (tenant_id, report_type,
    name) triple therefore get idempotent behaviour at no extra cost.
    """
    if ext is None:
        ext = "xlsx" if isinstance(content, bytes) else "html"

    base = Path(getattr(settings, "REPORT_OUTPUT_DIR", "/tmp/endure-reports"))
    out_dir = base / str(tenant_id) / report_type
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{name}.{ext}"
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return str(path)
