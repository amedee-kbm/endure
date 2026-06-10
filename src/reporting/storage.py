"""
Report artifact storage — saves files to REPORT_OUTPUT_DIR.
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
    """Write report artifact to disk and return the absolute path.

    If content is bytes, ext defaults to 'xlsx'.
    If content is str, ext defaults to 'html'.
    The write is unconditional: re-writing with the same content is idempotent.
    """
    if ext is None:
        ext = "xlsx" if isinstance(content, bytes) else "html"

    base = Path(getattr(settings, "REPORT_OUTPUT_DIR", "/tmp/endure-reports"))
    out_dir = base / tenant_id / report_type
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{name}.{ext}"
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return str(path)
