"""
DailyImportJob — multi-file ingest pipeline with step-level recovery.

Scheduler features exercised:
  - Pipeline stage checkpointing (resume on failure)
  - step() durable execution (each file processed as a recoverable step)
  - Data quality validation (fails if error rate exceeds threshold)
  - .xlsx artifact output (no external dependencies — uses zipfile)

Payload fields:
  tenant_id      (str)   — tenant identifier
  n_files        (int)   — number of synthetic import files  [default: 5]
  rows_per_file  (int)   — rows per synthetic file           [default: 100]
  seed           (int)   — RNG seed for deterministic data   [default: 42]
  inject_errors  (int)   — number of bad rows to inject      [default: 0]
"""

from __future__ import annotations

import asyncio
import io
import random
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from django.conf import settings

from endure import step
from src.reporting.jobs.base import BaseReportJob


# ---------------------------------------------------------------------------
# Minimal xlsx writer (no openpyxl / xlsxwriter dependency)
# ---------------------------------------------------------------------------

_CONTENT_TYPES = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
    '<Default Extension="xml" ContentType="application/xml"/>'
    '<Override PartName="/xl/workbook.xml"'
    ' ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
    '<Override PartName="/xl/worksheets/sheet1.xml"'
    ' ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
    '<Override PartName="/xl/worksheets/sheet2.xml"'
    ' ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
    '<Override PartName="/xl/sharedStrings.xml"'
    ' ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
    '</Types>'
)

_RELS = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1"'
    ' Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"'
    ' Target="xl/workbook.xml"/>'
    '</Relationships>'
)

_WORKBOOK = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
    '<sheets>'
    '<sheet name="Import" sheetId="1" r:id="rId1"/>'
    '<sheet name="Summary" sheetId="2" r:id="rId2"/>'
    '</sheets>'
    '</workbook>'
)

_WORKBOOK_RELS = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
    '<Relationship Id="rId1"'
    ' Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"'
    ' Target="worksheets/sheet1.xml"/>'
    '<Relationship Id="rId2"'
    ' Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"'
    ' Target="worksheets/sheet2.xml"/>'
    '<Relationship Id="rId3"'
    ' Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"'
    ' Target="sharedStrings.xml"/>'
    '</Relationships>'
)


def _escape_xml(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _build_shared_strings(strings: list[str]) -> str:
    items = "".join(f"<si><t>{_escape_xml(s)}</t></si>" for s in strings)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
        f' count="{len(strings)}" uniqueCount="{len(strings)}">{items}</sst>'
    )


def _build_data_sheet(rows: list[list]) -> tuple[str, list[str]]:
    """Return (sheet_xml, shared_string_list). Numbers are inlined; strings use sst."""
    sst: list[str] = []
    sst_index: dict[str, int] = {}
    col_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def _str_cell(val: str, col: str, row_num: int) -> str:
        if val not in sst_index:
            sst_index[val] = len(sst)
            sst.append(val)
        return f'<c r="{col}{row_num}" t="s"><v>{sst_index[val]}</v></c>'

    def _num_cell(val, col: str, row_num: int) -> str:
        return f'<c r="{col}{row_num}"><v>{val}</v></c>'

    xml_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, val in enumerate(row):
            col = col_letters[c_idx] if c_idx < 26 else "A" + col_letters[c_idx - 26]
            if isinstance(val, (int, float)):
                cells.append(_num_cell(val, col, r_idx))
            else:
                cells.append(_str_cell(str(val), col, r_idx))
        xml_rows.append(f'<row r="{r_idx}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        '</worksheet>'
    )
    return sheet_xml, sst


def _build_summary_sheet(summary: dict) -> str:
    rows_xml = []
    for r_idx, (k, v) in enumerate(summary.items(), start=1):
        rows_xml.append(
            f'<row r="{r_idx}">'
            f'<c r="A{r_idx}" t="inlineStr"><is><t>{_escape_xml(str(k))}</t></is></c>'
            f'<c r="B{r_idx}" t="inlineStr"><is><t>{_escape_xml(str(v))}</t></is></c>'
            f'</row>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        '</worksheet>'
    )


def write_xlsx(data_rows: list[list], summary: dict) -> bytes:
    """Build a minimal xlsx with an Import sheet and a Summary sheet."""
    sheet1_xml, sst_strings = _build_data_sheet(data_rows)
    sheet2_xml = _build_summary_sheet(summary)
    sst_xml = _build_shared_strings(sst_strings)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _CONTENT_TYPES)
        zf.writestr("_rels/.rels", _RELS)
        zf.writestr("xl/workbook.xml", _WORKBOOK)
        zf.writestr("xl/_rels/workbook.xml.rels", _WORKBOOK_RELS)
        zf.writestr("xl/worksheets/sheet1.xml", sheet1_xml)
        zf.writestr("xl/worksheets/sheet2.xml", sheet2_xml)
        zf.writestr("xl/sharedStrings.xml", sst_xml)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

IMPORT_FIELDS = [
    "record_id", "source_file", "product_sku", "quantity", "unit_cost", "imported_at",
]


def _generate_file_rows(file_idx: int, rows_per_file: int, seed: int) -> list[dict]:
    rng = random.Random(seed + file_idx * 1000)
    ts = datetime.now(timezone.utc).isoformat()
    return [
        {
            "record_id": f"f{file_idx:03d}-r{i:05d}",
            "source_file": f"import_{file_idx:03d}.csv",
            "product_sku": f"SKU-{rng.randint(1000, 9999)}",
            "quantity": rng.randint(1, 500),
            "unit_cost": round(rng.uniform(0.50, 250.0), 2),
            "imported_at": ts,
        }
        for i in range(rows_per_file)
    ]


# ---------------------------------------------------------------------------
# DailyImportJob
# ---------------------------------------------------------------------------


class DailyImportJob(BaseReportJob):
    """
    Multi-file ingest pipeline.  Each file is processed as a durable step()
    so that a worker failure mid-ingest resumes from the last completed file.

    Stages: ingest → validate → store

    The ingest stage drives one step() per file; step outputs are persisted in
    the StepOutput table so they survive worker restarts without re-ingesting.
    The validate and store stages run once all files are confirmed ingested.
    """

    stages = ["ingest", "validate", "store"]

    # ------------------------------------------------------------------
    # Stage: ingest
    #   Processes each synthetic file as a separate step().
    #   Returns serialisable summary of what was ingested (not the raw records,
    #   so checkpoint state stays small).  Per-file rows are re-generated from
    #   the deterministic seed when building the xlsx in the store stage.
    # ------------------------------------------------------------------

    async def ingest(self, payload: dict, state: dict) -> dict:
        n_files = int(payload.get("n_files", 5))
        rows_per_file = int(payload.get("rows_per_file", 100))
        seed = int(payload.get("seed", 42))
        tenant_id = payload.get("tenant_id", "default")

        file_counts: list[int] = []
        total_records = 0

        for file_idx in range(n_files):
            # Each file is a recoverable step — completed files are not
            # re-processed when the pipeline resumes after a worker crash.
            async def _process_file(
                fi: int = file_idx,
                rpp: int = rows_per_file,
                s: int = seed,
            ) -> dict:
                await asyncio.sleep(0)  # yield to event loop
                rows = _generate_file_rows(fi, rpp, s)
                return {"count": len(rows)}

            result = await step(f"file_{file_idx}", _process_file)
            file_counts.append(result["count"])
            total_records += result["count"]

        return {
            "file_counts": file_counts,
            "total_records": total_records,
            "n_files": n_files,
            "rows_per_file": rows_per_file,
            "tenant_id": tenant_id,
        }

    # ------------------------------------------------------------------
    # Stage: validate
    #   Quality gate: injected errors fail if they exceed 10% of total rows.
    # ------------------------------------------------------------------

    async def validate(self, payload: dict, state: dict) -> dict:
        inject_errors = int(payload.get("inject_errors", 0))
        total_records = state["total_records"]
        error_threshold = max(1, int(total_records * 0.10))

        await asyncio.sleep(0)

        if inject_errors > error_threshold:
            raise ValueError(
                f"Data quality failure: {inject_errors} injected errors "
                f"exceed threshold of {error_threshold} (10% of {total_records} records)"
            )

        valid_count = total_records - inject_errors
        summary = {
            "total_records": total_records,
            "valid_count": valid_count,
            "error_count": inject_errors,
            "error_rate": round(inject_errors / total_records, 4) if total_records else 0,
            "passed": True,
        }
        return {"summary": summary}

    # ------------------------------------------------------------------
    # Stage: store
    #   Re-generates record data from the deterministic seed (no bytes in state),
    #   builds the xlsx in memory, writes it to disk, and returns artifact_path.
    # ------------------------------------------------------------------

    async def store(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)

        n_files = state["n_files"]
        rows_per_file = state["rows_per_file"]
        seed = int(payload.get("seed", 42))
        tenant_id = state["tenant_id"]
        summary = state["summary"]

        # Rebuild records from the deterministic seed (avoids storing bytes in
        # checkpoint state).  Results are identical to what was ingested.
        all_records: list[dict] = []
        for file_idx in range(n_files):
            all_records.extend(_generate_file_rows(file_idx, rows_per_file, seed))

        header = IMPORT_FIELDS
        data_rows: list[list] = [header] + [
            [r.get(f, "") for f in IMPORT_FIELDS] for r in all_records
        ]
        xlsx_bytes = write_xlsx(data_rows, summary)

        base = Path(getattr(settings, "REPORT_OUTPUT_DIR", "/tmp/endure-reports"))
        out_dir = base / tenant_id / "daily_import"
        out_dir.mkdir(parents=True, exist_ok=True)

        today = date.today().isoformat()
        artifact_path = str(out_dir / f"{today}.xlsx")
        Path(artifact_path).write_bytes(xlsx_bytes)

        return {
            "artifact_path": artifact_path,
            "summary": summary,
        }
