"""openpyxl Excel renderer for DailyImportJob reports."""

import io
from typing import Any

import openpyxl
from openpyxl.styles import Font


def render_excel(
    quality: dict[str, Any],
    valid_records: list[dict],
    error_rows: list[dict],
) -> bytes:
    """Render a two-sheet xlsx workbook and return raw bytes.

    Sheet 1 — Quality Summary: totals, pass/fail counts per check.
    Sheet 2 — Exception Detail: error rows with error annotations.
    """
    wb = openpyxl.Workbook()

    # Sheet 1: Quality Summary
    ws1 = wb.active
    ws1.title = "Quality Summary"
    ws1.append(["Metric", "Value"])
    ws1.append(["Total Records", quality.get("total_records", 0)])
    ws1.append(["Valid Records", quality.get("valid_count", len(valid_records))])
    ws1.append(["Error Records", quality.get("error_count", len(error_rows))])
    ws1.append(["Error Rate (%)", quality.get("error_rate_pct", 0)])
    ws1.append(["Null Field Errors", quality.get("null_errors", 0)])
    ws1.append(["Duplicate ID Errors", quality.get("duplicate_id_errors", 0)])
    ws1.append(["Negative Quantity Errors", quality.get("negative_quantity_errors", 0)])
    ws1.append(["Invalid Product Code Errors", quality.get("invalid_code_errors", 0)])
    for cell in ws1[1]:
        cell.font = Font(bold=True)

    # Sheet 2: Exception Detail
    ws2 = wb.create_sheet("Exception Detail")
    if error_rows:
        headers = list(error_rows[0].keys())
        ws2.append(headers)
        for cell in ws2[1]:
            cell.font = Font(bold=True)
        for row in error_rows:
            ws2.append([row.get(h) for h in headers])
    else:
        ws2.append(["No errors found"])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
