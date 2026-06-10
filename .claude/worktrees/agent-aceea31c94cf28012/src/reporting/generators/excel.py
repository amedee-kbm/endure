# Excel report renderer using openpyxl
import io

from openpyxl import Workbook


def render_excel(quality: dict, valid_records: list, error_rows: list) -> bytes:
    """Render two-sheet Excel workbook. Returns raw bytes."""
    wb = Workbook()

    # Sheet 1: Quality Summary
    ws1 = wb.active
    ws1.title = "Quality Summary"
    ws1.append(["Metric", "Value"])
    ws1.append(["Total Records", quality.get("total_records", 0)])
    ws1.append(["Valid Records", quality.get("valid_count", 0)])
    ws1.append(["Error Rows", quality.get("error_count", 0)])
    ws1.append(["File Count", quality.get("file_count", 0)])
    ws1.append(["Null Values", quality.get("null_count", 0)])
    ws1.append(["Duplicate IDs", quality.get("duplicate_count", 0)])
    ws1.append(["Negative Quantities", quality.get("negative_count", 0)])
    ws1.append(["Invalid Product Codes", quality.get("invalid_code_count", 0)])
    ws1.append(["Pass", str(quality.get("passed", False))])

    # Sheet 2: Exception Detail
    ws2 = wb.create_sheet("Exception Detail")
    if error_rows:
        headers = list(error_rows[0].keys())
        ws2.append(headers)
        for row in error_rows:
            ws2.append([row.get(h) for h in headers])
    else:
        ws2.append(["No errors"])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
