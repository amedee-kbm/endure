import base64
import tempfile
from datetime import date as date_type
from pathlib import Path

from src.framework.pipeline import Pipeline
from src.framework.step import step
from src.reporting.generators.csv_data import compute_file_hash, generate_csv_files
from src.reporting.generators.excel import render_excel
from src.reporting.storage import save_artifact

ERROR_RATE_THRESHOLD = 0.10  # fail if >10% of records are errors


class DailyImportJob(Pipeline):
    stages = ["discover", "ingest", "validate", "transform", "report", "archive"]
    schedule = "0 6 * * *"
    timeout = 1800

    async def discover(self, payload: dict, state: dict) -> dict:
        from src.models import SourceFile

        tenant_id = payload["tenant_id"]
        date_str = payload.get("date", str(date_type.today()))
        n_files = payload.get("n_files", 20)
        rows_per_file = payload.get("rows_per_file", 500)
        seed = payload.get("seed", 42)
        inject_errors = payload.get("inject_errors", 5)

        tmp_dir = Path(tempfile.gettempdir()) / f"endure_{tenant_id}_{date_str}_{seed}"
        paths = generate_csv_files(seed, n_files, rows_per_file, inject_errors, tmp_dir)

        # Filter out files already recorded in SourceFile by hash
        new_paths = []
        for path in paths:
            file_hash = compute_file_hash(path)
            already_processed = await SourceFile.objects.filter(
                tenant_id=tenant_id, file_hash=file_hash
            ).aexists()
            if not already_processed:
                new_paths.append(str(path))

        return {"files": new_paths, "file_count": len(new_paths), "date": date_str}

    async def ingest(self, payload: dict, state: dict) -> dict:
        files = state.get("files", [])
        all_records: list[dict] = []
        for i, path_str in enumerate(files):
            records = await step(f"file_{i}", _read_csv, path_str)
            all_records.extend(records)
        return {"records": all_records, "total_records": len(all_records)}

    async def validate(self, payload: dict, state: dict) -> dict:
        records = state.get("records", [])
        valid_records: list[dict] = []
        error_rows: list[dict] = []
        null_count = duplicate_count = negative_count = invalid_code_count = 0
        seen_ids: dict = {}
        valid_codes = {"PROD001", "PROD002", "PROD003", "PROD004", "PROD005"}

        for r in records:
            errors: list[str] = []
            product = r.get("product_code")
            if product is None or product == "":
                null_count += 1
                errors.append("null_product_code")
            elif product not in valid_codes:
                invalid_code_count += 1
                errors.append("invalid_product_code")
            rid = r.get("id")
            if rid in seen_ids:
                duplicate_count += 1
                errors.append("duplicate_id")
            else:
                seen_ids[rid] = True
            qty = r.get("quantity", 0)
            try:
                qty_num = float(qty) if qty is not None else 0
            except (TypeError, ValueError):
                qty_num = 0
            if qty_num < 0:
                negative_count += 1
                errors.append("negative_quantity")
            if errors:
                error_rows.append({**r, "_errors": ",".join(errors)})
            else:
                valid_records.append(r)

        total = len(records)
        error_count = len(error_rows)
        error_rate = error_count / total if total > 0 else 0.0
        quality = {
            "total_records": total,
            "valid_count": len(valid_records),
            "error_count": error_count,
            "file_count": state.get("file_count", 0),
            "null_count": null_count,
            "duplicate_count": duplicate_count,
            "negative_count": negative_count,
            "invalid_code_count": invalid_code_count,
            "error_rate": error_rate,
            "passed": error_rate <= ERROR_RATE_THRESHOLD,
        }
        if not quality["passed"]:
            raise ValueError(
                f"Data quality failed: error rate {error_rate:.1%} > "
                f"threshold {ERROR_RATE_THRESHOLD:.0%}"
            )
        return {
            "quality": quality,
            "valid_records": valid_records,
            "error_rows": error_rows,
        }

    async def transform(self, payload: dict, state: dict) -> dict:
        records = state.get("valid_records", [])
        transformed = []
        for r in records:
            t: dict = {}
            for k, v in r.items():
                snake_k = k.lower().replace(" ", "_")
                if snake_k == "date":
                    v = str(v)
                elif snake_k == "quantity":
                    try:
                        v = int(v) if v is not None else None
                    except (TypeError, ValueError):
                        v = None
                elif snake_k == "unit_price":
                    try:
                        v = float(v) if v is not None else None
                    except (TypeError, ValueError):
                        v = None
                t[snake_k] = v
            transformed.append(t)
        return {"transformed": transformed}

    async def report(self, payload: dict, state: dict) -> dict:
        quality = state.get("quality", {})
        valid_records = state.get("valid_records", [])
        error_rows = state.get("error_rows", [])
        xlsx_bytes = render_excel(quality, valid_records, error_rows)
        # Base64-encode so it's JSON-serializable in checkpoint state
        summary_keys = ("total_records", "valid_count", "error_count", "error_rate", "passed")
        return {
            "xlsx_bytes_b64": base64.b64encode(xlsx_bytes).decode("ascii"),
            "summary": {k: quality.get(k) for k in summary_keys},
        }

    async def archive(self, payload: dict, state: dict) -> dict:
        from src.models import Job, SourceFile

        tenant_id = payload["tenant_id"]
        date_str = state.get("date", payload.get("date", str(date_type.today())))
        xlsx_bytes = base64.b64decode(state["xlsx_bytes_b64"])
        artifact_path = save_artifact(
            tenant_id,
            "daily_import",
            f"daily_import_{date_str}",
            xlsx_bytes,
            ext="xlsx",
        )

        # Write SourceFile rows for each processed file
        from src.framework.context import _current_job_id

        job_id = _current_job_id.get()
        job = None
        if job_id:
            try:
                job = await Job.objects.aget(id=job_id)
            except Job.DoesNotExist:
                pass

        for path_str in state.get("files", []):
            path = Path(path_str)
            file_hash = compute_file_hash(path)
            await SourceFile.objects.aupdate_or_create(
                tenant_id=tenant_id,
                file_hash=file_hash,
                defaults={"file_name": path.name, "job": job},
            )

        return {"artifact_path": artifact_path}


async def _read_csv(path_str: str) -> list[dict]:
    import csv

    records = []
    with open(path_str, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records
