"""DailyImportJob — 6-stage ETL pipeline for CSV file ingestion."""

import csv
import logging
import tempfile
from datetime import date
from pathlib import Path

from src.framework.pipeline import Pipeline
from src.framework.step import step
from src.reporting.generators.csv_data import compute_file_hash, generate_csv_files
from src.reporting.generators.excel import render_excel
from src.reporting.storage import save_artifact

logger = logging.getLogger("endure.reporting.daily_import")

ERROR_RATE_THRESHOLD = 0.10  # 10%
VALID_PRODUCT_CODES = {"ELEC-001", "CLTH-002", "FOOD-003", "BOOK-004", "SPRT-005"}


async def _read_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(dict(row))
    return rows


class DailyImportJob(Pipeline):
    stages = ["discover", "ingest", "validate", "transform", "report", "archive"]
    schedule = "0 6 * * *"
    timeout = 1800

    async def discover(self, payload: dict, state: dict) -> dict:
        from src.models import SourceFile

        tenant_id = payload["tenant_id"]
        report_date = payload.get("date", date.today().isoformat())
        n_files = payload.get("n_files", 20)
        rows_per_file = payload.get("rows_per_file", 500)
        seed = payload.get("seed", 42)
        inject_errors = payload.get("inject_errors", 5)

        tmp_dir = (
            Path(tempfile.gettempdir())
            / f"endure-import-{tenant_id}-{report_date}-{seed}"
        )
        csv_paths = generate_csv_files(
            seed=seed,
            n_files=n_files,
            rows_per_file=rows_per_file,
            inject_errors=inject_errors,
            output_dir=tmp_dir,
        )

        # Filter out files already processed in a previous job (cross-job idempotency)
        new_files: list[str] = []
        for path in csv_paths:
            file_hash = compute_file_hash(path)
            already_processed = await SourceFile.objects.filter(
                tenant_id=tenant_id, file_hash=file_hash
            ).aexists()
            if not already_processed:
                new_files.append(str(path))

        logger.info(f"discover: {len(new_files)}/{len(csv_paths)} new files")
        return {"files": new_files, "file_count": len(new_files)}

    async def ingest(self, payload: dict, state: dict) -> dict:
        files = state.get("files", [])
        all_records: list[dict] = []
        for i, path in enumerate(files):
            records = await step(f"file_{i}", _read_csv, path)
            all_records.extend(records)
            logger.debug(f"ingest: file_{i} ({Path(path).name}) → {len(records)} rows")
        return {"records": all_records, "total_records": len(all_records)}

    async def validate(self, payload: dict, state: dict) -> dict:
        records = state.get("records", [])
        rows_per_file = payload.get("rows_per_file", 500)
        file_count = state.get("file_count", payload.get("n_files", 20))
        expected_total = rows_per_file * file_count

        null_errors = 0
        duplicate_id_errors = 0
        negative_quantity_errors = 0
        invalid_code_errors = 0
        valid_records: list[dict] = []
        error_rows: list[dict] = []
        seen_ids: set[str] = set()

        for row in records:
            row_errors: list[str] = []

            for field in ("id", "product_code", "date", "quantity", "unit_price"):
                if not row.get(field):
                    row_errors.append(f"null_{field}")
                    null_errors += 1

            row_id = row.get("id", "")
            if row_id in seen_ids:
                row_errors.append("duplicate_id")
                duplicate_id_errors += 1
            seen_ids.add(row_id)

            try:
                qty = int(row.get("quantity", 1))
                if qty < 0:
                    row_errors.append("negative_quantity")
                    negative_quantity_errors += 1
            except (ValueError, TypeError):
                row_errors.append("invalid_quantity")
                negative_quantity_errors += 1

            if row.get("product_code") not in VALID_PRODUCT_CODES:
                row_errors.append("invalid_product_code")
                invalid_code_errors += 1

            if row_errors:
                error_rows.append({**row, "_errors": ",".join(row_errors)})
            else:
                valid_records.append(row)

        total_records = len(records)
        error_count = len(error_rows)
        error_rate = error_count / total_records if total_records else 0.0

        quality = {
            "total_records": total_records,
            "valid_count": len(valid_records),
            "error_count": error_count,
            "error_rate_pct": round(error_rate * 100, 2),
            "null_errors": null_errors,
            "duplicate_id_errors": duplicate_id_errors,
            "negative_quantity_errors": negative_quantity_errors,
            "invalid_code_errors": invalid_code_errors,
            "expected_total": expected_total,
        }

        if error_rate > ERROR_RATE_THRESHOLD:
            raise ValueError(
                f"Data quality error rate {error_rate:.1%} exceeds threshold "
                f"{ERROR_RATE_THRESHOLD:.1%}; aborting pipeline."
            )

        return {"quality": quality, "valid_records": valid_records, "error_rows": error_rows}

    async def transform(self, payload: dict, state: dict) -> dict:
        valid_records = state.get("valid_records", [])
        transformed: list[dict] = []
        for row in valid_records:
            t = {k.lower().replace(" ", "_"): v for k, v in row.items()}
            try:
                t["quantity"] = int(t.get("quantity", 0))
            except (ValueError, TypeError):
                t["quantity"] = 0
            try:
                t["unit_price"] = float(t.get("unit_price", 0.0))
            except (ValueError, TypeError):
                t["unit_price"] = 0.0
            t["date"] = str(t.get("date", "")).strip()
            transformed.append(t)
        return {"transformed": transformed}

    async def report(self, payload: dict, state: dict) -> dict:
        quality = state.get("quality", {})
        return {
            "summary": {
                "total_records": quality.get("total_records", 0),
                "valid_count": quality.get("valid_count", 0),
                "error_count": quality.get("error_count", 0),
                "error_rate_pct": quality.get("error_rate_pct", 0),
            }
        }

    async def archive(self, payload: dict, state: dict) -> dict:
        from src.models import SourceFile

        tenant_id = payload["tenant_id"]
        report_date = payload.get("date", date.today().isoformat())
        quality = state.get("quality", {})
        valid_records = state.get("valid_records", [])
        error_rows = state.get("error_rows", [])
        files = state.get("files", [])

        xlsx_bytes = render_excel(quality, valid_records, error_rows)
        artifact_path = save_artifact(
            tenant_id=tenant_id,
            report_type="daily_import",
            name=f"daily_import_{report_date}",
            content=xlsx_bytes,
            ext="xlsx",
        )

        for file_path_str in files:
            path = Path(file_path_str)
            if path.exists():
                file_hash = compute_file_hash(path)
                try:
                    await SourceFile.objects.aget_or_create(
                        tenant_id=tenant_id,
                        file_hash=file_hash,
                        defaults={"file_name": path.name},
                    )
                except Exception as exc:
                    logger.warning(f"SourceFile upsert failed for {path.name}: {exc}")

        logger.info(f"archive: artifact saved to {artifact_path}")
        return {"artifact_path": artifact_path}
