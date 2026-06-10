# Synthetic CSV file generator for DailyImportJob evaluation
import csv
import hashlib
import random
from pathlib import Path

PRODUCT_CODES = ["PROD001", "PROD002", "PROD003", "PROD004", "PROD005", "INVALID"]


def generate_csv_files(
    seed: int,
    n_files: int,
    rows_per_file: int,
    inject_errors: int,
    output_dir: Path,
) -> list[Path]:
    """Generate n_files CSV files deterministically. Returns list of file paths.

    Each file has columns: id, product_code, date, quantity, unit_price
    inject_errors total error rows spread across files (nulls, dup IDs, negatives,
    bad codes).
    """
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    errors_remaining = inject_errors

    for file_idx in range(n_files):
        path = output_dir / f"import_{seed}_{file_idx:04d}.csv"
        rows = []
        used_ids: set[int] = set()
        for _row_idx in range(rows_per_file):
            row_id = rng.randint(10000, 99999)
            while row_id in used_ids:
                row_id = rng.randint(10000, 99999)
            used_ids.add(row_id)
            product = rng.choice(PRODUCT_CODES[:5])  # valid by default
            date = f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
            quantity = rng.randint(1, 100)
            price = round(rng.uniform(1.0, 500.0), 2)
            rows.append(
                {
                    "id": row_id,
                    "product_code": product,
                    "date": date,
                    "quantity": quantity,
                    "unit_price": price,
                }
            )

        # inject errors into this file if any remain
        errors_this_file = min(
            errors_remaining,
            max(
                0,
                inject_errors // n_files + (1 if file_idx < inject_errors % n_files else 0),
            ),
        )
        for i in range(errors_this_file):
            error_type = i % 4
            row = rows[rng.randint(0, len(rows) - 1)]
            if error_type == 0:  # null
                row["product_code"] = None
            elif error_type == 1:  # duplicate ID (inject same id as row 0)
                row["id"] = rows[0]["id"]
            elif error_type == 2:  # negative quantity
                row["quantity"] = -abs(row["quantity"])
            else:  # bad product code
                row["product_code"] = "INVALID"
        errors_remaining -= errors_this_file

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["id", "product_code", "date", "quantity", "unit_price"]
            )
            writer.writeheader()
            writer.writerows(rows)
        paths.append(path)
    return paths


def compute_file_hash(path: Path) -> str:
    """SHA-256 hex digest of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
