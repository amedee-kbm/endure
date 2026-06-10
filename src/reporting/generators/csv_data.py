"""Deterministic synthetic CSV generator for DailyImportJob evaluation."""

import csv
import hashlib
import random
from pathlib import Path

PRODUCT_CODES = ["ELEC-001", "CLTH-002", "FOOD-003", "BOOK-004", "SPRT-005"]


def generate_csv_files(
    seed: int,
    n_files: int,
    rows_per_file: int,
    inject_errors: int,
    output_dir: Path,
) -> list[Path]:
    """Generate n_files CSV files deterministically from seed. Return list of paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    # Distribute injected errors across files/rows
    error_positions: set[tuple[int, int]] = set()
    for _ in range(inject_errors):
        f_idx = rng.randint(0, n_files - 1)
        r_idx = rng.randint(0, rows_per_file - 1)
        error_positions.add((f_idx, r_idx))

    paths: list[Path] = []
    for file_idx in range(n_files):
        file_seed = rng.randint(0, 2**32)
        file_rng = random.Random(file_seed)
        path = output_dir / f"import_{seed}_{file_idx:04d}.csv"

        rows = []
        seen_ids: set[str] = set()
        for row_idx in range(rows_per_file):
            is_error = (file_idx, row_idx) in error_positions
            row_id = f"R-{file_idx:04d}-{row_idx:06d}"
            if is_error and seen_ids:
                row_id = next(iter(seen_ids))  # duplicate ID
            seen_ids.add(row_id)

            product_code = file_rng.choice(PRODUCT_CODES)
            quantity = file_rng.randint(1, 100)
            if is_error:
                quantity = -1  # negative quantity

            rows.append({
                "id": row_id,
                "product_code": product_code,
                "date": (
                    f"2026-{file_rng.randint(1, 12):02d}-{file_rng.randint(1, 28):02d}"
                ),
                "quantity": quantity,
                "unit_price": round(file_rng.uniform(1.0, 500.0), 2),
            })

        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=["id", "product_code", "date", "quantity", "unit_price"]
            )
            writer.writeheader()
            writer.writerows(rows)
        paths.append(path)

    return paths


def compute_file_hash(path: Path) -> str:
    """Return SHA-256 hex digest of the file at path."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
