"""Unit tests for src.reporting.generators.csv_data."""
import csv
import tempfile
from pathlib import Path

import pytest

from src.reporting.generators.csv_data import (
    PRODUCT_CODES,
    compute_file_hash,
    generate_csv_files,
)


class TestGenerateCsvFiles:
    def test_returns_correct_number_of_files(self, tmp_path):
        paths = generate_csv_files(seed=1, n_files=3, rows_per_file=10, inject_errors=0, output_dir=tmp_path)
        assert len(paths) == 3

    def test_files_exist_on_disk(self, tmp_path):
        paths = generate_csv_files(seed=2, n_files=2, rows_per_file=5, inject_errors=0, output_dir=tmp_path)
        for p in paths:
            assert p.exists()

    def test_file_has_correct_columns(self, tmp_path):
        paths = generate_csv_files(seed=3, n_files=1, rows_per_file=5, inject_errors=0, output_dir=tmp_path)
        with open(paths[0], newline="") as f:
            reader = csv.DictReader(f)
            cols = reader.fieldnames
        assert cols == ["id", "product_code", "date", "quantity", "unit_price"]

    def test_file_has_correct_row_count(self, tmp_path):
        paths = generate_csv_files(seed=4, n_files=1, rows_per_file=20, inject_errors=0, output_dir=tmp_path)
        with open(paths[0], newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 20

    def test_deterministic_with_same_seed(self, tmp_path):
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        paths_a = generate_csv_files(seed=10, n_files=2, rows_per_file=10, inject_errors=0, output_dir=dir_a)
        paths_b = generate_csv_files(seed=10, n_files=2, rows_per_file=10, inject_errors=0, output_dir=dir_b)
        for pa, pb in zip(paths_a, paths_b):
            assert pa.read_text() == pb.read_text()

    def test_inject_errors_adds_bad_rows(self, tmp_path):
        # With 0 errors: all rows should have valid product codes (from PRODUCT_CODES[:5])
        paths_clean = generate_csv_files(seed=5, n_files=1, rows_per_file=50, inject_errors=0, output_dir=tmp_path / "clean")
        with open(paths_clean[0], newline="") as f:
            clean_rows = list(csv.DictReader(f))
        valid_codes = set(PRODUCT_CODES[:5])
        for row in clean_rows:
            assert row["product_code"] in valid_codes

    def test_output_dir_created_if_missing(self, tmp_path):
        new_dir = tmp_path / "nested" / "dir"
        paths = generate_csv_files(seed=6, n_files=1, rows_per_file=5, inject_errors=0, output_dir=new_dir)
        assert new_dir.exists()
        assert len(paths) == 1


class TestComputeFileHash:
    def test_returns_hex_string_of_length_64(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("id,name\n1,foo\n")
        h = compute_file_hash(p)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_content_same_hash(self, tmp_path):
        content = "id,name\n1,foo\n2,bar\n"
        p1 = tmp_path / "a.csv"
        p2 = tmp_path / "b.csv"
        p1.write_text(content)
        p2.write_text(content)
        assert compute_file_hash(p1) == compute_file_hash(p2)

    def test_different_content_different_hash(self, tmp_path):
        p1 = tmp_path / "a.csv"
        p2 = tmp_path / "b.csv"
        p1.write_text("id,name\n1,foo\n")
        p2.write_text("id,name\n1,bar\n")
        assert compute_file_hash(p1) != compute_file_hash(p2)

    def test_hash_matches_generated_file(self, tmp_path):
        """Hash of generated files should be stable with fixed seed."""
        paths = generate_csv_files(seed=99, n_files=1, rows_per_file=10, inject_errors=0, output_dir=tmp_path)
        h1 = compute_file_hash(paths[0])
        h2 = compute_file_hash(paths[0])
        assert h1 == h2
