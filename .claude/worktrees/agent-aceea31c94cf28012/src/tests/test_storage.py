"""Unit tests for src.reporting.storage."""
import tempfile
from pathlib import Path

import pytest
from django.test import override_settings

from src.reporting.storage import save_artifact


class TestSaveArtifact:
    def test_saves_html_string_with_default_ext(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-1", "daily_sales", "report_2024", "<html/>")
        assert path.endswith(".html")
        assert Path(path).exists()
        assert Path(path).read_text() == "<html/>"

    def test_saves_xlsx_bytes_with_default_ext(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-1", "daily_import", "import_2024", b"\x50\x4b\x03\x04")
        assert path.endswith(".xlsx")
        assert Path(path).exists()
        assert Path(path).read_bytes() == b"\x50\x4b\x03\x04"

    def test_explicit_ext_overrides_default(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-1", "custom", "myfile", "content", ext="txt")
        assert path.endswith(".txt")

    def test_creates_nested_directories(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-abc", "some_type", "artifact", "data")
        assert (tmp_path / "tenant-abc" / "some_type").is_dir()

    def test_returns_absolute_path_string(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-1", "t", "n", "content")
        assert isinstance(path, str)
        assert Path(path).is_absolute()

    def test_idempotent_overwrite(self, tmp_path):
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            save_artifact("tenant-1", "t", "n", "first")
            path = save_artifact("tenant-1", "t", "n", "second")
        assert Path(path).read_text() == "second"

    def test_html_encoding_utf8(self, tmp_path):
        content = "<html>Ä Ö Ü</html>"
        with override_settings(REPORT_OUTPUT_DIR=str(tmp_path)):
            path = save_artifact("tenant-1", "t", "n", content)
        assert Path(path).read_text(encoding="utf-8") == content
