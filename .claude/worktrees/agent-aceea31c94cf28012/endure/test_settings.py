"""
Minimal Django settings for unit tests — no DB env vars required.
Uses an in-memory SQLite database so no infrastructure is needed.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "test-insecure-key-for-unit-tests-only"
DEBUG = True
ALLOWED_HOSTS: list[str] = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "src",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"

# Reporting
REPORT_OUTPUT_DIR = "/tmp/endure-test-reports"

# Worker / scheduler (not used in unit tests but prevent import errors)
USE_PROCESS_ISOLATION = False
CHECKPOINT_DIR = "/tmp/endure-test-checkpoints"
CHECKPOINT_INTERVAL = 30.0
