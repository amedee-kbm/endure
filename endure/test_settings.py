from endure.settings import *  # noqa: F401, F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

USE_PROCESS_ISOLATION = False
CHECKPOINT_DIR = "/tmp/endure-test-checkpoints"
