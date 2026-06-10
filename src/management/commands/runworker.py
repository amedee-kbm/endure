import asyncio

from django.core.management.base import BaseCommand

from src.worker.worker import WorkerNode


class Command(BaseCommand):
    help = "Run an endure worker node"

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-inflight-jobs",
            type=int,
            default=None,
            help="Override WORKER_MAX_INFLIGHT_JOBS for this worker",
        )

    def handle(self, *args, **options):
        asyncio.run(WorkerNode(max_inflight_jobs=options["max_inflight_jobs"]).start())
