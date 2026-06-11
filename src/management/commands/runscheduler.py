import asyncio
import logging

from django.core.management.base import BaseCommand

from src.scheduler.scheduler import Scheduler


class Command(BaseCommand):
    help = "Run the endure scheduler loop"

    def handle(self, *args, **options):
        # Without an explicit handler only WARNING+ reaches stderr via the
        # lastResort fallback; assignments and failovers log at INFO and were
        # invisible in container logs.
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        asyncio.run(Scheduler().start())
