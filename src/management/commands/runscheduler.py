import asyncio

from django.core.management.base import BaseCommand

from src.scheduler.scheduler import Scheduler


class Command(BaseCommand):
    help = "Run the endure scheduler loop"

    def handle(self, *args, **options):
        asyncio.run(Scheduler().start())
