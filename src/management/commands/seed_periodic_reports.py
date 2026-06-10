"""
management command: seed_periodic_reports

Creates (or updates) the PeriodicTask row for DailyImportJob:
  - DailyImportJob — daily at 06:00 UTC (cron: 0 6 * * *)

Usage:
  python manage.py seed_periodic_reports --tenant-id <uuid>
"""

from datetime import datetime, timezone

import croniter
from django.core.management.base import BaseCommand, CommandError

from src.models import PeriodicTask, Tenant

REPORT_TASKS = [
    {
        "name": "daily-import-report",
        "job_type": "src.reporting.jobs.daily_import:DailyImportJob",
        "cron_expression": "0 6 * * *",
        "payload": {
            "n_files": 20,
            "rows_per_file": 500,
            "seed": 42,
            "inject_errors": 5,
        },
    },
]


class Command(BaseCommand):
    help = "Seed PeriodicTask rows for scheduled report jobs."

    def add_arguments(self, parser):
        parser.add_argument(
            "--tenant-id",
            required=True,
            help="UUID of the tenant to own the periodic tasks",
        )

    def handle(self, *args, **options):
        tenant_id = options["tenant_id"]
        try:
            tenant = Tenant.objects.get(id=tenant_id)
        except Tenant.DoesNotExist:
            raise CommandError(f"Tenant {tenant_id!r} not found.")

        now = datetime.now(timezone.utc)

        for task_def in REPORT_TASKS:
            cron = croniter.croniter(task_def["cron_expression"], now)
            next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)

            payload = {**task_def["payload"], "tenant_id": str(tenant.id)}

            obj, created = PeriodicTask.objects.update_or_create(
                name=task_def["name"],
                tenant=tenant,
                defaults={
                    "job_type": task_def["job_type"],
                    "cron_expression": task_def["cron_expression"],
                    "payload": payload,
                    "is_active": True,
                    "next_run_at": next_run,
                },
            )

            status = "created" if created else "updated"
            self.stdout.write(
                self.style.SUCCESS(
                    f"[{status}] {task_def['name']} — next run: {next_run.isoformat()}"
                )
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nSeeded {len(REPORT_TASKS)} periodic report task(s) for tenant {tenant.name}."
            )
        )
