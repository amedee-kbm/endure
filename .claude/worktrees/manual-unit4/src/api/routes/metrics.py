from datetime import datetime, timedelta, timezone

from django.db.models import Avg, Count, FloatField, Sum
from django.db.models.expressions import RawSQL
from ninja import Router

from src.constants import JobState, WorkerState
from src.models import Checkpoint, DeadLetterJob, Job, Worker
from src.queue.redis_queue import redis_queue

router = Router()


@router.get("")
async def get_metrics(request):
    now = datetime.now(timezone.utc)

    # Job state counts
    job_state_counts: dict[str, int] = {}
    async for row in Job.objects.values("state").annotate(count=Count("id")):
        job_state_counts[row["state"]] = row["count"]
    total_jobs = sum(job_state_counts.values())

    # Avg job latency (created → completed)
    latency_agg = await Job.objects.filter(
        state=JobState.COMPLETED, completed_at__isnull=False
    ).aaggregate(
        avg_latency=Avg(
            RawSQL(
                "EXTRACT(epoch FROM completed_at) - EXTRACT(epoch FROM created_at)",
                [],
                output_field=FloatField(),
            )
        )
    )
    avg_latency = latency_agg.get("avg_latency")

    # Avg queue wait time (created → started)
    wait_agg = await Job.objects.filter(started_at__isnull=False).aaggregate(
        avg_wait=Avg(
            RawSQL(
                "EXTRACT(epoch FROM started_at) - EXTRACT(epoch FROM created_at)",
                [],
                output_field=FloatField(),
            )
        )
    )
    avg_wait_time = wait_agg.get("avg_wait")

    # Jobs completed in last hour
    one_hour_ago = now - timedelta(hours=1)
    jobs_completed_last_hour = await Job.objects.filter(
        state=JobState.COMPLETED, completed_at__gte=one_hour_ago
    ).acount()

    # Failure rate
    failed_count = job_state_counts.get("FAILED", 0) + job_state_counts.get("DEAD_LETTER", 0)
    completed_count = job_state_counts.get("COMPLETED", 0)
    failure_rate = (
        failed_count / (failed_count + completed_count)
        if (failed_count + completed_count) > 0
        else 0
    )

    # Queue depth
    queue_depth = await redis_queue.queue_length()

    # Worker metrics grouped by state
    worker_stats: dict[str, dict] = {}
    total_capacity = 0
    total_load = 0
    async for row in Worker.objects.values("state").annotate(
        count=Count("id"),
        load=Sum("inflight_job_count"),
        capacity=Sum("max_inflight_jobs"),
    ):
        worker_stats[row["state"]] = {
            "count": row["count"],
            "current_load": row["load"] or 0,
            "max_slots": row["capacity"] or 0,
        }
        if row["state"] == WorkerState.ONLINE:
            total_capacity = row["capacity"] or 0
            total_load = row["load"] or 0

    utilization = total_load / total_capacity if total_capacity > 0 else 0

    # Per-tenant job counts by state
    tenant_metrics: dict[str, dict] = {}
    async for row in Job.objects.values("tenant_id", "state").annotate(count=Count("id")):
        tid = str(row["tenant_id"])
        tenant_metrics.setdefault(tid, {})
        tenant_metrics[tid][row["state"]] = row["count"]

    # Dead letter count
    dead_letter_count = await DeadLetterJob.objects.acount()

    # Checkpoint metrics
    checkpoint_agg = await Checkpoint.objects.aaggregate(
        total=Count("id"), total_bytes=Sum("size_bytes")
    )
    total_checkpoints = checkpoint_agg.get("total") or 0
    total_checkpoint_bytes = checkpoint_agg.get("total_bytes") or 0

    return {
        "timestamp": now.isoformat(),
        "jobs": {
            "total": total_jobs,
            "by_state": job_state_counts,
            "avg_latency_seconds": round(avg_latency, 2) if avg_latency else None,
            "avg_wait_time_seconds": round(avg_wait_time, 2) if avg_wait_time else None,
            "throughput_last_hour": jobs_completed_last_hour,
            "failure_rate": round(failure_rate, 4),
        },
        "queue": {
            "depth": queue_depth,
        },
        "workers": {
            "by_state": worker_stats,
            "total_capacity": total_capacity,
            "total_load": total_load,
            "utilization": round(utilization, 4),
        },
        "tenants": tenant_metrics,
        "dead_letter": {
            "total": dead_letter_count,
        },
        "checkpoints": {
            "total": total_checkpoints,
            "total_size_bytes": total_checkpoint_bytes,
        },
    }
