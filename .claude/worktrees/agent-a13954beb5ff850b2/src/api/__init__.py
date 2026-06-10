from ninja import NinjaAPI

from .routes import admin, events, jobs, metrics, reports, workers

api = NinjaAPI(title="Endure - Distributed Job Scheduler", version="0.1.0")

api.add_router("/v1/jobs", jobs.router)
api.add_router("/v1/workers", workers.router)
api.add_router("/v1/admin", admin.router)
api.add_router("/v1/metrics", metrics.router)
api.add_router("/v1/events", events.router)
api.add_router("/v1/reports", reports.router)
