"""
WeeklyActivityReportJob — runs weekly via PeriodicTask.

Scheduler features exercised:
  - PeriodicTask (cron: 0 7 * * 1)
  - Data quality validation (validate stage)
"""

import asyncio
from datetime import date, timedelta

from src.reporting.generators import data as gen_data
from src.reporting.generators import html as gen_html
from src.reporting.jobs.base import BaseReportJob
from src.reporting.storage import save_artifact


class WeeklyActivityReportJob(BaseReportJob):
    """
    Payload fields:
      tenant_id   (str)  — tenant identifier
      week_start  (str)  — ISO-8601 Monday date  [default: last Monday]
      seed        (int)  — RNG seed              [default: 7]
      n_sessions  (int)  — fake session count    [default: 500]
    """

    stages = ["extract", "validate", "aggregate", "render", "store"]

    async def extract(self, payload: dict, state: dict) -> dict:
        tenant_id = payload.get("tenant_id", "default")
        today = date.today()
        default_monday = (today - timedelta(days=today.weekday())).isoformat()
        week_start = payload.get("week_start", default_monday)
        seed = int(payload.get("seed", 7))
        n_sessions = int(payload.get("n_sessions", 500))

        await asyncio.sleep(0)
        sessions = gen_data.generate_session_data(tenant_id, week_start, seed, n_sessions)
        return {"sessions": sessions, "tenant_id": tenant_id, "week_start": week_start,
                "n_sessions": n_sessions}

    async def validate(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        quality_summary = gen_data.validate_session_data(
            state["sessions"],
            expected_count=state["n_sessions"],
        )
        error_threshold = max(1, int(state["n_sessions"] * 0.1))
        if quality_summary["error_count"] > error_threshold:
            raise ValueError(
                f"Data quality failure: {quality_summary['error_count']} errors "
                f"exceed threshold of {error_threshold}"
            )
        return {"quality_summary": quality_summary}

    async def aggregate(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        aggregated = gen_data.aggregate_sessions(state["sessions"])
        return {"aggregated": aggregated}

    async def render(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        html_content = gen_html.render_activity_report(
            state["aggregated"],
            tenant_id=state["tenant_id"],
            week_start=state["week_start"],
            quality_summary=state.get("quality_summary"),
        )
        return {"html_content": html_content}

    async def store(self, payload: dict, state: dict) -> dict:
        # Path is keyed on (tenant_id, week_start); seeded generation makes
        # content deterministic, so a re-run overwrites with an identical file.
        await asyncio.sleep(0)
        artifact_path = save_artifact(
            tenant_id=state["tenant_id"],
            report_type="activity",
            name=state["week_start"],
            content=state["html_content"],
        )
        return {"artifact_path": artifact_path}
