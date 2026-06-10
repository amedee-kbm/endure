"""
AlertDigestReportJob — submitted on-demand or via API.

Short pipeline (3 stages): validate metric thresholds, render digest, store artifact.
"""

import asyncio
from datetime import datetime, timezone

from src.reporting.generators import data as gen_data
from src.reporting.generators import html as gen_html
from src.reporting.jobs.base import BaseReportJob
from src.reporting.storage import save_artifact


class AlertDigestReportJob(BaseReportJob):
    """
    Payload fields:
      tenant_id  (str)  — tenant identifier
      seed       (int)  — RNG seed for metric generation  [default: 99]
      n_metrics  (int)  — number of metrics to check      [default: 20]
    """

    stages = ["validate", "render", "store"]

    async def validate(self, payload: dict, state: dict) -> dict:
        tenant_id = payload.get("tenant_id", "default")
        seed = int(payload.get("seed", 99))
        n_metrics = int(payload.get("n_metrics", 20))

        await asyncio.sleep(0)
        alerts = gen_data.generate_metric_alerts(tenant_id, seed, n_metrics)
        generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return {"alerts": alerts, "tenant_id": tenant_id, "generated_at": generated_at}

    async def render(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        html_content = gen_html.render_alert_digest(
            state["alerts"],
            tenant_id=state["tenant_id"],
            generated_at=state["generated_at"],
        )
        return {"html_content": html_content}

    async def store(self, payload: dict, state: dict) -> dict:
        # Path is keyed on (tenant_id, generated_at).  generated_at is captured
        # once in validate and preserved in checkpoint state, so a resume that
        # skips validate still writes to the same path.
        await asyncio.sleep(0)
        artifact_path = save_artifact(
            tenant_id=state["tenant_id"],
            report_type="alerts",
            name=state["generated_at"],
            content=state["html_content"],
        )
        return {"artifact_path": artifact_path}
