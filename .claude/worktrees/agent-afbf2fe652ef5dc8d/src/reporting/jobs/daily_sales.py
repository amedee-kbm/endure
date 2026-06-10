"""
DailySalesReportJob — runs daily via PeriodicTask.

Scheduler features exercised:
  - PeriodicTask (cron: 0 6 * * *)
  - Checkpoint / resume (Pipeline stage skipping)
  - Step-level checkpointing (step() in aggregate)
  - Data quality validation (validate stage)
"""

import asyncio
from datetime import date

from endure import step
from src.reporting.generators import data as gen_data
from src.reporting.generators import html as gen_html
from src.reporting.jobs.base import BaseReportJob
from src.reporting.storage import save_artifact


class DailySalesReportJob(BaseReportJob):
    """
    Payload fields:
      tenant_id      (str)   — tenant identifier
      date           (str)   — ISO-8601 date, e.g. "2026-06-03"  [default: today]
      seed           (int)   — RNG seed for deterministic fake data [default: 42]
      n_orders       (int)   — number of fake orders to generate   [default: 200]
      inject_errors  (int)   — number of duplicate order IDs to inject for testing [default: 0]
    """

    stages = ["extract", "validate", "aggregate", "render", "store"]

    async def extract(self, payload: dict, state: dict) -> dict:
        tenant_id = payload.get("tenant_id", "default")
        report_date = payload.get("date", date.today().isoformat())
        seed = int(payload.get("seed", 42))
        n_orders = int(payload.get("n_orders", 200))

        await asyncio.sleep(0)
        orders = gen_data.generate_sales_data(tenant_id, report_date, seed, n_orders)

        inject = int(payload.get("inject_errors", 0))
        for i in range(inject):
            if orders:
                duplicate = dict(orders[i % len(orders)])
                orders.append(duplicate)

        return {"orders": orders, "tenant_id": tenant_id, "report_date": report_date,
                "n_orders": n_orders}

    async def validate(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        quality_summary = gen_data.validate_sales_data(
            state["orders"],
            expected_count=state["n_orders"],
        )
        error_threshold = max(1, int(state["n_orders"] * 0.1))
        if quality_summary["error_count"] > error_threshold:
            raise ValueError(
                f"Data quality failure: {quality_summary['error_count']} errors "
                f"exceed threshold of {error_threshold}"
            )
        return {"quality_summary": quality_summary}

    async def aggregate(self, payload: dict, state: dict) -> dict:
        orders = state["orders"]

        async def _group_by_product() -> dict:
            await asyncio.sleep(1.5)
            by_product: dict[str, float] = {}
            by_category: dict[str, float] = {}
            by_hour: dict[int, float] = {}
            for o in orders:
                by_product[o["product"]] = round(by_product.get(o["product"], 0) + o["revenue"], 2)
                by_category[o["category"]] = round(by_category.get(o["category"], 0) + o["revenue"], 2)
                by_hour[o["hour"]] = round(by_hour.get(o["hour"], 0) + o["revenue"], 2)
            return {
                "total_revenue": round(sum(o["revenue"] for o in orders), 2),
                "total_orders": len(orders),
                "by_category": dict(sorted(by_category.items(), key=lambda x: x[1], reverse=True)),
                "by_hour": {str(h): v for h, v in sorted(by_hour.items())},
                "by_product": by_product,
            }

        async def _apply_fx(grouped: dict) -> dict:
            await asyncio.sleep(1.5)
            top_products = sorted(
                grouped["by_product"].items(), key=lambda x: x[1], reverse=True
            )[:5]
            result = {k: v for k, v in grouped.items() if k != "by_product"}
            result["top_products"] = [{"product": p, "revenue": r} for p, r in top_products]
            return result

        grouped = await step("group_by_product", _group_by_product)
        aggregated = await step("apply_fx", _apply_fx, grouped)
        return {"aggregated": aggregated}

    async def render(self, payload: dict, state: dict) -> dict:
        await asyncio.sleep(0)
        html_content = gen_html.render_sales_report(
            state["aggregated"],
            tenant_id=state["tenant_id"],
            report_date=state["report_date"],
            quality_summary=state.get("quality_summary"),
        )
        return {"html_content": html_content}

    async def store(self, payload: dict, state: dict) -> dict:
        # Path is keyed on (tenant_id, report_date); seeded generation makes
        # content deterministic, so a re-run overwrites with an identical file.
        await asyncio.sleep(0)
        artifact_path = save_artifact(
            tenant_id=state["tenant_id"],
            report_type="sales",
            name=state["report_date"],
            content=state["html_content"],
        )
        return {"artifact_path": artifact_path}
