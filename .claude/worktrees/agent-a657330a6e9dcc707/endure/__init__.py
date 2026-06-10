"""
Endure miniframework public API.

Usage::

    from endure import Pipeline, step

    class MyReport(Pipeline):
        stages = ["extract", "validate", "aggregate", "render", "store"]

        async def extract(self, payload: dict, ctx: dict) -> dict:
            data = await step("fetch_rows", fetch_from_db, payload["date"])
            return {"rows": data}

        async def aggregate(self, payload: dict, ctx: dict) -> dict:
            summary = await step("group_by_product", group, ctx["rows"])
            return {"summary": summary}
        ...
"""
from src.framework.pipeline import Pipeline, BaseReportJob
from src.framework.step import step

__all__ = ["Pipeline", "BaseReportJob", "step"]
