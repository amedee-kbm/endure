"""
BaseReportJob — alias for Pipeline, the miniframework base class.

Kept for import compatibility; new code should import Pipeline directly:

    from endure import Pipeline, step
"""

from src.framework.pipeline import Pipeline

BaseReportJob = Pipeline

__all__ = ["BaseReportJob", "Pipeline"]
