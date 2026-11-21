from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Optional

from adapters import DatabaseAdapter, ExecutionSample
from history_store import LearningAssessment, RollingHistoryStore

logger = logging.getLogger("sql_query_profiler")


class ScopedTimer:
    """Context manager that measures elapsed execution time in microseconds."""

    def __init__(
        self,
        name: str,
        on_complete: Optional[Callable[[str, float], None]] = None,
    ) -> None:
        self.name = name
        self.on_complete = on_complete
        self._start: float = 0.0
        self.elapsed_us: float = 0.0

    def __enter__(self) -> "ScopedTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        end = time.perf_counter()
        self.elapsed_us = (end - self._start) * 1_000_000.0
        if self.on_complete is not None:
            self.on_complete(self.name, self.elapsed_us)
        else:
            logger.info("%s took %.1f us", self.name, self.elapsed_us)


@dataclass
class ProfiledQueryReport:
    target_name: str
    driver: str
    query_name: str
    elapsed_us: float
    row_count: int
    plan_warnings: list[str]
    learning: dict


def profile_query(
    adapter: DatabaseAdapter,
    query_name: str,
    sql: str,
    history_store: Optional[RollingHistoryStore] = None,
    static_threshold_us: float = 50_000.0,
) -> ProfiledQueryReport:
    """Executes a query on any DatabaseAdapter, inspects its plan, and records rolling learning."""
    elapsed_us, row_count = adapter.execute_and_time(
        sql=sql,
        timer_factory=lambda: ScopedTimer(query_name, on_complete=lambda _n, _us: None),
    )
    plan_lines = adapter.fetch_plan(sql)
    plan_warnings = adapter.extract_scan_warnings(plan_lines)

    sample = ExecutionSample(
        target_name=adapter.target_name,
        driver=adapter.driver_name,
        query_name=query_name,
        elapsed_us=round(elapsed_us, 2),
        row_count=row_count,
        plan_lines=plan_lines,
        plan_warnings=plan_warnings,
    )

    if history_store is not None:
        assessment: LearningAssessment = history_store.record_and_learn(
            target_name=sample.target_name,
            driver=sample.driver,
            query_name=sample.query_name,
            elapsed_us=sample.elapsed_us,
            row_count=sample.row_count,
            plan_lines=sample.plan_lines,
            static_threshold_us=static_threshold_us,
        )
        learning_dict = asdict(assessment)
    else:
        learning_dict = {
            "sample_count": 1,
            "latency_regression": sample.elapsed_us > static_threshold_us,
            "plan_changed": False,
            "volume_anomaly": False,
            "reasons": [],
        }

    return ProfiledQueryReport(
        target_name=sample.target_name,
        driver=sample.driver,
        query_name=sample.query_name,
        elapsed_us=sample.elapsed_us,
        row_count=sample.row_count,
        plan_warnings=sample.plan_warnings,
        learning=learning_dict,
    )
