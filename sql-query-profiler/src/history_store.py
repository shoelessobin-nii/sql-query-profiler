from __future__ import annotations

import hashlib
import math
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class LearningAssessment:
    """Multi-layer learning evaluation for a single query execution."""
    sample_count: int
    rolling_p50_us: Optional[float]
    rolling_p95_us: Optional[float]
    rolling_mad_us: Optional[float]
    rolling_avg_rows: Optional[float]
    adaptive_threshold_us: float
    latency_regression: bool
    plan_changed: bool
    previous_plan_hash: Optional[str]
    current_plan_hash: str
    volume_anomaly: bool
    reasons: list[str]


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = (len(sorted_values) - 1) * pct
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return sorted_values[lower]
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (idx - lower)


class RollingHistoryStore:
    """
    Stores rolling query execution telemetry in SQLite and learns baselines,
    plan drift, and row-count patterns across any database engine.
    """

    def __init__(
        self,
        db_path: str = "./data/profiler_history.db",
        window_size: int = 200,
        floor_threshold_us: float = 25_000.0,
        cap_threshold_us: float = 5_000_000.0,
    ) -> None:
        self.db_path = db_path
        self.window_size = max(10, int(window_size))
        self.floor_threshold_us = float(floor_threshold_us)
        self.cap_threshold_us = float(cap_threshold_us)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        if self.db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(self.db_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    driver TEXT NOT NULL,
                    query_name TEXT NOT NULL,
                    elapsed_us REAL NOT NULL,
                    row_count INTEGER NOT NULL,
                    plan_hash TEXT NOT NULL,
                    plan_summary TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_query_history_lookup
                ON query_history (target_name, query_name, id DESC);
                """
            )
            conn.commit()

    @staticmethod
    def compute_plan_hash(plan_lines: list[str]) -> str:
        normalized = "\n".join(line.strip().upper() for line in plan_lines if line.strip())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    def record_and_learn(
        self,
        target_name: str,
        driver: str,
        query_name: str,
        elapsed_us: float,
        row_count: int,
        plan_lines: list[str],
        static_threshold_us: float = 50_000.0,
    ) -> LearningAssessment:
        """
        Records a new execution sample, prunes beyond `window_size`, and evaluates
        rolling latency, execution plan drift, and row-count shifts.
        """
        plan_hash = self.compute_plan_hash(plan_lines)
        plan_summary = " | ".join(line.strip() for line in plan_lines[:5])
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._connect() as conn:
            prior_rows = conn.execute(
                """
                SELECT elapsed_us, row_count, plan_hash
                FROM query_history
                WHERE target_name = ? AND query_name = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (target_name, query_name, self.window_size),
            ).fetchall()

            previous_plan_hash = prior_rows[0][2] if prior_rows else None

            conn.execute(
                """
                INSERT INTO query_history (
                    recorded_at, target_name, driver, query_name,
                    elapsed_us, row_count, plan_hash, plan_summary
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_iso,
                    target_name,
                    driver,
                    query_name,
                    float(elapsed_us),
                    int(row_count),
                    plan_hash,
                    plan_summary,
                ),
            )

            conn.execute(
                """
                DELETE FROM query_history
                WHERE target_name = ? AND query_name = ?
                  AND id NOT IN (
                      SELECT id FROM query_history
                      WHERE target_name = ? AND query_name = ?
                      ORDER BY id DESC
                      LIMIT ?
                  )
                """,
                (target_name, query_name, target_name, query_name, self.window_size),
            )
            conn.commit()

        reasons: list[str] = []
        sample_count = len(prior_rows) + 1

        if not prior_rows:
            adaptive_threshold = max(
                self.floor_threshold_us,
                min(self.cap_threshold_us, max(static_threshold_us, elapsed_us * 2.0)),
            )
            return LearningAssessment(
                sample_count=sample_count,
                rolling_p50_us=round(elapsed_us, 2),
                rolling_p95_us=round(elapsed_us, 2),
                rolling_mad_us=0.0,
                rolling_avg_rows=float(row_count),
                adaptive_threshold_us=round(adaptive_threshold, 2),
                latency_regression=elapsed_us > static_threshold_us,
                plan_changed=False,
                previous_plan_hash=None,
                current_plan_hash=plan_hash,
                volume_anomaly=False,
                reasons=["initial_baseline_recorded"] if elapsed_us <= static_threshold_us else ["exceeded_static_threshold"],
            )

        hist_latencies = sorted(float(r[0]) for r in prior_rows)
        hist_row_counts = [float(r[1]) for r in prior_rows]

        p50 = _percentile(hist_latencies, 0.50)
        p95 = _percentile(hist_latencies, 0.95)
        abs_devs = sorted(abs(x - p50) for x in hist_latencies)
        mad = _percentile(abs_devs, 0.50)

        learned_target = max(p95 * 1.25, p50 + max(mad * 4.0, p50 * 0.5))
        adaptive_threshold = max(
            self.floor_threshold_us,
            min(self.cap_threshold_us, learned_target),
        )

        latency_regression = elapsed_us > adaptive_threshold or elapsed_us > static_threshold_us
        if latency_regression:
            reasons.append(
                f"latency_regression ({elapsed_us:.1f}us > adaptive {adaptive_threshold:.1f}us / P50 {p50:.1f}us)"
            )

        plan_changed = previous_plan_hash is not None and previous_plan_hash != plan_hash
        if plan_changed:
            reasons.append(f"plan_drift ({previous_plan_hash} -> {plan_hash})")

        avg_rows = sum(hist_row_counts) / len(hist_row_counts)
        volume_anomaly = False
        if avg_rows > 0 and (row_count > avg_rows * 3.0 or row_count < avg_rows * 0.25):
            volume_anomaly = True
            reasons.append(f"row_count_shift (rows={row_count}, rolling_avg={avg_rows:.1f})")

        return LearningAssessment(
            sample_count=sample_count,
            rolling_p50_us=round(p50, 2),
            rolling_p95_us=round(p95, 2),
            rolling_mad_us=round(mad, 2),
            rolling_avg_rows=round(avg_rows, 2),
            adaptive_threshold_us=round(adaptive_threshold, 2),
            latency_regression=latency_regression,
            plan_changed=plan_changed,
            previous_plan_hash=previous_plan_hash,
            current_plan_hash=plan_hash,
            volume_anomaly=volume_anomaly,
            reasons=reasons,
        )
