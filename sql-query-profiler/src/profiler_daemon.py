from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adapters import AdapterFactory, DatabaseAdapter
from history_store import RollingHistoryStore
from query_profiler import profile_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sql_query_profiler.daemon")


class QueryProfilerDaemon:
    """
    Universal, self-learning, adaptive, and self-healing SQL query profiler daemon.
    Supports SQLite, Oracle, PostgreSQL, and generic SQL targets via adapters.
    """

    def __init__(
        self,
        config: dict[str, Any],
        adapter_overrides: Optional[dict[str, DatabaseAdapter]] = None,
    ) -> None:
        self.config = config
        self.running = True
        self.cycle_count = 0

        self.report_path = Path(config.get("report_path", "./reports/latest_profile.json"))
        self.base_interval = float(config.get("interval_seconds", 10.0))
        self.min_interval = float(config.get("min_interval_seconds", max(2.0, self.base_interval * 0.25)))
        self.max_interval = float(config.get("max_interval_seconds", max(self.base_interval * 4.0, 60.0)))
        self.current_interval = self.base_interval

        self.static_threshold_us = float(config.get("warn_threshold_us", 50_000.0))
        self.auto_heal_mode = str(config.get("auto_heal_mode", "advisory")).lower()
        self.consecutive_anomalies = 0
        self.consecutive_healthy_cycles = 0

        history_db = str(config.get("history_db_path", "./data/profiler_history.db"))
        window_size = int(config.get("history_window_size", 200))
        self.history_store = RollingHistoryStore(db_path=history_db, window_size=window_size)

        self.targets_cfg: dict[str, dict[str, Any]] = config.get("targets", {})
        self.adapters: dict[str, DatabaseAdapter] = {}
        for target_name, t_cfg in self.targets_cfg.items():
            if adapter_overrides and target_name in adapter_overrides:
                self.adapters[target_name] = adapter_overrides[target_name]
            else:
                self.adapters[target_name] = AdapterFactory.create(target_name, t_cfg)

        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def _handle_stop(self, signum: int, _frame: Any) -> None:
        logger.info("Received signal %s; stopping QueryProfilerDaemon...", signum)
        self.running = False

    def _heal_target(self, target_name: str, adapter: DatabaseAdapter, reason: str) -> list[str]:
        """Performs connection recovery and advisory or safe-repair self-healing actions."""
        actions_taken: list[str] = []
        try:
            adapter.close()
            adapter.connect()
            actions_taken.append(f"reconnected_target:{target_name}")
        except Exception as exc:
            actions_taken.append(f"reconnect_failed:{target_name}:{exc}")
            return actions_taken

        healthy, health_msg = adapter.check_health()
        actions_taken.append(f"health_check:{health_msg}")

        if self.auto_heal_mode == "safe_repair":
            if adapter.driver_name == "sqlite" and getattr(adapter, "conn", None) is not None:
                try:
                    adapter.conn.execute("ANALYZE;")
                    adapter.conn.commit()
                    actions_taken.append("sqlite_analyze_executed")
                except Exception as exc:
                    actions_taken.append(f"sqlite_analyze_error:{exc}")
            logger.info("Self-heal [safe_repair] on '%s' (%s): %s", target_name, reason, actions_taken)
        else:
            logger.info("Self-heal [advisory] on '%s' (%s): %s", target_name, reason, actions_taken)

        return actions_taken

    def _adapt_interval(self, had_error: bool, had_anomaly: bool) -> None:
        """Adjusts polling cadence based on target health, latency regressions, and plan drift."""
        if had_error:
            self.consecutive_anomalies += 1
            self.consecutive_healthy_cycles = 0
            self.current_interval = min(self.max_interval, self.current_interval * 1.5)
        elif had_anomaly:
            self.consecutive_anomalies += 1
            self.consecutive_healthy_cycles = 0
            self.current_interval = max(self.min_interval, self.current_interval * 0.75)
        else:
            self.consecutive_anomalies = 0
            self.consecutive_healthy_cycles += 1
            if self.consecutive_healthy_cycles >= 3:
                if self.current_interval < self.base_interval:
                    self.current_interval = min(self.base_interval, self.current_interval * 1.2)
                elif self.current_interval > self.base_interval:
                    self.current_interval = max(self.base_interval, self.current_interval * 0.8)

    def run_cycle(self) -> dict[str, Any]:
        """Executes one profiling, learning, and self-healing pass across all targets."""
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.cycle_count += 1

        target_reports: list[dict[str, Any]] = []
        had_error = False
        had_anomaly = False

        for target_name, t_cfg in self.targets_cfg.items():
            adapter = self.adapters[target_name]
            queries: dict[str, str] = t_cfg.get("queries", {})
            query_reports: list[dict[str, Any]] = []
            target_errors: list[str] = []
            healing_actions: list[str] = []

            try:
                adapter.connect()
                healthy, health_msg = adapter.check_health()
                if not healthy:
                    had_error = True
                    target_errors.append(f"unhealthy:{health_msg}")
                    healing_actions.extend(self._heal_target(target_name, adapter, health_msg))

                for query_name, sql in queries.items():
                    try:
                        q_rep = profile_query(
                            adapter=adapter,
                            query_name=query_name,
                            sql=sql,
                            history_store=self.history_store,
                            static_threshold_us=self.static_threshold_us,
                        )
                        q_dict = asdict(q_rep)
                        query_reports.append(q_dict)

                        learning = q_rep.learning
                        is_anomalous = bool(
                            q_rep.plan_warnings
                            or learning.get("latency_regression")
                            or learning.get("plan_changed")
                            or learning.get("volume_anomaly")
                        )
                        if is_anomalous:
                            had_anomaly = True
                            reasons_str = ", ".join(learning.get("reasons", [])) or "plan_scan_warning"
                            logger.warning(
                                "Anomaly on [%s/%s] (%s): %.1f us | warnings=%s",
                                target_name,
                                query_name,
                                reasons_str,
                                q_rep.elapsed_us,
                                q_rep.plan_warnings,
                            )
                            healing_actions.extend(
                                self._heal_target(target_name, adapter, f"{query_name}:{reasons_str}")
                            )
                    except Exception as q_exc:
                        had_error = True
                        err_msg = f"query '{query_name}' failed: {q_exc}"
                        target_errors.append(err_msg)
                        logger.error("[%s] %s", target_name, err_msg)
                        healing_actions.extend(self._heal_target(target_name, adapter, err_msg))

            except Exception as t_exc:
                had_error = True
                err_msg = f"target connection failed: {t_exc}"
                target_errors.append(err_msg)
                logger.error("[%s] %s", target_name, err_msg)
                healing_actions.extend(self._heal_target(target_name, adapter, err_msg))
            finally:
                adapter.close()

            target_reports.append(
                {
                    "target_name": target_name,
                    "driver": adapter.driver_name,
                    "queries": query_reports,
                    "errors": target_errors,
                    "healing_actions": healing_actions,
                }
            )

        self._adapt_interval(had_error=had_error, had_anomaly=had_anomaly)

        snapshot = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "cycle": self.cycle_count,
            "auto_heal_mode": self.auto_heal_mode,
            "next_interval_seconds": round(self.current_interval, 2),
            "consecutive_anomalies": self.consecutive_anomalies,
            "targets": target_reports,
        }
        self.report_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        return snapshot

    def run(self, max_cycles: int | None = None) -> None:
        """Runs the daemon loop until stopped via signal or max_cycles is reached."""
        logger.info(
            "Starting QueryProfilerDaemon (targets=%s, base_interval=%.1fs, heal_mode=%s)",
            list(self.targets_cfg.keys()),
            self.base_interval,
            self.auto_heal_mode,
        )
        try:
            while self.running:
                self.run_cycle()
                if max_cycles is not None and self.cycle_count >= max_cycles:
                    break

                deadline = time.monotonic() + self.current_interval
                while self.running and time.monotonic() < deadline:
                    time.sleep(0.2)
        finally:
            for adapter in self.adapters.values():
                adapter.close()
            logger.info("QueryProfilerDaemon stopped cleanly.")


if __name__ == "__main__":
    cfg_path = Path("config/profiler_config.json")
    default_cfg: dict[str, Any] = {
        "history_db_path": "./data/profiler_history.db",
        "history_window_size": 200,
        "report_path": "./reports/latest_profile.json",
        "interval_seconds": 10.0,
        "warn_threshold_us": 50000.0,
        "auto_heal_mode": "advisory",
        "targets": {
            "local_sqlite": {
                "driver": "sqlite",
                "db_path": "./data/sample.db",
                "queries": {
                    "sqlite_master_check": "SELECT name, type FROM sqlite_master;"
                },
            }
        },
    }
    loaded = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else default_cfg
    QueryProfilerDaemon(loaded).run()
