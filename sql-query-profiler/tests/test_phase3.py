import sqlite3
from pathlib import Path

from profiler_daemon import QueryProfilerDaemon


def test_daemon_adapts_interval_on_scan_anomaly_and_writes_report(tmp_path: Path) -> None:
    db_file = tmp_path / "target.db"
    hist_file = tmp_path / "history.db"
    report_file = tmp_path / "report.json"

    with sqlite3.connect(db_file) as conn:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")
        conn.execute("INSERT INTO users (email) VALUES ('a@example.com');")

    cfg = {
        "history_db_path": str(hist_file),
        "history_window_size": 50,
        "report_path": str(report_file),
        "interval_seconds": 10.0,
        "min_interval_seconds": 2.0,
        "max_interval_seconds": 30.0,
        "warn_threshold_us": 1_000_000.0,
        "auto_heal_mode": "safe_repair",
        "targets": {
            "sqlite_app": {
                "driver": "sqlite",
                "db_path": str(db_file),
                "queries": {
                    "full_scan_lookup": "SELECT * FROM users WHERE email = 'a@example.com';"
                },
            }
        },
    }

    daemon = QueryProfilerDaemon(cfg)
    snap = daemon.run_cycle()

    assert report_file.exists()
    assert snap["consecutive_anomalies"] >= 1
    assert snap["next_interval_seconds"] < 10.0
    assert len(snap["targets"][0]["healing_actions"]) > 0
