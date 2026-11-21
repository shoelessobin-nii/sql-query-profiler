from pathlib import Path
from typing import Any

from adapters import AdapterFactory, OracleAdapter
from history_store import RollingHistoryStore
from query_profiler import ScopedTimer, profile_query


class FakeCursor:
    def __init__(self, sql: str) -> None:
        self.sql = sql.upper()
        self.description = [("ID",)] if self.sql.startswith("SELECT") else None

    def execute(self, sql: str) -> None:
        self.sql = sql.upper()

    def fetchall(self) -> list[tuple[Any, ...]]:
        if "DBMS_XPLAN.DISPLAY" in self.sql:
            return [
                ("Plan hash value: 123456789",),
                ("| Id | Operation | Name |",),
                ("| 0 | SELECT STATEMENT | |",),
                ("| 1 | TABLE ACCESS FULL | ORDERS |",),
            ]
        return [(1,), (2,)]

    def fetchone(self) -> tuple[Any, ...]:
        return (1,)

    def close(self) -> None:
        pass


class FakeOracleConn:
    def cursor(self) -> FakeCursor:
        return FakeCursor("")

    def close(self) -> None:
        pass


def test_sqlite_adapter_and_rolling_history(tmp_path: Path) -> None:
    db_file = tmp_path / "app.db"
    hist_file = tmp_path / "history.db"

    adapter = AdapterFactory.create("local_sqlite", {"driver": "sqlite", "db_path": str(db_file)})
    adapter.connect()
    try:
        adapter.execute_and_time(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
            lambda: ScopedTimer("create_items"),
        )
        adapter.execute_and_time(
            "INSERT INTO items (name) VALUES ('widget');",
            lambda: ScopedTimer("insert_item"),
        )
        store = RollingHistoryStore(db_path=str(hist_file), window_size=50)
        report = profile_query(
            adapter=adapter,
            query_name="scan_items",
            sql="SELECT * FROM items WHERE name = 'widget';",
            history_store=store,
            static_threshold_us=1_000_000.0,
        )
        assert report.row_count == 1
        assert any("SCAN" in w.upper() for w in report.plan_warnings)
        assert report.learning["sample_count"] == 1
    finally:
        adapter.close()


def test_oracle_adapter_xplan_and_plan_drift(tmp_path: Path) -> None:
    hist_file = tmp_path / "oracle_history.db"
    store = RollingHistoryStore(db_path=str(hist_file), window_size=20)
    oracle = OracleAdapter(
        target_name="oracle_prod",
        config={"driver": "oracle", "dsn": "localhost:1521/ORCLPDB1"},
        connection_factory=FakeOracleConn,
    )
    oracle.connect()
    try:
        r1 = profile_query(oracle, "orders_q", "SELECT * FROM orders", history_store=store)
        r2 = profile_query(oracle, "orders_q", "SELECT * FROM orders", history_store=store)
        assert r1.learning["current_plan_hash"] == r2.learning["current_plan_hash"]
        assert any("TABLE ACCESS FULL" in w.upper() for w in r2.plan_warnings)
    finally:
        oracle.close()
