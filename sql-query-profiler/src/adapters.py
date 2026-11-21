from __future__ import annotations

import os
import re
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional


def resolve_env_placeholders(value: Any) -> Any:
    """Replaces ${VAR_NAME} strings with environment variable values when present."""
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
        return re.sub(r"\$\{([A-Z0-9_]+)\}", _replace, value)
    if isinstance(value, dict):
        return {k: resolve_env_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_env_placeholders(item) for item in value]
    return value


@dataclass
class ExecutionSample:
    """Normalized result from running a SQL statement through any database adapter."""
    target_name: str
    driver: str
    query_name: str
    elapsed_us: float
    row_count: int
    plan_lines: list[str] = field(default_factory=list)
    plan_warnings: list[str] = field(default_factory=list)


class DatabaseAdapter(ABC):
    """Engine-agnostic contract for connecting, timing queries, and inspecting plans."""

    driver_name: str = "generic"

    def __init__(self, target_name: str, config: dict[str, Any]) -> None:
        self.target_name = target_name
        self.config = resolve_env_placeholders(config)

    @abstractmethod
    def connect(self) -> None:
        """Establishes or verifies the connection/session to the database."""

    @abstractmethod
    def close(self) -> None:
        """Closes active connections/sessions cleanly."""

    @abstractmethod
    def execute_and_time(self, sql: str, timer_factory: Callable[[], Any]) -> tuple[float, int]:
        """Executes SQL while timing with a timer context manager; returns (elapsed_us, row_count)."""

    @abstractmethod
    def fetch_plan(self, sql: str) -> list[str]:
        """Retrieves human-readable execution plan lines for a SQL statement."""

    @abstractmethod
    def check_health(self) -> tuple[bool, str]:
        """Runs a lightweight health check; returns (healthy, status_message)."""

    @staticmethod
    def extract_scan_warnings(plan_lines: list[str]) -> list[str]:
        """Flags full-table or unindexed scan patterns across SQLite, Oracle, Postgres, etc."""
        warnings: list[str] = []
        scan_markers = (
            "TABLE ACCESS FULL",
            "FULL SCAN",
            "SEQ SCAN",
            "SCAN TABLE",
            "SCAN ",
        )
        for line in plan_lines:
            upper = line.upper()
            if any(marker in upper for marker in scan_markers) and "INDEX RANGE SCAN" not in upper:
                warnings.append(line.strip())
        return warnings


class SQLiteAdapter(DatabaseAdapter):
    """Universal adapter for local or embedded SQLite databases."""

    driver_name = "sqlite"

    def __init__(self, target_name: str, config: dict[str, Any]) -> None:
        super().__init__(target_name, config)
        self.db_path = str(self.config.get("db_path", "./data/sample.db"))
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        if self.db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _ensure_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            self.connect()
        assert self.conn is not None
        return self.conn

    def execute_and_time(self, sql: str, timer_factory: Callable[[], Any]) -> tuple[float, int]:
        conn = self._ensure_conn()
        with timer_factory() as timer:
            cursor = conn.execute(sql)
            rows = cursor.fetchall() if cursor.description else []
        return float(timer.elapsed_us), len(rows)

    def fetch_plan(self, sql: str) -> list[str]:
        conn = self._ensure_conn()
        rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
        return [" ".join(str(col) for col in r) for r in rows]

    def check_health(self) -> tuple[bool, str]:
        try:
            conn = self._ensure_conn()
            row = conn.execute("PRAGMA integrity_check;").fetchone()
            status = str(row[0]) if row else "unknown"
            return (status.lower() == "ok"), f"integrity_check={status}"
        except Exception as exc:
            return False, f"sqlite health error: {exc}"


class OracleAdapter(DatabaseAdapter):
    """
    Universal adapter for Oracle databases using EXPLAIN PLAN FOR + DBMS_XPLAN.DISPLAY.
    Accepts an optional `connection_factory` for testing or custom session hooks.
    """

    driver_name = "oracle"

    def __init__(
        self,
        target_name: str,
        config: dict[str, Any],
        connection_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        super().__init__(target_name, config)
        self._connection_factory = connection_factory
        self.conn: Any = None

    def connect(self) -> None:
        if self._connection_factory is not None:
            self.conn = self._connection_factory()
            return
        try:
            import oracledb  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "Oracle driver 'oracledb' is not installed. Install with: pip install oracledb"
            ) from exc

        dsn = self.config.get("dsn")
        user = self.config.get("user")
        password = self.config.get("password")
        if dsn and user and password:
            self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        elif dsn:
            self.conn = oracledb.connect(dsn=dsn)
        else:
            raise ValueError(f"Oracle target '{self.target_name}' requires 'dsn' (and credentials).")

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _ensure_conn(self) -> Any:
        if self.conn is None:
            self.connect()
        return self.conn

    def execute_and_time(self, sql: str, timer_factory: Callable[[], Any]) -> tuple[float, int]:
        conn = self._ensure_conn()
        with timer_factory() as timer:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall() if getattr(cursor, "description", None) else []
            if hasattr(cursor, "close"):
                cursor.close()
        return float(timer.elapsed_us), len(rows)

    def fetch_plan(self, sql: str) -> list[str]:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        try:
            clean_sql = sql.strip().rstrip(";")
            cursor.execute(f"EXPLAIN PLAN FOR {clean_sql}")
            cursor.execute("SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY())")
            rows = cursor.fetchall()
            return [str(r[0]) for r in rows if r and r[0] is not None]
        finally:
            if hasattr(cursor, "close"):
                cursor.close()

    def check_health(self) -> tuple[bool, str]:
        try:
            conn = self._ensure_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            row = cursor.fetchone()
            if hasattr(cursor, "close"):
                cursor.close()
            return (row is not None), "oracle SELECT 1 FROM DUAL ok"
        except Exception as exc:
            return False, f"oracle health error: {exc}"


class GenericSQLAdapter(DatabaseAdapter):
    """
    Plug-and-play adapter for PostgreSQL, MySQL, SQL Server, or any DB-API-like target.
    Supports `plan_prefix` (defaults to 'EXPLAIN ') and an optional `connection_factory`.
    """

    def __init__(
        self,
        target_name: str,
        config: dict[str, Any],
        connection_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        super().__init__(target_name, config)
        self.driver_name = str(self.config.get("driver", "sql")).lower()
        self.plan_prefix = str(self.config.get("plan_prefix", "EXPLAIN "))
        self.health_sql = str(self.config.get("health_sql", "SELECT 1;"))
        self._connection_factory = connection_factory
        self.conn: Any = None

    def connect(self) -> None:
        if self._connection_factory is not None:
            self.conn = self._connection_factory()
            return
        dsn = self.config.get("dsn")
        if not dsn:
            raise ValueError(
                f"Target '{self.target_name}' ({self.driver_name}) requires 'dsn' or a connection_factory."
            )
        if self.driver_name in ("postgres", "postgresql"):
            try:
                import psycopg  # type: ignore
                self.conn = psycopg.connect(dsn)
                return
            except ImportError as exc:
                raise ImportError("Install 'psycopg' (pip install psycopg[binary]) for Postgres.") from exc
        raise ValueError(
            f"No built-in client for driver '{self.driver_name}' without a connection_factory."
        )

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _ensure_conn(self) -> Any:
        if self.conn is None:
            self.connect()
        return self.conn

    def execute_and_time(self, sql: str, timer_factory: Callable[[], Any]) -> tuple[float, int]:
        conn = self._ensure_conn()
        with timer_factory() as timer:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall() if getattr(cursor, "description", None) else []
            if hasattr(cursor, "close"):
                cursor.close()
        return float(timer.elapsed_us), len(rows)

    def fetch_plan(self, sql: str) -> list[str]:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f"{self.plan_prefix}{sql}")
            rows = cursor.fetchall()
            return [" ".join(str(col) for col in r) for r in rows]
        finally:
            if hasattr(cursor, "close"):
                cursor.close()

    def check_health(self) -> tuple[bool, str]:
        try:
            conn = self._ensure_conn()
            cursor = conn.cursor()
            cursor.execute(self.health_sql)
            _ = cursor.fetchone()
            if hasattr(cursor, "close"):
                cursor.close()
            return True, f"{self.driver_name} health check ok"
        except Exception as exc:
            return False, f"{self.driver_name} health error: {exc}"


class AdapterFactory:
    """Creates the right adapter for any configured database target."""

    @staticmethod
    def create(
        target_name: str,
        config: dict[str, Any],
        connection_factory: Optional[Callable[[], Any]] = None,
    ) -> DatabaseAdapter:
        driver = str(config.get("driver", "sqlite")).lower()
        if driver == "sqlite":
            return SQLiteAdapter(target_name, config)
        if driver == "oracle":
            return OracleAdapter(target_name, config, connection_factory=connection_factory)
        return GenericSQLAdapter(target_name, config, connection_factory=connection_factory)
