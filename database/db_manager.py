"""
database/db_manager.py — runtime database abstraction for SQLite or PostgreSQL.

SQLite remains the default local backend. When POLYMARKET_DB_BACKEND=postgres
or POLYMARKET_DATABASE_URL is set, runtime connections are opened against a
local PostgreSQL instance instead.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from config.settings import DATABASE_URL, DB_BACKEND, DB_PATH

try:
    import psycopg
except ImportError:  # pragma: no cover - optional until Postgres is enabled
    psycopg = None


BASE_DIR = Path(__file__).resolve().parent
SQLITE_SCHEMA_PATH = BASE_DIR / "schema.sql"
POSTGRES_SCHEMA_PATH = BASE_DIR / "postgres_schema.sql"

_EVENTS_NEW_COLS = [
    ("description", "TEXT"),
    ("category", "TEXT"),
    ("tags", "TEXT"),
    ("tag_ids", "TEXT"),
    ("volume", "REAL DEFAULT 0"),
    ("volume_24hr", "REAL DEFAULT 0"),
    ("volume_1wk", "REAL DEFAULT 0"),
    ("volume_1mo", "REAL DEFAULT 0"),
    ("liquidity", "REAL DEFAULT 0"),
    ("open_interest", "REAL DEFAULT 0"),
    ("comment_count", "INTEGER DEFAULT 0"),
    ("competitive", "REAL DEFAULT 0"),
    ("start_date", "TEXT"),
    ("end_date", "TEXT"),
    ("creation_date", "TEXT"),
    ("neg_risk", "INTEGER DEFAULT 0"),
    ("featured", "INTEGER DEFAULT 0"),
    ("restricted", "INTEGER DEFAULT 0"),
    ("status", "TEXT DEFAULT 'active'"),
    ("first_seen_at", "DATETIME"),
    ("last_updated_at", "DATETIME"),
    ("closed_at", "DATETIME"),
]

_MARKETS_NEW_COLS = [
    ("description", "TEXT"),
    ("slug", "TEXT"),
    ("condition_id", "TEXT"),
    ("no_token_id", "TEXT"),
    ("outcomes", "TEXT"),
    ("outcome_prices", "TEXT"),
    ("volume", "REAL DEFAULT 0"),
    ("volume_24hr", "REAL DEFAULT 0"),
    ("volume_1wk", "REAL DEFAULT 0"),
    ("volume_1mo", "REAL DEFAULT 0"),
    ("liquidity", "REAL DEFAULT 0"),
    ("best_bid", "REAL"),
    ("best_ask", "REAL"),
    ("spread", "REAL"),
    ("last_trade_price", "REAL"),
    ("price_change_1d", "REAL"),
    ("price_change_1wk", "REAL"),
    ("min_tick_size", "REAL"),
    ("min_order_size", "REAL"),
    ("accepts_orders", "INTEGER DEFAULT 0"),
    ("enable_order_book", "INTEGER DEFAULT 0"),
    ("neg_risk", "INTEGER DEFAULT 0"),
    ("restricted", "INTEGER DEFAULT 0"),
    ("automated", "INTEGER DEFAULT 0"),
    ("outcome", "TEXT"),
    ("start_date", "TEXT"),
    ("end_date", "TEXT"),
    ("tier", "INTEGER DEFAULT 3"),
    ("status", "TEXT DEFAULT 'active'"),
    ("first_seen_at", "DATETIME"),
    ("last_updated_at", "DATETIME"),
    ("closed_at", "DATETIME"),
]

_TRADES_NEW_COLS = [
    ("asset_id", "TEXT"),
    ("condition_id", "TEXT"),
    ("proxy_wallet", "TEXT"),
    ("transaction_hash", "TEXT"),
    ("outcome_side", "TEXT"),
    ("usdc_notional", "REAL"),
    ("dedupe_key", "TEXT"),
    ("source_priority", "INTEGER DEFAULT 0"),
]

_NAMED_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_SQLALCHEMY_POSTGRES_SCHEME_RE = re.compile(r"^postgresql\+[A-Za-z0-9_]+://", re.IGNORECASE)


def backend_name() -> str:
    """Return the normalized runtime backend name."""

    return (DB_BACKEND or "sqlite").strip().lower()


def is_postgres_backend() -> bool:
    """Return True when runtime storage is configured for PostgreSQL."""

    return backend_name() in {"postgres", "postgresql"}


def _normalize_postgres_dsn(dsn: str) -> str:
    """Convert SQLAlchemy-style PostgreSQL URLs into libpq-compatible DSNs."""

    stripped = dsn.strip()
    if _SQLALCHEMY_POSTGRES_SCHEME_RE.match(stripped):
        return _SQLALCHEMY_POSTGRES_SCHEME_RE.sub("postgresql://", stripped, count=1)
    return stripped


class RowProxy:
    """Tuple-like row wrapper that also supports dict-style column lookup."""

    __slots__ = ("_columns", "_index", "_values")

    def __init__(self, columns: Iterable[str], values: Iterable[Any]):
        self._columns = tuple(columns)
        self._index = {column: idx for idx, column in enumerate(self._columns)}
        self._values = tuple(values)

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._index[key]]

    def get(self, key: str, default: Any = None) -> Any:
        idx = self._index.get(key)
        if idx is None:
            return default
        return self._values[idx]

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        items = ", ".join(f"{column}={self[column]!r}" for column in self._columns)
        return f"RowProxy({items})"


class PostgresCursorWrapper:
    """Cursor adapter with sqlite-like fetch semantics."""

    def __init__(self, cursor):
        self._cursor = cursor
        self._columns = tuple(desc.name for desc in (cursor.description or ()))
        self.rowcount = cursor.rowcount

    def _wrap(self, row: Any) -> RowProxy | None:
        if row is None:
            self._cursor.close()
            return None
        return RowProxy(self._columns, row)

    def fetchone(self) -> RowProxy | None:
        row = self._cursor.fetchone()
        wrapped = self._wrap(row)
        if row is None:
            return None
        return wrapped

    def fetchall(self) -> list[RowProxy]:
        rows = self._cursor.fetchall()
        self._cursor.close()
        return [RowProxy(self._columns, row) for row in rows]

    def close(self) -> None:
        self._cursor.close()


class PostgresConnectionWrapper:
    """Compatibility layer for current sqlite-style runtime SQL."""

    def __init__(self, dsn: str):
        if psycopg is None:
            raise RuntimeError(
                "PostgreSQL backend requested but psycopg is not installed in the active environment."
            )
        self._conn = psycopg.connect(_normalize_postgres_dsn(dsn))

    def _normalize_sql(self, sql: str, params: Any) -> tuple[str, Any]:
        if params is None:
            return sql, None
        if isinstance(params, dict):
            return _NAMED_PARAM_RE.sub(r"%(\1)s", sql), params
        if isinstance(params, (list, tuple)):
            return sql.replace("?", "%s"), tuple(params)
        return sql, params

    def execute(self, sql: str, params: Any = None) -> PostgresCursorWrapper:
        normalized_sql, normalized_params = self._normalize_sql(sql, params)
        cursor = self._conn.cursor()
        if normalized_params is None:
            cursor.execute(normalized_sql)
        else:
            cursor.execute(normalized_sql, normalized_params)
        return PostgresCursorWrapper(cursor)

    def executemany(self, sql: str, seq_of_params: Iterable[Any]) -> PostgresCursorWrapper:
        rows = list(seq_of_params)
        normalized_sql = sql
        normalized_rows = rows
        if rows:
            normalized_sql, _ = self._normalize_sql(sql, rows[0])
            if isinstance(rows[0], (list, tuple)):
                normalized_rows = [tuple(row) for row in rows]
        cursor = self._conn.cursor()
        cursor.executemany(normalized_sql, normalized_rows)
        return PostgresCursorWrapper(cursor)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


def _sqlite_get_conn() -> sqlite3.Connection:
    """Return a SQLite connection with the local performance pragmas applied."""

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    return conn


def _postgres_get_conn() -> PostgresConnectionWrapper:
    """Return a compatibility-wrapped PostgreSQL connection."""

    if not DATABASE_URL:
        raise RuntimeError(
            "PostgreSQL backend is enabled but POLYMARKET_DATABASE_URL is not set."
        )
    return PostgresConnectionWrapper(DATABASE_URL)


def get_conn():
    """Return a backend-appropriate runtime connection."""

    if is_postgres_backend():
        return _postgres_get_conn()
    return _sqlite_get_conn()


def _sqlite_table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _sqlite_add_missing_columns(conn: sqlite3.Connection, table: str, col_defs: list[tuple[str, str]]):
    """ALTER TABLE to add any columns not already present."""

    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for column, column_type in col_defs:
        if column in existing:
            continue
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
            if conn.isolation_level is not None:
                conn.commit()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "duplicate column name" not in message and "already exists" not in message:
                raise


def _apply_sql_statements(conn, sql_path: Path, *, skip_pragmas: bool = False) -> None:
    """Apply one schema SQL file statement-by-statement."""

    statements = [statement.strip() for statement in sql_path.read_text(encoding="utf-8").split(";") if statement.strip()]
    for statement in statements:
        if skip_pragmas and statement.upper().startswith("PRAGMA"):
            continue
        try:
            conn.execute(statement)
        except Exception as exc:  # pragma: no cover - defensive startup logging
            message = str(exc).lower()
            if "already exists" in message or "duplicate" in message:
                continue
            print(f"[apply_schema] Warning: {exc} (stmt={statement[:80]}...)")


def _apply_sqlite_schema() -> None:
    """Apply the canonical SQLite schema idempotently."""

    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA busy_timeout=10000")

    try:
        if _sqlite_table_exists(conn, "events"):
            _sqlite_add_missing_columns(conn, "events", _EVENTS_NEW_COLS)
        if _sqlite_table_exists(conn, "markets"):
            _sqlite_add_missing_columns(conn, "markets", _MARKETS_NEW_COLS)
        if _sqlite_table_exists(conn, "trades"):
            _sqlite_add_missing_columns(conn, "trades", _TRADES_NEW_COLS)
        if _sqlite_table_exists(conn, "order_books"):
            conn.execute("DROP TABLE order_books")
        _apply_sql_statements(conn, SQLITE_SCHEMA_PATH, skip_pragmas=False)
    finally:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.close()


def _apply_postgres_schema() -> None:
    """Apply the canonical PostgreSQL schema idempotently."""

    conn = _postgres_get_conn()
    try:
        _apply_sql_statements(conn, POSTGRES_SCHEMA_PATH, skip_pragmas=True)
        conn.commit()
    finally:
        conn.close()


def apply_schema() -> None:
    """Apply the configured backend schema idempotently on startup."""

    if is_postgres_backend():
        _apply_postgres_schema()
        return
    _apply_sqlite_schema()
