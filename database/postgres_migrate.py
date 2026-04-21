"""SQLite -> PostgreSQL cutover helper for Phase 2.

This script is intentionally explicit:
- it applies the PostgreSQL target schema
- it migrates canonical tables from the local SQLite source database
- it records row counts so the cutover can be audited

It expects a SQLAlchemy-compatible PostgreSQL URL, for example:
    postgresql+psycopg://user:password@localhost:5432/polymarket
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DB = REPO_ROOT / "database" / "polymarket_state.db"
DEFAULT_SCHEMA = REPO_ROOT / "database" / "postgres_schema.sql"
DEFAULT_TABLES = (
    "events",
    "markets",
    "market_resolutions",
    "snapshots",
    "order_book_snapshots",
    "universe_review_candidates",
    "trades",
    "raw_archive_manifests",
    "detector_input_manifests",
    "schema_versions",
    "replay_runs",
)


def _read_schema_statements(schema_path: Path) -> list[str]:
    """Load semicolon-delimited SQL statements from the target schema file."""

    sql = schema_path.read_text(encoding="utf-8")
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _connect_sqlite(source_db: Path) -> sqlite3.Connection:
    """Open the SQLite source database with row access by column name."""

    conn = sqlite3.connect(source_db)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Return SQLite column names in table order."""

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _sqlite_supports_rowid(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return True when the SQLite table exposes the implicit rowid column."""

    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    if row is None:
        return False
    create_sql = str(row[0] or "").upper()
    return "WITHOUT ROWID" not in create_sql


def _apply_target_schema(engine: Engine, schema_path: Path) -> None:
    """Apply the PostgreSQL target schema."""

    statements = _read_schema_statements(schema_path)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def _truncate_target_tables(engine: Engine, table_names: Iterable[str]) -> None:
    """Clear target tables before a full reload."""

    ordered = list(table_names)
    with engine.begin() as conn:
        for table_name in reversed(ordered):
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))


def _insert_rows(
    *,
    engine: Engine,
    table_name: str,
    rows: list[dict],
) -> None:
    """Insert one row batch into the target database."""

    if not rows:
        return
    columns = list(rows[0].keys())
    column_sql = ", ".join(columns)
    bind_sql = ", ".join(f":{column}" for column in columns)
    statement = text(f"INSERT INTO {table_name} ({column_sql}) VALUES ({bind_sql})")
    with engine.begin() as conn:
        conn.execute(statement, rows)


def migrate_table(
    *,
    sqlite_conn: sqlite3.Connection,
    engine: Engine,
    table_name: str,
    batch_size: int,
) -> int:
    """Migrate one table from SQLite into PostgreSQL."""

    columns = _sqlite_columns(sqlite_conn, table_name)
    if not columns:
        return 0

    total_rows = 0
    if _sqlite_supports_rowid(sqlite_conn, table_name):
        last_rowid = 0
        select_sql = f"""
            SELECT rowid AS _source_rowid, *
            FROM {table_name}
            WHERE rowid > ?
            ORDER BY rowid
            LIMIT ?
        """
        while True:
            batch = sqlite_conn.execute(select_sql, (last_rowid, batch_size)).fetchall()
            if not batch:
                break
            payload = [{column: row[column] for column in columns} for row in batch]
            _insert_rows(engine=engine, table_name=table_name, rows=payload)
            total_rows += len(payload)
            last_rowid = int(batch[-1]["_source_rowid"])
    else:
        offset = 0
        select_sql = f"SELECT * FROM {table_name} LIMIT ? OFFSET ?"
        while True:
            batch = sqlite_conn.execute(select_sql, (batch_size, offset)).fetchall()
            if not batch:
                break
            payload = [{column: row[column] for column in columns} for row in batch]
            _insert_rows(engine=engine, table_name=table_name, rows=payload)
            total_rows += len(payload)
            offset += batch_size
    return total_rows


def build_parser() -> argparse.ArgumentParser:
    """Create the cutover CLI parser."""

    parser = argparse.ArgumentParser(description="Migrate the local SQLite dataset into PostgreSQL.")
    parser.add_argument(
        "--source-db",
        default=str(DEFAULT_SOURCE_DB),
        help="Path to the SQLite source database.",
    )
    parser.add_argument(
        "--target-url",
        required=True,
        help="SQLAlchemy PostgreSQL target URL, e.g. postgresql+psycopg://user:pass@localhost:5432/db",
    )
    parser.add_argument(
        "--schema-file",
        default=str(DEFAULT_SCHEMA),
        help="Path to the PostgreSQL schema SQL file.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=list(DEFAULT_TABLES),
        help="Optional subset of tables to migrate.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of source rows per insert batch.",
    )
    parser.add_argument(
        "--skip-schema-apply",
        action="store_true",
        help="Do not apply the PostgreSQL schema before migrating.",
    )
    parser.add_argument(
        "--skip-truncate",
        action="store_true",
        help="Do not truncate target tables before migrating.",
    )
    return parser


def main() -> int:
    """Run the cutover migration."""

    args = build_parser().parse_args()
    source_db = Path(args.source_db)
    schema_file = Path(args.schema_file)
    engine = create_engine(args.target_url)

    if not args.skip_schema_apply:
        _apply_target_schema(engine, schema_file)

    if not args.skip_truncate:
        _truncate_target_tables(engine, args.tables)

    sqlite_conn = _connect_sqlite(source_db)
    try:
        for table_name in args.tables:
            row_count = migrate_table(
                sqlite_conn=sqlite_conn,
                engine=engine,
                table_name=table_name,
                batch_size=args.batch_size,
            )
            print(f"{table_name}: migrated {row_count} rows")
    finally:
        sqlite_conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
