"""
database/db_manager.py — SQLite connection helper + idempotent schema applier.

apply_schema() uses isolation_level=None (autocommit) so every DDL statement
(ALTER TABLE, CREATE TABLE, CREATE INDEX) commits immediately.
This is required on Python 3.12+ where sqlite3 no longer auto-commits DDL.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "polymarket_state.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def get_conn() -> sqlite3.Connection:
    """Return a connection with WAL mode and row factory enabled."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # Off for bulk ingestion — markets may ref uncached events
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    return conn


# ---------------------------------------------------------------------------
# All v2 columns to add to existing v1 tables.
# Listed in dependency order — no column depends on another in this list.
# ---------------------------------------------------------------------------
_EVENTS_NEW_COLS = [
    ("description",     "TEXT"),
    ("category",        "TEXT"),
    ("tags",            "TEXT"),
    ("volume",          "REAL DEFAULT 0"),
    ("volume_24hr",     "REAL DEFAULT 0"),
    ("volume_1wk",      "REAL DEFAULT 0"),
    ("volume_1mo",      "REAL DEFAULT 0"),
    ("liquidity",       "REAL DEFAULT 0"),
    ("open_interest",   "REAL DEFAULT 0"),
    ("comment_count",   "INTEGER DEFAULT 0"),
    ("competitive",     "REAL DEFAULT 0"),
    ("start_date",      "TEXT"),
    ("end_date",        "TEXT"),
    ("creation_date",   "TEXT"),
    ("neg_risk",        "INTEGER DEFAULT 0"),
    ("featured",        "INTEGER DEFAULT 0"),
    ("restricted",      "INTEGER DEFAULT 0"),
    ("status",          "TEXT DEFAULT 'active'"),
    # NOTE: ALTER TABLE ADD COLUMN in SQLite does NOT allow non-constant defaults
    # (e.g. CURRENT_TIMESTAMP). Use plain DATETIME with no default here — our Python
    # code always provides these values explicitly on insert.
    ("first_seen_at",   "DATETIME"),
    ("last_updated_at", "DATETIME"),
    ("closed_at",       "DATETIME"),
]

_MARKETS_NEW_COLS = [
    ("description",      "TEXT"),
    ("slug",             "TEXT"),
    ("condition_id",     "TEXT"),
    ("no_token_id",      "TEXT"),
    ("outcomes",         "TEXT"),
    ("outcome_prices",   "TEXT"),
    ("volume",           "REAL DEFAULT 0"),
    ("volume_24hr",      "REAL DEFAULT 0"),
    ("volume_1wk",       "REAL DEFAULT 0"),
    ("volume_1mo",       "REAL DEFAULT 0"),
    ("liquidity",        "REAL DEFAULT 0"),
    ("best_bid",         "REAL"),
    ("best_ask",         "REAL"),
    ("spread",           "REAL"),
    ("last_trade_price", "REAL"),
    ("price_change_1d",  "REAL"),
    ("price_change_1wk", "REAL"),
    ("min_tick_size",    "REAL"),
    ("min_order_size",   "REAL"),
    ("accepts_orders",   "INTEGER DEFAULT 0"),
    ("enable_order_book","INTEGER DEFAULT 0"),
    ("neg_risk",         "INTEGER DEFAULT 0"),
    ("restricted",       "INTEGER DEFAULT 0"),
    ("automated",        "INTEGER DEFAULT 0"),
    ("outcome",          "TEXT"),
    ("start_date",       "TEXT"),
    ("end_date",         "TEXT"),
    ("tier",             "INTEGER DEFAULT 3"),
    ("status",           "TEXT DEFAULT 'active'"),
    ("first_seen_at",    "DATETIME"),
    ("last_updated_at",  "DATETIME"),
    ("closed_at",        "DATETIME"),
]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return r is not None


def _add_missing_columns(conn: sqlite3.Connection, table: str, col_defs: list):
    """
    ALTER TABLE to add any columns not already in the table.
    Uses an autocommit connection so each ALTER commits immediately.
    """
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, coltype in col_defs:
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
                # In autocommit mode (isolation_level=None) each execute commits.
                # If NOT in autocommit, force commit explicitly:
                if conn.isolation_level is not None:
                    conn.commit()
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower() and "already exists" not in str(e).lower():
                    raise  # Only swallow "already exists" errors


def apply_schema():
    """
    Apply the canonical v2 schema idempotently on every startup.

    Uses isolation_level=None (autocommit) so every DDL statement commits
    immediately — required on Python 3.12+ where sqlite3 no longer implicitly
    auto-commits DDL inside a pending transaction.
    """
    # autocommit connection for schema operations
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("PRAGMA busy_timeout=10000")  # Wait up to 10s if DB is locked

    try:
        # --- Step 1: Patch existing tables with missing columns ---
        if _table_exists(conn, "events"):
            _add_missing_columns(conn, "events", _EVENTS_NEW_COLS)

        if _table_exists(conn, "markets"):
            _add_missing_columns(conn, "markets", _MARKETS_NEW_COLS)

        # Drop old order_books table (replaced by order_book_snapshots)
        if _table_exists(conn, "order_books"):
            conn.execute("DROP TABLE order_books")

        # --- Step 2: Apply full v2 schema (CREATE TABLE IF NOT EXISTS + indexes) ---
        with open(SCHEMA_PATH, "r") as f:
            sql = f.read()

        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as e:
                err = str(e).lower()
                if "already exists" in err or "duplicate" in err:
                    continue
                if stmt.strip().upper().startswith("PRAGMA"):
                    continue
                # Log unexpected errors but don't crash startup
                print(f"[apply_schema] Warning: {e} (stmt={stmt[:60]}...)")

    finally:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.close()