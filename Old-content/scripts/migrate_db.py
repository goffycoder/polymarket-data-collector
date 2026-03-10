"""
migrate_db.py — One-time migration from v1 → v2 schema.

Run this ONCE before restarting run_collector.py:
    python migrate_db.py

What it does:
  - Backs up old snapshots → snapshots_v1_backup
  - Drops old incompatible snapshots table
  - Adds missing columns to events and markets tables (ALTER TABLE ADD COLUMN, safe)
  - Applies the full v2 schema so new tables (order_book_snapshots, trades) are created
  - Verifies the final schema looks correct
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "database", "polymarket_state.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "database", "schema.sql")


def get_existing_columns(conn, table):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def table_exists(conn, name):
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return r is not None


def migrate():
    print("=" * 55)
    print("  POLYMARKET DB MIGRATION: v1 → v2")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=OFF")  # Disable FK checks during migration

    # -------------------------------------------------------
    # 1. Backup + drop old snapshots table (incompatible schema)
    # -------------------------------------------------------
    if table_exists(conn, "snapshots"):
        existing_cols = get_existing_columns(conn, "snapshots")
        if "captured_at" not in existing_cols or "yes_price" not in existing_cols:
            print("⚠️  Old snapshots table detected — backing up and dropping...")
            old_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

            # Drop old backup if exists
            conn.execute("DROP TABLE IF EXISTS snapshots_v1_backup")
            # Rename old table as backup
            conn.execute("ALTER TABLE snapshots RENAME TO snapshots_v1_backup")
            print(f"   → Backed up {old_count:,} rows to snapshots_v1_backup")
        else:
            print("✅ snapshots table already has v2 schema — skipping backup")

    # -------------------------------------------------------
    # 2. Drop old order_books table if it exists (replaced by order_book_snapshots)
    # -------------------------------------------------------
    if table_exists(conn, "order_books"):
        conn.execute("DROP TABLE IF EXISTS order_books")
        print("🗑  Dropped old order_books table (replaced by order_book_snapshots)")

    conn.commit()

    # -------------------------------------------------------
    # 3. Patch events table — add any missing v2 columns
    # -------------------------------------------------------
    events_cols = get_existing_columns(conn, "events") if table_exists(conn, "events") else set()
    events_new_cols = [
        ("description",     "TEXT"),
        ("category",        "TEXT"),
        ("volume_24hr",     "REAL DEFAULT 0"),
        ("volume_1wk",      "REAL DEFAULT 0"),
        ("volume_1mo",      "REAL DEFAULT 0"),
        ("open_interest",   "REAL DEFAULT 0"),
        ("comment_count",   "INTEGER DEFAULT 0"),
        ("competitive",     "REAL DEFAULT 0"),
        ("creation_date",   "TEXT"),
        ("neg_risk",        "INTEGER DEFAULT 0"),
        ("featured",        "INTEGER DEFAULT 0"),
        ("restricted",      "INTEGER DEFAULT 0"),
        ("first_seen_at",   "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("last_updated_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
    ]
    added = 0
    for col, coltype in events_new_cols:
        if col not in events_cols:
            try:
                conn.execute(f"ALTER TABLE events ADD COLUMN {col} {coltype}")
                added += 1
            except sqlite3.OperationalError:
                pass

    if added:
        print(f"✅ events table: added {added} missing columns")
    else:
        print("✅ events table: already up to date")

    # -------------------------------------------------------
    # 4. Patch markets table — add any missing v2 columns
    # -------------------------------------------------------
    markets_cols = get_existing_columns(conn, "markets") if table_exists(conn, "markets") else set()
    markets_new_cols = [
        ("description",     "TEXT"),
        ("slug",            "TEXT"),
        ("no_token_id",     "TEXT"),
        ("outcomes",        "TEXT"),
        ("outcome_prices",  "TEXT"),
        ("volume_24hr",     "REAL DEFAULT 0"),
        ("volume_1wk",      "REAL DEFAULT 0"),
        ("volume_1mo",      "REAL DEFAULT 0"),
        ("best_bid",        "REAL"),
        ("best_ask",        "REAL"),
        ("spread",          "REAL"),
        ("last_trade_price","REAL"),
        ("price_change_1d", "REAL"),
        ("price_change_1wk","REAL"),
        ("min_order_size",  "REAL"),
        ("enable_order_book","INTEGER DEFAULT 0"),
        ("neg_risk",        "INTEGER DEFAULT 0"),
        ("restricted",      "INTEGER DEFAULT 0"),
        ("automated",       "INTEGER DEFAULT 0"),
        ("start_date",      "TEXT"),
        ("end_date",        "TEXT"),
        ("tier",            "INTEGER DEFAULT 3"),
        ("first_seen_at",   "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("last_updated_at", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("closed_at",       "DATETIME NULL"),
    ]
    added = 0
    for col, coltype in markets_new_cols:
        if col not in markets_cols:
            try:
                conn.execute(f"ALTER TABLE markets ADD COLUMN {col} {coltype}")
                added += 1
            except sqlite3.OperationalError:
                pass

    if added:
        print(f"✅ markets table: added {added} missing columns")
    else:
        print("✅ markets table: already up to date")

    conn.commit()

    # -------------------------------------------------------
    # 5. Apply full v2 schema (creates new tables idempotently)
    # -------------------------------------------------------
    print("📐 Applying v2 schema (creates snapshots, order_book_snapshots, trades)...")
    with open(SCHEMA_PATH, "r") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                print(f"   ⚠️  Schema warning: {e}")

    conn.commit()

    # -------------------------------------------------------
    # 6. Verify
    # -------------------------------------------------------
    print("\n📊 FINAL TABLE VERIFICATION:")
    tables = ["events", "markets", "snapshots", "order_book_snapshots", "trades"]
    all_ok = True
    for t in tables:
        if table_exists(conn, t):
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            cols = get_existing_columns(conn, t)
            print(f"  ✅ {t:<30} {n:>10,} rows  ({len(cols)} columns)")
        else:
            print(f"  ❌ {t:<30} MISSING!")
            all_ok = False

    # Check key new columns exist
    snap_cols = get_existing_columns(conn, "snapshots")
    required = {"yes_price", "captured_at", "source", "best_bid", "volume_24hr"}
    missing = required - snap_cols
    if missing:
        print(f"\n  ❌ snapshots still missing columns: {missing}")
        all_ok = False
    else:
        print(f"\n  ✅ snapshots has all required v2 columns")

    market_cols = get_existing_columns(conn, "markets")
    if "tier" not in market_cols:
        print("  ❌ markets missing 'tier' column")
        all_ok = False
    else:
        print("  ✅ markets has 'tier' column")

    conn.execute("PRAGMA foreign_keys=ON")
    conn.close()

    print("\n" + "=" * 55)
    if all_ok:
        print("✅ Migration complete! Now restart the collector:")
        print("   launchctl unload ~/Library/LaunchAgents/com.polymarket.collector.plist")
        print("   launchctl load ~/Library/LaunchAgents/com.polymarket.collector.plist")
    else:
        print("❌ Migration had issues — check warnings above")
    print("=" * 55)


if __name__ == "__main__":
    migrate()
