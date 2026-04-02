"""
audit_v2.py — Comprehensive data health check for Polymarket v2 collector.

Run with: python audit_v2.py
"""
import sqlite3
from datetime import datetime, timezone, timedelta

DB_PATH = "database/polymarket_state.db"


def fmt(n):
    return f"{n:,}"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc)

    print("=" * 64)
    print("  POLYMARKET V2 — DATA AUDIT REPORT")
    print(f"  {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 64)

    # ── TABLE ROW COUNTS ───────────────────────────────────────────
    print("\n📊 TABLE ROW COUNTS:")
    tables = ["events", "markets", "snapshots", "order_book_snapshots", "trades"]
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<30} {fmt(n):>10}")
        except Exception as e:
            print(f"  {t:<30}  ERROR: {e}")

    # ── EVENT STATUS ───────────────────────────────────────────────
    print("\n🔵 EVENTS: status distribution")
    rows = conn.execute(
        "SELECT status, COUNT(*) as n FROM events GROUP BY status ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r['status']:<12} {fmt(r['n']):>10}")

    # ── EVENT DATES ────────────────────────────────────────────────
    print("\n📅 EVENTS: date coverage (active only)")
    r = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(end_date) as have_end_date,
            COUNT(CASE WHEN end_date > ? THEN 1 END) as future_end,
            COUNT(CASE WHEN end_date <= ? AND end_date IS NOT NULL THEN 1 END) as past_end,
            MIN(end_date) as earliest_end,
            MAX(end_date) as latest_end
        FROM events WHERE status='active'
    """, (now.isoformat(), now.isoformat())).fetchone()
    print(f"  Total active:    {fmt(r['total'])}")
    print(f"  Have end_date:   {fmt(r['have_end_date'])}")
    print(f"  Future end_date: {fmt(r['future_end'])} ← live events")
    print(f"  Past end_date:   {fmt(r['past_end'])} ← possibly stale")
    print(f"  Date range:      {r['earliest_end']} → {r['latest_end']}")

    # ── TOP EVENTS ─────────────────────────────────────────────────
    print("\n🏆 TOP 10 ACTIVE EVENTS (by volume):")
    rows = conn.execute("""
        SELECT title, end_date, volume
        FROM events WHERE status='active'
        ORDER BY volume DESC LIMIT 10
    """).fetchall()
    for r in rows:
        ends = r['end_date'] or 'no end date'
        print(f"  ${r['volume']:>12.0f}  {ends[:10]}  {(r['title'] or '')[:50]}")

    # ── MARKET TIERS ───────────────────────────────────────────────
    print("\n🎯 MARKETS: tier × status")
    rows = conn.execute("""
        SELECT status, tier, COUNT(*) as n
        FROM markets GROUP BY status, tier ORDER BY status, tier
    """).fetchall()
    for r in rows:
        print(f"  {r['status']:<8}  T{r['tier']}  {fmt(r['n']):>10}")

    # ── MARKET DATES ───────────────────────────────────────────────
    print("\n📅 MARKETS: date coverage (active, Tier1)")
    r = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(end_date) as have_end_date,
            COUNT(CASE WHEN end_date > ? THEN 1 END) as future_end,
            COUNT(CASE WHEN end_date <= ? AND end_date IS NOT NULL THEN 1 END) as past_end,
            MIN(end_date) as earliest, MAX(end_date) as latest
        FROM markets WHERE status='active' AND tier=1
    """, (now.isoformat(), now.isoformat())).fetchone()
    print(f"  T1 active total: {fmt(r['total'])}")
    print(f"  Have end_date:   {fmt(r['have_end_date'])}")
    print(f"  Future end_date: {fmt(r['future_end'])} ← genuinely live")
    print(f"  Past end_date:   {fmt(r['past_end'])} ← pending close")
    print(f"  Date range:      {r['earliest']} → {r['latest']}")

    # ── SNAPSHOT RECENCY ───────────────────────────────────────────
    print("\n⏱  SNAPSHOTS: recency check")
    try:
        r = conn.execute("""
            SELECT
                COUNT(*) as total,
                MAX(captured_at) as latest,
                COUNT(CASE WHEN captured_at > ? THEN 1 END) as last_5min,
                COUNT(CASE WHEN captured_at > ? THEN 1 END) as last_1hr
            FROM snapshots
        """, (
            (now - timedelta(minutes=5)).isoformat(),
            (now - timedelta(hours=1)).isoformat()
        )).fetchone()
        print(f"  Total snapshots:    {fmt(r['total'])}")
        print(f"  Latest captured_at: {r['latest']}")
        print(f"  Last 5 minutes:     {fmt(r['last_5min'])}")
        print(f"  Last 1 hour:        {fmt(r['last_1hr'])}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── ORDER BOOK RECENCY ─────────────────────────────────────────
    print("\n📖 ORDER_BOOK_SNAPSHOTS: recency")
    try:
        r = conn.execute("""
            SELECT COUNT(*) as total, MAX(captured_at) as latest,
                   COUNT(CASE WHEN captured_at > ? THEN 1 END) as last_1hr
            FROM order_book_snapshots
        """, ((now - timedelta(hours=1)).isoformat(),)).fetchone()
        print(f"  Total:       {fmt(r['total'])}")
        print(f"  Latest:      {r['latest']}")
        print(f"  Last 1 hour: {fmt(r['last_1hr'])}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── TRADES RECENCY ─────────────────────────────────────────────
    print("\n💱 TRADES: recency")
    try:
        r = conn.execute("""
            SELECT COUNT(*) as total, MAX(trade_time) as latest,
                   COUNT(CASE WHEN trade_time > ? THEN 1 END) as last_1hr
            FROM trades
        """, ((now - timedelta(hours=1)).isoformat(),)).fetchone()
        print(f"  Total:       {fmt(r['total'])}")
        print(f"  Latest:      {r['latest']}")
        print(f"  Last 1 hour: {fmt(r['last_1hr'])}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── TTL DECAY CHECK ────────────────────────────────────────────
    print("\n🧹 TTL DECAY: closed market data age")
    cutoff = (now - timedelta(hours=24)).isoformat()
    try:
        r = conn.execute("""
            SELECT COUNT(*) as stale_snaps
            FROM snapshots
            WHERE captured_at < ?
              AND market_id IN (SELECT market_id FROM markets WHERE status='closed')
        """, (cutoff,)).fetchone()
        print(f"  Stale snapshots (>24h, closed markets): {fmt(r['stale_snaps'])}")
        if r['stale_snaps'] == 0:
            print("  ✅ TTL decay working — no stale closed-market data retained")
        else:
            print("  ⚠️  TTL hasn't run yet (runs every 30 min after first sync)")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── SOURCE BREAKDOWN ───────────────────────────────────────────
    print("\n📡 SNAPSHOT SOURCE BREAKDOWN:")
    try:
        rows = conn.execute("""
            SELECT source, COUNT(*) as n FROM snapshots GROUP BY source ORDER BY n DESC
        """).fetchall()
        for r in rows:
            print(f"  {r['source']:<8} {fmt(r['n']):>10}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n" + "=" * 64)
    conn.close()


if __name__ == "__main__":
    main()
