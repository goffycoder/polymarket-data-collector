import sqlite3, asyncio, time
from core.monitor import monitor_tier
from core.ingestor import run_maintenance_pipeline
from core.expand_markets import expand_active_markets
from config.settings import DB_PATH

def get_tiered_markets():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        # Tier 1: High Volume (Smart Money)
        cursor.execute("""
            SELECT market_id, yes_token_id FROM markets 
            WHERE status = 'active' AND volume > 500
        """)
        tier1 = cursor.fetchall()
        # Tier 2: Lower Volume
        cursor.execute("""
            SELECT market_id, yes_token_id FROM markets 
            WHERE status = 'active' AND volume <= 500
        """)
        tier2 = cursor.fetchall()
    except: return [], []
    finally: conn.close()
    return tier1, tier2

async def tier1_loop():
    while True:
        t1, _ = get_tiered_markets()
        if t1: await monitor_tier(t1, fetch_depth=True)
        await asyncio.sleep(60)

async def tier2_loop():
    while True:
        _, t2 = get_tiered_markets()
        if t2:
            for i in range(0, len(t2), 500):
                await monitor_tier(t2[i:i+500], fetch_depth=False)
                await asyncio.sleep(2)
        await asyncio.sleep(600)

async def discovery_loop():
    while True:
        await asyncio.sleep(1800)
        print(f"🔄 [{time.strftime('%H:%M:%S')}] DISCOVERY: Periodic Sync...")
        try:
            await run_maintenance_pipeline()
            await expand_active_markets()
        except: pass

async def main():
    print("🛠️  PHASE 1: Initializing Database & Registry...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY, title TEXT, slug TEXT, tags TEXT, 
            status TEXT DEFAULT 'active', volume REAL DEFAULT 0, 
            liquidity REAL DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, 
            closed_at DATETIME NULL
        )""")
    # UPDATED SCHEMA (Must match expand_markets.py)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            market_id TEXT PRIMARY KEY, event_id TEXT, question TEXT, 
            condition_id TEXT, yes_token_id TEXT, volume REAL DEFAULT 0, 
            liquidity REAL DEFAULT 0, status TEXT DEFAULT 'active',
            FOREIGN KEY(event_id) REFERENCES events(event_id)
        )""")
    conn.execute("CREATE TABLE IF NOT EXISTS order_books (market_id TEXT, best_bid REAL, best_ask REAL, spread REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, market_id TEXT, price REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.close()

    await run_maintenance_pipeline()
    await expand_active_markets()
    
    print("🚀 PHASE 2: Starting Monitoring Loops.")
    await asyncio.gather(tier1_loop(), tier2_loop(), discovery_loop())

if __name__ == "__main__":
    asyncio.run(main())