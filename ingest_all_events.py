import httpx
import asyncio
import sqlite3
from config.settings import GAMMA_API_URL, DB_PATH

# 1. SYSTEMATIC EXCLUSION LIST (Based on your Tag Analysis)
# Any event containing these tags will be ignored.
TRASH_TAGS = {
    "Sports", "Games", "Soccer", "Basketball", "Hockey", "NHL", 
    "NCAA", "Esports", "Tennis", "NCAA Basketball", "CWBB", 
    "Argentina Primera División", "EFL Championship", "counter strike 2"
}

async def ingest_all_active_events():
    print("🚀 Starting Clean Systematic Ingestion (No Sports/Games)...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Initialize Schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            title TEXT,
            slug TEXT,
            category TEXT,
            subcategory TEXT,
            status TEXT DEFAULT 'active',
            volume REAL,
            liquidity REAL,
            tags TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    all_events = []
    limit = 100
    offset = 0
    saved_count = 0
    skipped_count = 0
    
    async with httpx.AsyncClient() as client:
        while True:
            url = f"{GAMMA_API_URL}/events?active=true&closed=false&limit={limit}&offset={offset}"
            response = await client.get(url)
            data = response.json()
            
            if not data or len(data) == 0:
                break
                
            for event in data:
                tag_list = [t.get('label') for t in event.get('tags', [])]
                
                # 2. SYSTEMATIC FILTERING LOGIC
                if any(tag in TRASH_TAGS for tag in tag_list):
                    skipped_count += 1
                    continue # Skip saving this to the DB
                
                # If it passed the filter, save it
                tags_str = ", ".join(tag_list)
                cursor.execute("""
                    INSERT OR REPLACE INTO events 
                    (event_id, title, slug, category, subcategory, status, volume, liquidity, tags, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    event.get('id'), event.get('title'), event.get('slug'),
                    event.get('category'), event.get('subcategory'), 'active',
                    event.get('volume', 0), event.get('liquidity', 0), tags_str
                ))
                saved_count += 1

            print(f"📡 Processed offset {offset}... (Saved: {saved_count}, Skipped: {skipped_count})")
            offset += limit
            await asyncio.sleep(0.05)

    conn.commit()
    conn.close()
    print(f"✅ Ingestion Complete. Saved {saved_count} clean events. Ignored {skipped_count} sports events.")

if __name__ == "__main__":
    asyncio.run(ingest_all_active_events())