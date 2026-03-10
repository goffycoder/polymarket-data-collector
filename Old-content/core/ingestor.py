import httpx, asyncio, sqlite3, datetime
from config.settings import GAMMA_API_URL, DB_PATH

TRASH_TAGS = {    "Sports", "Games", "Soccer", "Basketball", "Hockey", "NHL", 
    "NCAA", "Esports", "Tennis", "NCAA Basketball", "CWBB", 
    "Argentina Primera División", "EFL Championship", "counter strike 2"}

async def run_maintenance_pipeline():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # --- 0. INITIALIZE SCHEMA (Create tables if they were deleted) ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            title TEXT,
            slug TEXT,
            tags TEXT,
            status TEXT DEFAULT 'active',
            volume REAL DEFAULT 0,
            liquidity REAL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at DATETIME NULL
        )
    """)
    conn.commit()

    # --- A. PURGE EXPIRED DATA (1-Day Decay) ---
    one_day_ago = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()
    cursor.execute("DELETE FROM events WHERE status = 'closed' AND closed_at < ?", (one_day_ago,))
    
    # --- B. FETCH ACTIVE DATA ---
    all_active_ids = []
    print("📡 Fetching active events from Gamma...")
    
    async with httpx.AsyncClient() as client:
        offset = 0
        while True:
            url = f"{GAMMA_API_URL}/events?active=true&closed=false&limit=100&offset={offset}"
            try:
                response = await client.get(url)
                data = response.json()
            except Exception as e:
                print(f"Connection Error: {e}")
                break
                
            if not data or len(data) == 0: break
            
            for event in data:
                tag_list = [t.get('label') for t in event.get('tags', [])]
                if any(tag in TRASH_TAGS for tag in tag_list): continue
                
                all_active_ids.append(event['id'])
                cursor.execute("""
                    INSERT OR REPLACE INTO events (event_id, title, slug, tags, status, volume, liquidity)
                    VALUES (?, ?, ?, ?, 'active', ?, ?)
                """, (event['id'], event['title'], event['slug'], ", ".join(tag_list),event.get('volume', 0),
                    event.get('liquidity', 0)))
            
            offset += 100
            await asyncio.sleep(0.05)

    # --- C. MARK CONCLUDED EVENTS (Transition Logic) ---
    if all_active_ids:
        placeholders = ', '.join(['?'] * len(all_active_ids))
        cursor.execute(f"""
            UPDATE events SET status = 'closed', closed_at = CURRENT_TIMESTAMP 
            WHERE status = 'active' AND event_id NOT IN ({placeholders})
        """, all_active_ids)
    
    conn.commit()
    conn.close()
    print(f"✅ Pipeline Complete: {len(all_active_ids)} events currently active.")