import httpx, asyncio, sqlite3, json
from config.settings import GAMMA_API_URL, DB_PATH

async def expand_active_markets():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # SYSTEMATIC SCHEMA
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            market_id TEXT PRIMARY KEY,
            event_id TEXT,
            question TEXT,
            condition_id TEXT,
            yes_token_id TEXT,
            volume REAL DEFAULT 0,
            liquidity REAL DEFAULT 0,
            status TEXT DEFAULT 'active',
            FOREIGN KEY(event_id) REFERENCES events(event_id)
        )
    """)

    cursor.execute("SELECT event_id FROM events")
    clean_event_ids = {str(row[0]) for row in cursor.fetchall()}
    
    print(f"📡 Expanding markets... Checking against {len(clean_event_ids)} clean events.")
    
    all_markets_saved = 0
    offset = 0
    limit = 100

    async with httpx.AsyncClient() as client:
        while True:
            url = f"{GAMMA_API_URL}/markets?closed=false&limit={limit}&offset={offset}"
            try:
                response = await client.get(url)
                data = response.json()
            except: break
            if not data: break

            for m in data:
                events_list = m.get('events', [])
                potential_ids = [str(e.get('id')) for e in events_list if e.get('id')]
                if m.get('eventId'): potential_ids.append(str(m.get('eventId')))
                
                parent_event_id = next((eid for eid in potential_ids if eid in clean_event_ids), None)
                if not parent_event_id: continue

                yes_token = None
                try:
                    raw_tokens = m.get('clobTokenIds')
                    tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
                    raw_outcomes = m.get('outcomes')
                    outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

                    if tokens and outcomes:
                        if isinstance(tokens[0], str):
                            for idx, label in enumerate(outcomes):
                                if str(label).lower() == 'yes':
                                    yes_token = tokens[idx]
                                    break
                        elif isinstance(tokens[0], dict):
                            for t in tokens:
                                if str(t.get('outcome', '')).lower() == 'yes':
                                    yes_token = t.get('tokenId')
                                    break
                except: continue

                if yes_token:
                    # Use volumeNum and liquidityNum for high-precision ML data
                    cursor.execute("""
                        INSERT OR REPLACE INTO markets 
                        (market_id, event_id, question, condition_id, yes_token_id, volume, liquidity)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        str(m['id']), 
                        parent_event_id, 
                        m.get('question'), 
                        m.get('conditionId'), 
                        yes_token,
                        m.get('volumeNum', 0),
                        m.get('liquidityNum', 0)
                    ))
                    all_markets_saved += 1

            print(f"📦 Scanned {offset + len(data)}... Found {all_markets_saved} markets.")
            offset += limit
            if offset > 40000: break
            await asyncio.sleep(0.02)

    conn.commit()
    conn.close()