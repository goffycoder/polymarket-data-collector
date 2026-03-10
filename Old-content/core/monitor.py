import httpx, asyncio, sqlite3, json
from config.settings import CLOB_API_URL, DB_PATH

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Strictly control concurrency: No more than 20 simultaneous requests
SEMAPHORE = asyncio.Semaphore(20)

async def fetch_market_data(client, mid, token_id, fetch_depth=False):
    async with SEMAPHORE:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            # 1. Fetch Current Price
            price_url = f"{CLOB_API_URL}/price?token_id={token_id}&side=buy"
            resp = await client.get(price_url)
            if resp.status_code == 200:
                price = resp.json().get('price')
                if price:
                    cursor.execute("INSERT INTO snapshots (market_id, price) VALUES (?, ?)", (mid, float(price)))

            # 2. Fetch Order Book (Only for Tier 1)
            if fetch_depth:
                book_url = f"{CLOB_API_URL}/book?token_id={token_id}"
                resp = await client.get(book_url)
                if resp.status_code == 200:
                    book = resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    if bids and asks:
                        best_bid = float(bids[0]['price'])
                        best_ask = float(asks[0]['price'])
                        cursor.execute("""
                            INSERT INTO order_books (market_id, best_bid, best_ask, spread)
                            VALUES (?, ?, ?, ?)
                        """, (mid, best_bid, best_ask, best_ask - best_bid))
            
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

async def monitor_tier(market_list, fetch_depth=False):
    async with httpx.AsyncClient(headers=HEADERS, timeout=10.0) as client:
        tasks = [fetch_market_data(client, mid, tid, fetch_depth) for mid, tid in market_list]
        await asyncio.gather(*tasks)