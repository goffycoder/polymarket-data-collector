import httpx, asyncio, json

async def discover_fields(market_id="1281029"):
    url = f"https://gamma-api.polymarket.com/markets/{market_id}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        data = resp.json()
        
        print(f"📦 FULL DATA PACKET FOR MARKET {market_id}:")
        print("="*50)
        # We print keys and a sample value to help us design the SQL table
        for key, value in data.items():
            print(f"{key:<25} | {type(value).__name__:<10} | {str(value)[:50]}")
            
if __name__ == "__main__":
    mid = input("Enter Market ID to inspect (default 1345810): ") or "1345810"
    asyncio.run(discover_fields(mid))