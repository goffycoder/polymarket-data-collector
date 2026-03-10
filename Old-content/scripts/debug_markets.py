import httpx
import asyncio
import json

# TARGET IDs
EVENT_ID = "200499"
MARKET_ID = "1345810"

async def inspect():
    url = f"https://gamma-api.polymarket.com/events/{EVENT_ID}"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            print("Failed to fetch event.")
            return
        
        event_data = resp.json()
        
        # Find the specific market in the list of markets
        target_market = next((m for m in event_data['markets'] if m['id'] == MARKET_ID), None)
        
        if not target_market:
            print(f"Market {MARKET_ID} not found in this event.")
            return

        # CLEAN DATA CONSTRUCT (Focus on Volume & Liquidity)
        clean_json = {
            "metadata": {
                "event_title": event_data.get('title'),
                "market_question": target_market.get('question'),
                "status": "Closed" if target_market.get('closed') else "Active"
            },
            "liquidity_metrics": {
                "total_liquidity": target_market.get('liquidity'),
                "liquidity_num": target_market.get('liquidityNum'),
                "best_bid": target_market.get('bestBid'),
                "best_ask": target_market.get('bestAsk'),
                "spread": target_market.get('spread')
            },
            "volume_metrics": {
                "total_volume": target_market.get('volume'),
                "volume_24h": target_market.get('volume24hr'),
                "volume_1wk": target_market.get('volume1wk'),
                "last_trade_price": target_market.get('lastTradePrice')
            },
            "trading_keys": {
                "clob_token_ids": json.loads(target_market.get('clobTokenIds', '[]')),
                "outcome_prices": json.loads(target_market.get('outcomePrices', '[]'))
            }
        }

        print("\n--- TARGETED MARKET ANALYSIS ---")
        print(json.dumps(clean_json, indent=2))

if __name__ == "__main__":
    asyncio.run(inspect())