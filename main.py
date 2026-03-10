# main.py
import asyncio
from core.ingestor import run_maintenance_pipeline
from core.expand_markets import expand_active_markets

async def main():
    # 1. Update the "Source of Truth" (Events)
    await run_maintenance_pipeline()
    
    # 2. Get the Token IDs for tracking (Markets)
    await expand_active_markets()
    
    print("🚀 System is synchronized. Ready for Analysis.")

if __name__ == "__main__":
    asyncio.run(main())