import sqlite3
import pandas as pd
from config.settings import DB_PATH

def audit_snapshots():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Count Total vs Unique (Market_id + Price)
    # This identifies "Stale" data where the price was identical across multiple timestamps
    query = """
    SELECT 
        market_id, 
        COUNT(*) as total_entries,
        COUNT(DISTINCT price) as unique_price_changes
    FROM snapshots 
    GROUP BY market_id
    ORDER BY total_entries DESC
    LIMIT 10
    """
    
    df = pd.read_sql_query(query, conn)
    
    print("📊 SNAPSHOT FIDELITY AUDIT")
    print("-" * 50)
    for _, row in df.iterrows():
        waste = row['total_entries'] - row['unique_price_changes']
        waste_pct = (waste / row['total_entries']) * 100
        print(f"Market ID: {row['market_id']}")
        print(f"  Total Snapshots: {row['total_entries']}")
        print(f"  Actual Price Moves: {row['unique_price_changes']}")
        print(f"  Waste (Stale Data): {waste} rows ({waste_pct:.1f}%)")
        print("-" * 30)

    conn.close()

if __name__ == "__main__":
    audit_snapshots()