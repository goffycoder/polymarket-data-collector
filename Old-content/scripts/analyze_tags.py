import sqlite3
from collections import Counter

def analyze_real_tags():
    conn = sqlite3.connect("database/polymarket_state.db")
    cursor = conn.cursor()
    
    # Fetch all the tags strings we saved
    cursor.execute("SELECT tags FROM events")
    rows = cursor.fetchall()
    conn.close()

    all_tags = []
    for row in rows:
        if row[0]:
            # Split the comma-separated tags and clean them
            tags = [t.strip() for t in row[0].split(',')]
            all_tags.extend(tags)

    # Count the most common tags
    tag_counts = Counter(all_tags)
    
    print("📊 Top 50 Systematic Tags (Actual Data):\n")
    print(f"{'Tag Label':<30} | {'Event Count':<10}")
    print("-" * 45)
    for tag, count in tag_counts.most_common(50):
        print(f"{tag:<30} | {count:<10}")

if __name__ == "__main__":
    analyze_real_tags()