-- 1. Events: The Parent Container
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    title TEXT,
    slug TEXT,
    tags TEXT,
    status TEXT DEFAULT 'active', -- 'active' or 'closed'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at DATETIME NULL        -- Populated when event disappears from active API
);

-- 2. Markets: The actual tradeable contracts
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
);

-- 3. Snapshots: The ML Time-series data
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT,
    price REAL,
    volume REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(market_id) REFERENCES markets(market_id)
);

CREATE TABLE IF NOT EXISTS order_books (
    market_id TEXT,
    best_bid REAL,
    best_ask REAL,
    spread REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);