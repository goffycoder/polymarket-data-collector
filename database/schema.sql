-- =================================================================
-- POLYMARKET V2 CANONICAL SCHEMA
-- Single source of truth. Applied idempotently on every startup.
-- =================================================================

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- -----------------------------------------------------------------
-- 1. EVENTS
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    title           TEXT,
    description     TEXT,
    slug            TEXT,
    category        TEXT,
    tags            TEXT,
    status          TEXT DEFAULT 'active',
    volume          REAL DEFAULT 0,
    volume_24hr     REAL DEFAULT 0,
    volume_1wk      REAL DEFAULT 0,
    volume_1mo      REAL DEFAULT 0,
    liquidity       REAL DEFAULT 0,
    open_interest   REAL DEFAULT 0,
    comment_count   INTEGER DEFAULT 0,
    competitive     REAL DEFAULT 0,
    start_date      TEXT,
    end_date        TEXT,
    creation_date   TEXT,
    neg_risk        INTEGER DEFAULT 0,
    featured        INTEGER DEFAULT 0,
    restricted      INTEGER DEFAULT 0,
    first_seen_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at       DATETIME NULL
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC);

-- -----------------------------------------------------------------
-- 2. MARKETS
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS markets (
    market_id           TEXT PRIMARY KEY,
    event_id            TEXT,
    question            TEXT,
    description         TEXT,
    slug                TEXT,
    condition_id        TEXT,
    yes_token_id        TEXT,
    no_token_id         TEXT,
    outcomes            TEXT,
    outcome_prices      TEXT,
    volume              REAL DEFAULT 0,
    volume_24hr         REAL DEFAULT 0,
    volume_1wk          REAL DEFAULT 0,
    volume_1mo          REAL DEFAULT 0,
    liquidity           REAL DEFAULT 0,
    best_bid            REAL,
    best_ask            REAL,
    spread              REAL,
    last_trade_price    REAL,
    price_change_1d     REAL,
    price_change_1wk    REAL,
    min_tick_size       REAL,
    min_order_size      REAL,
    accepts_orders      INTEGER DEFAULT 0,
    enable_order_book   INTEGER DEFAULT 0,
    neg_risk            INTEGER DEFAULT 0,
    restricted          INTEGER DEFAULT 0,
    automated           INTEGER DEFAULT 0,
    outcome             TEXT NULL,          -- 'YES' | 'NO' | 'N/A' — set on resolution
    start_date          TEXT,
    end_date            TEXT,
    tier                INTEGER DEFAULT 3,
    status              TEXT DEFAULT 'active',
    first_seen_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at           DATETIME NULL
);

CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_tier ON markets(tier);
CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume DESC);
CREATE INDEX IF NOT EXISTS idx_markets_yes_token ON markets(yes_token_id);

-- -----------------------------------------------------------------
-- 2b. MARKET_RESOLUTIONS — ground truth labels for ML
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_resolutions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL,
    condition_id    TEXT,
    outcome         TEXT NOT NULL,      -- 'YES' | 'NO' | 'N/A'
    final_price     REAL,              -- 1.0 = YES won, 0.0 = NO won
    resolved_at     DATETIME NOT NULL,
    source          TEXT DEFAULT 'ws'  -- 'ws' | 'api'
);

CREATE INDEX IF NOT EXISTS idx_resolutions_market ON market_resolutions(market_id);
CREATE INDEX IF NOT EXISTS idx_resolutions_time ON market_resolutions(resolved_at DESC);

-- -----------------------------------------------------------------
-- 3. SNAPSHOTS — rich ML time-series
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id           TEXT NOT NULL,
    captured_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    yes_price           REAL,
    no_price            REAL,
    last_trade_price    REAL,
    mid_price           REAL,
    best_bid            REAL,
    best_ask            REAL,
    spread              REAL,
    volume_total        REAL,
    volume_24hr         REAL,
    volume_1wk          REAL,
    volume_1mo          REAL,
    liquidity           REAL,
    price_change_1d     REAL,
    price_change_1wk    REAL,
    source              TEXT DEFAULT 'gamma',
    FOREIGN KEY(market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_market_time ON snapshots(market_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(captured_at DESC);

-- -----------------------------------------------------------------
-- 4. ORDER_BOOK_SNAPSHOTS — full depth (Tier 1 only)
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id   TEXT NOT NULL,
    token_id    TEXT,
    captured_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    bids_json   TEXT,
    asks_json   TEXT,
    best_bid    REAL,
    best_ask    REAL,
    spread      REAL,
    depth_bids  INTEGER,
    depth_asks  INTEGER,
    bid_volume  REAL,
    ask_volume  REAL,
    source      TEXT DEFAULT 'clob'
);

CREATE INDEX IF NOT EXISTS idx_ob_market_time ON order_book_snapshots(market_id, captured_at DESC);

-- -----------------------------------------------------------------
-- 5. TRADES — individual matched trades
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    trade_id        TEXT PRIMARY KEY,
    market_id       TEXT NOT NULL,
    token_id        TEXT,
    side            TEXT,
    price           REAL,
    size            REAL,
    fee_rate_bps    TEXT,
    trade_time      DATETIME,
    captured_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    source          TEXT DEFAULT 'clob'
);

CREATE INDEX IF NOT EXISTS idx_trades_market_time ON trades(market_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(trade_time DESC);