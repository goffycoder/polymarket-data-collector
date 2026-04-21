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
    tag_ids         TEXT,
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
CREATE INDEX IF NOT EXISTS idx_markets_no_token ON markets(no_token_id);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);

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
-- 4b. UNIVERSE_REVIEW_CANDIDATES — excluded-but-interesting events
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS universe_review_candidates (
    event_id            TEXT PRIMARY KEY,
    event_slug          TEXT,
    event_title         TEXT,
    event_liquidity     REAL,
    event_volume        REAL,
    matched_keywords    TEXT,
    matched_tag_ids     TEXT,
    reason              TEXT,
    generated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_universe_review_generated_at ON universe_review_candidates(generated_at DESC);

-- -----------------------------------------------------------------
-- 5. TRADES — individual matched trades
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    trade_id        TEXT PRIMARY KEY,
    market_id       TEXT NOT NULL,
    token_id        TEXT,
    asset_id        TEXT,
    condition_id    TEXT,
    proxy_wallet    TEXT,
    transaction_hash TEXT,
    outcome_side    TEXT,
    side            TEXT,
    price           REAL,
    size            REAL,
    usdc_notional   REAL,
    fee_rate_bps    TEXT,
    trade_time      DATETIME,
    captured_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    source          TEXT DEFAULT 'clob',
    dedupe_key      TEXT,
    source_priority INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_trades_market_time ON trades(market_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_condition_time ON trades(condition_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_asset_time ON trades(asset_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet_time ON trades(proxy_wallet, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_dedupe_key ON trades(dedupe_key);

-- -----------------------------------------------------------------
-- 6. PHASE 2 DURABLE DATA PLANE METADATA
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_archive_manifests (
    partition_path       TEXT PRIMARY KEY,
    source_system        TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    schema_version       TEXT NOT NULL,
    row_count            INTEGER DEFAULT 0,
    byte_count           INTEGER DEFAULT 0,
    first_captured_at    DATETIME,
    last_captured_at     DATETIME,
    last_envelope_id     TEXT,
    last_updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_archive_source_time
    ON raw_archive_manifests(source_system, last_captured_at DESC);

CREATE TABLE IF NOT EXISTS detector_input_manifests (
    partition_path       TEXT PRIMARY KEY,
    source_system        TEXT NOT NULL,
    entity_type          TEXT NOT NULL,
    schema_version       TEXT NOT NULL,
    row_count            INTEGER DEFAULT 0,
    byte_count           INTEGER DEFAULT 0,
    first_captured_at    DATETIME,
    last_captured_at     DATETIME,
    last_ordering_key    TEXT,
    last_updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detector_input_source_time
    ON detector_input_manifests(source_system, last_captured_at DESC);

CREATE TABLE IF NOT EXISTS schema_versions (
    component            TEXT PRIMARY KEY,
    schema_version       TEXT NOT NULL,
    notes                TEXT,
    updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS replay_runs (
    replay_run_id        TEXT PRIMARY KEY,
    source_system        TEXT NOT NULL,
    start_time           DATETIME NOT NULL,
    end_time             DATETIME NOT NULL,
    status               TEXT NOT NULL,
    raw_partitions_touched INTEGER DEFAULT 0,
    raw_rows_scanned     INTEGER DEFAULT 0,
    rows_republished     INTEGER DEFAULT 0,
    output_path          TEXT,
    notes                TEXT,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at         DATETIME
);

CREATE INDEX IF NOT EXISTS idx_replay_runs_source_time
    ON replay_runs(source_system, created_at DESC);

-- -----------------------------------------------------------------
-- 7. PHASE 3 ONLINE STATE / CANDIDATE DETECTION
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS detector_versions (
    detector_version        TEXT PRIMARY KEY,
    feature_schema_version  TEXT NOT NULL,
    state_backend           TEXT NOT NULL,
    notes                   TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used_at            DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_episodes (
    episode_id               TEXT PRIMARY KEY,
    market_id                TEXT NOT NULL,
    event_id                 TEXT,
    event_family_id          TEXT,
    rule_family              TEXT NOT NULL,
    episode_start_event_time DATETIME NOT NULL,
    episode_end_event_time   DATETIME NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    detector_version         TEXT NOT NULL,
    episode_status           TEXT DEFAULT 'candidate',
    metadata_json            TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signal_episodes_market_time
    ON signal_episodes(market_id, episode_end_event_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_episodes_event_time
    ON signal_episodes(event_id, episode_end_event_time DESC);

CREATE TABLE IF NOT EXISTS signal_candidates (
    candidate_id             TEXT PRIMARY KEY,
    episode_id               TEXT NOT NULL,
    market_id                TEXT NOT NULL,
    event_id                 TEXT,
    event_family_id          TEXT,
    trigger_time             DATETIME NOT NULL,
    episode_start_event_time DATETIME NOT NULL,
    episode_end_event_time   DATETIME NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    detector_version         TEXT NOT NULL,
    triggering_rules         TEXT NOT NULL,
    cooldown_state           TEXT,
    feature_snapshot         TEXT NOT NULL,
    severity_score           REAL,
    emitted                  INTEGER DEFAULT 1,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signal_candidates_market_time
    ON signal_candidates(market_id, trigger_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_candidates_event_time
    ON signal_candidates(event_id, trigger_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_candidates_detector_time
    ON signal_candidates(detector_version, trigger_time DESC);

CREATE TABLE IF NOT EXISTS signal_features (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id             TEXT NOT NULL,
    episode_id               TEXT NOT NULL,
    market_id                TEXT NOT NULL,
    feature_name             TEXT NOT NULL,
    feature_value            REAL,
    feature_schema_version   TEXT NOT NULL,
    observed_at              DATETIME NOT NULL,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signal_features_candidate
    ON signal_features(candidate_id);
CREATE INDEX IF NOT EXISTS idx_signal_features_market_time
    ON signal_features(market_id, observed_at DESC);

CREATE VIEW IF NOT EXISTS canonical_trades AS
SELECT
    trade_id,
    market_id,
    token_id,
    asset_id,
    condition_id,
    proxy_wallet,
    transaction_hash,
    outcome_side,
    side,
    price,
    size,
    usdc_notional,
    fee_rate_bps,
    trade_time,
    captured_at,
    source,
    dedupe_key,
    source_priority
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(NULLIF(t.dedupe_key, ''), t.trade_id)
            ORDER BY t.source_priority DESC, t.captured_at DESC, t.trade_id DESC
        ) AS dedupe_rank
    FROM trades t
)
WHERE dedupe_rank = 1;
