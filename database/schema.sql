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

CREATE TABLE IF NOT EXISTS validation_runs (
    validation_run_id      TEXT PRIMARY KEY,
    replay_run_id          TEXT,
    validation_type        TEXT NOT NULL,
    split_name             TEXT,
    status                 TEXT NOT NULL,
    config_json            TEXT,
    metrics_json           TEXT,
    output_path            TEXT,
    notes                  TEXT,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at           DATETIME
);

CREATE INDEX IF NOT EXISTS idx_validation_runs_replay_time
    ON validation_runs(replay_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS backtest_artifacts (
    backtest_artifact_id   TEXT PRIMARY KEY,
    replay_run_id          TEXT,
    artifact_type          TEXT NOT NULL,
    status                 TEXT NOT NULL,
    config_json            TEXT,
    summary_json           TEXT,
    output_path            TEXT,
    notes                  TEXT,
    created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at           DATETIME
);

CREATE INDEX IF NOT EXISTS idx_backtest_artifacts_replay_time
    ON backtest_artifacts(replay_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS backfill_requests (
    backfill_request_id     TEXT PRIMARY KEY,
    source_system           TEXT NOT NULL,
    start_time              DATETIME NOT NULL,
    end_time                DATETIME NOT NULL,
    request_status          TEXT NOT NULL,
    priority                TEXT DEFAULT 'normal',
    requested_by            TEXT,
    reason                  TEXT,
    request_payload         TEXT,
    output_path             TEXT,
    notes                   TEXT,
    created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at            DATETIME
);

CREATE INDEX IF NOT EXISTS idx_backfill_requests_source_time
    ON backfill_requests(source_system, created_at DESC);

-- -----------------------------------------------------------------
-- 7. PHASE 6 ML V1 FOUNDATIONS
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feature_materialization_runs (
    materialization_run_id   TEXT PRIMARY KEY,
    feature_schema_version   TEXT NOT NULL,
    materialization_mode     TEXT NOT NULL,
    start_time               DATETIME NOT NULL,
    end_time                 DATETIME NOT NULL,
    source_row_count         INTEGER DEFAULT 0,
    feature_row_count        INTEGER DEFAULT 0,
    dataset_hash             TEXT,
    output_path              TEXT,
    status                   TEXT NOT NULL,
    notes                    TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at             DATETIME
);

CREATE INDEX IF NOT EXISTS idx_feature_materialization_runs_time
    ON feature_materialization_runs(feature_schema_version, created_at DESC);

CREATE TABLE IF NOT EXISTS model_registry (
    model_version            TEXT PRIMARY KEY,
    model_name               TEXT NOT NULL,
    registry_version         TEXT NOT NULL,
    artifact_path            TEXT NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    training_dataset_hash    TEXT NOT NULL,
    calibration_metadata     TEXT,
    deployment_status        TEXT NOT NULL,
    shadow_enabled           INTEGER DEFAULT 0,
    deployed_at              DATETIME,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes                    TEXT
);

CREATE INDEX IF NOT EXISTS idx_model_registry_name_time
    ON model_registry(model_name, created_at DESC);

CREATE TABLE IF NOT EXISTS shadow_model_scores (
    shadow_score_id          TEXT PRIMARY KEY,
    model_version            TEXT NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    candidate_id             TEXT NOT NULL,
    alert_id                 TEXT,
    market_id                TEXT NOT NULL,
    score_value              REAL NOT NULL,
    score_label              TEXT,
    score_metadata           TEXT,
    scored_at                DATETIME NOT NULL,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shadow_model_scores_candidate_time
    ON shadow_model_scores(candidate_id, scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_shadow_model_scores_model_time
    ON shadow_model_scores(model_version, scored_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_shadow_model_scores_model_candidate_time
    ON shadow_model_scores(model_version, candidate_id, scored_at);

CREATE TABLE IF NOT EXISTS model_evaluation_runs (
    evaluation_run_id        TEXT PRIMARY KEY,
    model_version            TEXT NOT NULL,
    evaluation_version       TEXT NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    dataset_hash             TEXT NOT NULL,
    start_time               TEXT NOT NULL,
    end_time                 TEXT NOT NULL,
    train_row_count          INTEGER DEFAULT 0,
    validation_row_count     INTEGER DEFAULT 0,
    test_row_count           INTEGER DEFAULT 0,
    labeled_row_count        INTEGER DEFAULT 0,
    output_path              TEXT,
    summary_json             TEXT NOT NULL,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_model_evaluation_runs_model_time
    ON model_evaluation_runs(model_version, created_at DESC);

CREATE TABLE IF NOT EXISTS calibration_profiles (
    calibration_profile_id   TEXT PRIMARY KEY,
    model_version            TEXT NOT NULL,
    calibration_version      TEXT NOT NULL,
    profile_scope            TEXT NOT NULL,
    profile_key              TEXT NOT NULL,
    sample_count             INTEGER DEFAULT 0,
    positive_rate            REAL,
    watch_threshold          REAL,
    actionable_threshold     REAL,
    critical_threshold       REAL,
    metadata_json            TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_calibration_profiles_model_time
    ON calibration_profiles(model_version, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calibration_profiles_scope_key
    ON calibration_profiles(profile_scope, profile_key, created_at DESC);

-- -----------------------------------------------------------------
-- 8. PHASE 3 ONLINE STATE / CANDIDATE DETECTION
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

CREATE TABLE IF NOT EXISTS detector_checkpoints (
    checkpoint_key          TEXT PRIMARY KEY,
    detector_version        TEXT NOT NULL,
    source_system           TEXT NOT NULL,
    partition_path          TEXT NOT NULL,
    file_offset             INTEGER DEFAULT 0,
    last_ordering_key       TEXT,
    last_captured_at        DATETIME,
    updated_at              DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detector_checkpoints_source
    ON detector_checkpoints(detector_version, source_system, updated_at DESC);

-- -----------------------------------------------------------------
-- 8. PHASE 4 EVIDENCE / ALERTS / ANALYST WORKFLOW
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_workflow_versions (
    workflow_version         TEXT PRIMARY KEY,
    evidence_schema_version  TEXT NOT NULL,
    alert_schema_version     TEXT NOT NULL,
    delivery_channels        TEXT NOT NULL,
    notes                    TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS evidence_queries (
    evidence_query_id        TEXT PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    alert_id                 TEXT,
    provider_name            TEXT NOT NULL,
    provider_query_type      TEXT NOT NULL,
    provider_query_text      TEXT NOT NULL,
    request_started_at       DATETIME NOT NULL,
    response_completed_at    DATETIME,
    latency_ms               REAL,
    result_count             INTEGER DEFAULT 0,
    query_status             TEXT NOT NULL,
    timeout_seconds          INTEGER,
    raw_response_metadata    TEXT,
    error_message            TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_evidence_queries_candidate_time
    ON evidence_queries(candidate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_evidence_queries_provider_time
    ON evidence_queries(provider_name, created_at DESC);

CREATE TABLE IF NOT EXISTS evidence_snapshots (
    evidence_snapshot_id     TEXT PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    alert_id                 TEXT,
    snapshot_time            DATETIME NOT NULL,
    evidence_state           TEXT NOT NULL,
    provider_summary         TEXT NOT NULL,
    confidence_modifier      REAL,
    cache_key                TEXT,
    freshness_seconds        INTEGER,
    metadata_json            TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_evidence_snapshots_candidate_time
    ON evidence_snapshots(candidate_id, snapshot_time DESC);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id                 TEXT PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    severity                 TEXT NOT NULL,
    alert_status             TEXT NOT NULL,
    title                    TEXT,
    rendered_payload         TEXT NOT NULL,
    workflow_version         TEXT NOT NULL,
    detector_version         TEXT,
    feature_schema_version   TEXT,
    evidence_snapshot_id     TEXT,
    suppression_key          TEXT,
    suppression_state        TEXT,
    first_delivery_at        DATETIME,
    last_delivery_at         DATETIME,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_candidate_time
    ON alerts(candidate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_status_time
    ON alerts(alert_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity_time
    ON alerts(severity, created_at DESC);

CREATE TABLE IF NOT EXISTS alert_delivery_attempts (
    delivery_attempt_id      TEXT PRIMARY KEY,
    alert_id                 TEXT NOT NULL,
    delivery_channel         TEXT NOT NULL,
    attempt_number           INTEGER NOT NULL,
    delivery_status          TEXT NOT NULL,
    provider_message_id      TEXT,
    request_payload          TEXT,
    response_metadata        TEXT,
    attempted_at             DATETIME NOT NULL,
    completed_at             DATETIME,
    error_message            TEXT,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_delivery_attempts_alert_time
    ON alert_delivery_attempts(alert_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_delivery_attempts_channel_time
    ON alert_delivery_attempts(delivery_channel, attempted_at DESC);

CREATE TABLE IF NOT EXISTS analyst_feedback (
    feedback_id              TEXT PRIMARY KEY,
    alert_id                 TEXT NOT NULL,
    action_type              TEXT NOT NULL,
    actor                    TEXT,
    notes                    TEXT,
    follow_up_at             DATETIME,
    created_at               DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_analyst_feedback_alert_time
    ON analyst_feedback(alert_id, created_at DESC);

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
