-- =================================================================
-- POLYMARKET V2 / PHASE 2 POSTGRESQL SCHEMA
-- Canonical target schema for local PostgreSQL cutover.
-- =================================================================

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    title           TEXT,
    description     TEXT,
    slug            TEXT,
    category        TEXT,
    tags            TEXT,
    tag_ids         TEXT,
    status          TEXT DEFAULT 'active',
    volume          DOUBLE PRECISION DEFAULT 0,
    volume_24hr     DOUBLE PRECISION DEFAULT 0,
    volume_1wk      DOUBLE PRECISION DEFAULT 0,
    volume_1mo      DOUBLE PRECISION DEFAULT 0,
    liquidity       DOUBLE PRECISION DEFAULT 0,
    open_interest   DOUBLE PRECISION DEFAULT 0,
    comment_count   INTEGER DEFAULT 0,
    competitive     DOUBLE PRECISION DEFAULT 0,
    start_date      TEXT,
    end_date        TEXT,
    creation_date   TEXT,
    neg_risk        INTEGER DEFAULT 0,
    featured        INTEGER DEFAULT 0,
    restricted      INTEGER DEFAULT 0,
    first_seen_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    closed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC);

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
    volume              DOUBLE PRECISION DEFAULT 0,
    volume_24hr         DOUBLE PRECISION DEFAULT 0,
    volume_1wk          DOUBLE PRECISION DEFAULT 0,
    volume_1mo          DOUBLE PRECISION DEFAULT 0,
    liquidity           DOUBLE PRECISION DEFAULT 0,
    best_bid            DOUBLE PRECISION,
    best_ask            DOUBLE PRECISION,
    spread              DOUBLE PRECISION,
    last_trade_price    DOUBLE PRECISION,
    price_change_1d     DOUBLE PRECISION,
    price_change_1wk    DOUBLE PRECISION,
    min_tick_size       DOUBLE PRECISION,
    min_order_size      DOUBLE PRECISION,
    accepts_orders      INTEGER DEFAULT 0,
    enable_order_book   INTEGER DEFAULT 0,
    neg_risk            INTEGER DEFAULT 0,
    restricted          INTEGER DEFAULT 0,
    automated           INTEGER DEFAULT 0,
    outcome             TEXT,
    start_date          TEXT,
    end_date            TEXT,
    tier                INTEGER DEFAULT 3,
    status              TEXT DEFAULT 'active',
    first_seen_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    closed_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_tier ON markets(tier);
CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume DESC);
CREATE INDEX IF NOT EXISTS idx_markets_yes_token ON markets(yes_token_id);
CREATE INDEX IF NOT EXISTS idx_markets_no_token ON markets(no_token_id);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);

CREATE TABLE IF NOT EXISTS market_resolutions (
    id              BIGSERIAL PRIMARY KEY,
    market_id       TEXT NOT NULL,
    condition_id    TEXT,
    outcome         TEXT NOT NULL,
    final_price     DOUBLE PRECISION,
    resolved_at     TIMESTAMPTZ NOT NULL,
    source          TEXT DEFAULT 'ws'
);

CREATE INDEX IF NOT EXISTS idx_resolutions_market ON market_resolutions(market_id);
CREATE INDEX IF NOT EXISTS idx_resolutions_time ON market_resolutions(resolved_at DESC);

CREATE TABLE IF NOT EXISTS snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    market_id           TEXT NOT NULL,
    captured_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    yes_price           DOUBLE PRECISION,
    no_price            DOUBLE PRECISION,
    last_trade_price    DOUBLE PRECISION,
    mid_price           DOUBLE PRECISION,
    best_bid            DOUBLE PRECISION,
    best_ask            DOUBLE PRECISION,
    spread              DOUBLE PRECISION,
    volume_total        DOUBLE PRECISION,
    volume_24hr         DOUBLE PRECISION,
    volume_1wk          DOUBLE PRECISION,
    volume_1mo          DOUBLE PRECISION,
    liquidity           DOUBLE PRECISION,
    price_change_1d     DOUBLE PRECISION,
    price_change_1wk    DOUBLE PRECISION,
    source              TEXT DEFAULT 'gamma'
);

CREATE INDEX IF NOT EXISTS idx_snapshots_market_time ON snapshots(market_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(captured_at DESC);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    market_id   TEXT NOT NULL,
    token_id    TEXT,
    captured_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    bids_json   TEXT,
    asks_json   TEXT,
    best_bid    DOUBLE PRECISION,
    best_ask    DOUBLE PRECISION,
    spread      DOUBLE PRECISION,
    depth_bids  INTEGER,
    depth_asks  INTEGER,
    bid_volume  DOUBLE PRECISION,
    ask_volume  DOUBLE PRECISION,
    source      TEXT DEFAULT 'clob'
);

CREATE INDEX IF NOT EXISTS idx_ob_market_time ON order_book_snapshots(market_id, captured_at DESC);

CREATE TABLE IF NOT EXISTS universe_review_candidates (
    event_id            TEXT PRIMARY KEY,
    event_slug          TEXT,
    event_title         TEXT,
    event_liquidity     DOUBLE PRECISION,
    event_volume        DOUBLE PRECISION,
    matched_keywords    TEXT,
    matched_tag_ids     TEXT,
    reason              TEXT,
    generated_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_universe_review_generated_at ON universe_review_candidates(generated_at DESC);

CREATE TABLE IF NOT EXISTS trades (
    trade_id         TEXT PRIMARY KEY,
    market_id        TEXT NOT NULL,
    token_id         TEXT,
    asset_id         TEXT,
    condition_id     TEXT,
    proxy_wallet     TEXT,
    transaction_hash TEXT,
    outcome_side     TEXT,
    side             TEXT,
    price            DOUBLE PRECISION,
    size             DOUBLE PRECISION,
    usdc_notional    DOUBLE PRECISION,
    fee_rate_bps     TEXT,
    trade_time       TIMESTAMPTZ,
    captured_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    source           TEXT DEFAULT 'clob',
    dedupe_key       TEXT,
    source_priority  INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_trades_market_time ON trades(market_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_condition_time ON trades(condition_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_asset_time ON trades(asset_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet_time ON trades(proxy_wallet, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_dedupe_key ON trades(dedupe_key);

CREATE TABLE IF NOT EXISTS raw_archive_manifests (
    partition_path       TEXT PRIMARY KEY,
    source_system        TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    schema_version       TEXT NOT NULL,
    row_count            INTEGER DEFAULT 0,
    byte_count           BIGINT DEFAULT 0,
    first_captured_at    TIMESTAMPTZ,
    last_captured_at     TIMESTAMPTZ,
    last_envelope_id     TEXT,
    last_updated_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_archive_source_time
    ON raw_archive_manifests(source_system, last_captured_at DESC);

CREATE TABLE IF NOT EXISTS detector_input_manifests (
    partition_path       TEXT PRIMARY KEY,
    source_system        TEXT NOT NULL,
    entity_type          TEXT NOT NULL,
    schema_version       TEXT NOT NULL,
    row_count            INTEGER DEFAULT 0,
    byte_count           BIGINT DEFAULT 0,
    first_captured_at    TIMESTAMPTZ,
    last_captured_at     TIMESTAMPTZ,
    last_ordering_key    TEXT,
    last_updated_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detector_input_source_time
    ON detector_input_manifests(source_system, last_captured_at DESC);

CREATE TABLE IF NOT EXISTS schema_versions (
    component            TEXT PRIMARY KEY,
    schema_version       TEXT NOT NULL,
    notes                TEXT,
    updated_at           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS replay_runs (
    replay_run_id          TEXT PRIMARY KEY,
    source_system          TEXT NOT NULL,
    start_time             TIMESTAMPTZ NOT NULL,
    end_time               TIMESTAMPTZ NOT NULL,
    status                 TEXT NOT NULL,
    raw_partitions_touched INTEGER DEFAULT 0,
    raw_rows_scanned       INTEGER DEFAULT 0,
    rows_republished       INTEGER DEFAULT 0,
    output_path            TEXT,
    notes                  TEXT,
    created_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at           TIMESTAMPTZ
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
    created_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at           TIMESTAMPTZ
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
    created_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_backtest_artifacts_replay_time
    ON backtest_artifacts(replay_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS backfill_requests (
    backfill_request_id     TEXT PRIMARY KEY,
    source_system           TEXT NOT NULL,
    start_time              TIMESTAMPTZ NOT NULL,
    end_time                TIMESTAMPTZ NOT NULL,
    request_status          TEXT NOT NULL,
    priority                TEXT DEFAULT 'normal',
    requested_by            TEXT,
    reason                  TEXT,
    request_payload         TEXT,
    output_path             TEXT,
    notes                   TEXT,
    created_at              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at            TIMESTAMPTZ
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
    start_time               TIMESTAMPTZ NOT NULL,
    end_time                 TIMESTAMPTZ NOT NULL,
    source_row_count         INTEGER DEFAULT 0,
    feature_row_count        INTEGER DEFAULT 0,
    dataset_hash             TEXT,
    output_path              TEXT,
    status                   TEXT NOT NULL,
    notes                    TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at             TIMESTAMPTZ
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
    deployed_at              TIMESTAMPTZ,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
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
    score_value              DOUBLE PRECISION NOT NULL,
    score_label              TEXT,
    score_metadata           TEXT,
    scored_at                TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shadow_model_scores_candidate_time
    ON shadow_model_scores(candidate_id, scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_shadow_model_scores_model_time
    ON shadow_model_scores(model_version, scored_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_shadow_model_scores_model_candidate_time
    ON shadow_model_scores(model_version, candidate_id, scored_at);

-- -----------------------------------------------------------------
-- 8. PHASE 7 SCALE-UP / STORAGE AUDIT
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS storage_audit_runs (
    storage_audit_run_id     TEXT PRIMARY KEY,
    audit_scope              TEXT NOT NULL,
    status                   TEXT NOT NULL,
    total_partitions         INTEGER DEFAULT 0,
    total_bytes              BIGINT DEFAULT 0,
    missing_file_count       INTEGER DEFAULT 0,
    compact_candidate_count  INTEGER DEFAULT 0,
    cold_candidate_count     INTEGER DEFAULT 0,
    output_path              TEXT,
    summary_json             TEXT,
    notes                    TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_storage_audit_runs_time
    ON storage_audit_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS archive_tiering_decisions (
    tiering_decision_id      TEXT PRIMARY KEY,
    storage_audit_run_id     TEXT NOT NULL,
    partition_path           TEXT NOT NULL,
    source_system            TEXT NOT NULL,
    storage_class            TEXT NOT NULL,
    recommended_tier         TEXT NOT NULL,
    recommended_action       TEXT NOT NULL,
    byte_count               BIGINT DEFAULT 0,
    age_days                 DOUBLE PRECISION,
    file_exists              INTEGER DEFAULT 1,
    metadata_json            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_archive_tiering_decisions_audit_time
    ON archive_tiering_decisions(storage_audit_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_archive_tiering_decisions_partition
    ON archive_tiering_decisions(partition_path, created_at DESC);

CREATE TABLE IF NOT EXISTS compaction_plan_runs (
    compaction_plan_run_id    TEXT PRIMARY KEY,
    storage_audit_run_id      TEXT,
    plan_scope                TEXT NOT NULL,
    status                    TEXT NOT NULL,
    total_items               INTEGER DEFAULT 0,
    compact_item_count        INTEGER DEFAULT 0,
    cold_archive_item_count   INTEGER DEFAULT 0,
    output_path               TEXT,
    summary_json              TEXT,
    created_at                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at              TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_compaction_plan_runs_time
    ON compaction_plan_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS restore_plan_runs (
    restore_plan_run_id      TEXT PRIMARY KEY,
    storage_audit_run_id     TEXT,
    restore_scope            TEXT NOT NULL,
    requested_start_time     TIMESTAMPTZ,
    requested_end_time       TIMESTAMPTZ,
    status                   TEXT NOT NULL,
    total_items              INTEGER DEFAULT 0,
    missing_item_count       INTEGER DEFAULT 0,
    output_path              TEXT,
    summary_json             TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_restore_plan_runs_time
    ON restore_plan_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS integrity_summary_runs (
    integrity_summary_run_id   TEXT PRIMARY KEY,
    storage_audit_run_id       TEXT,
    summary_scope              TEXT NOT NULL,
    status                     TEXT NOT NULL,
    source_count               INTEGER DEFAULT 0,
    total_partitions           INTEGER DEFAULT 0,
    missing_file_count         INTEGER DEFAULT 0,
    compact_candidate_count    INTEGER DEFAULT 0,
    cold_candidate_count       INTEGER DEFAULT 0,
    output_path                TEXT,
    summary_json               TEXT,
    created_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at               TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_integrity_summary_runs_time
    ON integrity_summary_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS archive_action_runs (
    archive_action_run_id      TEXT PRIMARY KEY,
    storage_audit_run_id       TEXT,
    execution_mode             TEXT NOT NULL,
    status                     TEXT NOT NULL,
    total_items                INTEGER DEFAULT 0,
    compact_item_count         INTEGER DEFAULT 0,
    cold_archive_item_count    INTEGER DEFAULT 0,
    investigate_item_count     INTEGER DEFAULT 0,
    output_path                TEXT,
    summary_json               TEXT,
    created_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at               TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_archive_action_runs_time
    ON archive_action_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS archive_action_items (
    archive_action_item_id     TEXT PRIMARY KEY,
    archive_action_run_id      TEXT NOT NULL,
    partition_path             TEXT NOT NULL,
    source_system              TEXT NOT NULL,
    storage_class              TEXT NOT NULL,
    recommended_action         TEXT NOT NULL,
    enforcement_action         TEXT NOT NULL,
    status                     TEXT NOT NULL,
    byte_count                 BIGINT DEFAULT 0,
    age_days                   DOUBLE PRECISION,
    file_exists                INTEGER DEFAULT 1,
    metadata_json              TEXT,
    created_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_archive_action_items_run_time
    ON archive_action_items(archive_action_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS service_profile_runs (
    service_profile_run_id     TEXT PRIMARY KEY,
    profile_scope              TEXT NOT NULL,
    status                     TEXT NOT NULL,
    service_count              INTEGER DEFAULT 0,
    bottleneck_count           INTEGER DEFAULT 0,
    failure_risk_count         INTEGER DEFAULT 0,
    output_path                TEXT,
    summary_json               TEXT,
    created_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at               TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_service_profile_runs_time
    ON service_profile_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS model_evaluation_runs (
    evaluation_run_id        TEXT PRIMARY KEY,
    model_version            TEXT NOT NULL,
    evaluation_version       TEXT NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    dataset_hash             TEXT NOT NULL,
    start_time               TIMESTAMPTZ NOT NULL,
    end_time                 TIMESTAMPTZ NOT NULL,
    train_row_count          INTEGER DEFAULT 0,
    validation_row_count     INTEGER DEFAULT 0,
    test_row_count           INTEGER DEFAULT 0,
    labeled_row_count        INTEGER DEFAULT 0,
    output_path              TEXT,
    summary_json             TEXT NOT NULL,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    positive_rate            DOUBLE PRECISION,
    watch_threshold          DOUBLE PRECISION,
    actionable_threshold     DOUBLE PRECISION,
    critical_threshold       DOUBLE PRECISION,
    metadata_json            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_calibration_profiles_model_time
    ON calibration_profiles(model_version, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calibration_profiles_scope_key
    ON calibration_profiles(profile_scope, profile_key, created_at DESC);

CREATE TABLE IF NOT EXISTS detector_versions (
    detector_version        TEXT PRIMARY KEY,
    feature_schema_version  TEXT NOT NULL,
    state_backend           TEXT NOT NULL,
    notes                   TEXT,
    created_at              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_episodes (
    episode_id               TEXT PRIMARY KEY,
    market_id                TEXT NOT NULL,
    event_id                 TEXT,
    event_family_id          TEXT,
    rule_family              TEXT NOT NULL,
    episode_start_event_time TIMESTAMPTZ NOT NULL,
    episode_end_event_time   TIMESTAMPTZ NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    detector_version         TEXT NOT NULL,
    episode_status           TEXT DEFAULT 'candidate',
    metadata_json            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    trigger_time             TIMESTAMPTZ NOT NULL,
    episode_start_event_time TIMESTAMPTZ NOT NULL,
    episode_end_event_time   TIMESTAMPTZ NOT NULL,
    feature_schema_version   TEXT NOT NULL,
    detector_version         TEXT NOT NULL,
    triggering_rules         TEXT NOT NULL,
    cooldown_state           TEXT,
    feature_snapshot         TEXT NOT NULL,
    severity_score           DOUBLE PRECISION,
    emitted                  INTEGER DEFAULT 1,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signal_candidates_market_time
    ON signal_candidates(market_id, trigger_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_candidates_event_time
    ON signal_candidates(event_id, trigger_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_candidates_detector_time
    ON signal_candidates(detector_version, trigger_time DESC);

CREATE TABLE IF NOT EXISTS signal_features (
    id                       BIGSERIAL PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    episode_id               TEXT NOT NULL,
    market_id                TEXT NOT NULL,
    feature_name             TEXT NOT NULL,
    feature_value            DOUBLE PRECISION,
    feature_schema_version   TEXT NOT NULL,
    observed_at              TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    file_offset             BIGINT DEFAULT 0,
    last_ordering_key       TEXT,
    last_captured_at        TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detector_checkpoints_source
    ON detector_checkpoints(detector_version, source_system, updated_at DESC);

CREATE TABLE IF NOT EXISTS alert_workflow_versions (
    workflow_version         TEXT PRIMARY KEY,
    evidence_schema_version  TEXT NOT NULL,
    alert_schema_version     TEXT NOT NULL,
    delivery_channels        TEXT NOT NULL,
    notes                    TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS evidence_queries (
    evidence_query_id        TEXT PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    alert_id                 TEXT,
    provider_name            TEXT NOT NULL,
    provider_query_type      TEXT NOT NULL,
    provider_query_text      TEXT NOT NULL,
    request_started_at       TIMESTAMPTZ NOT NULL,
    response_completed_at    TIMESTAMPTZ,
    latency_ms               DOUBLE PRECISION,
    result_count             INTEGER DEFAULT 0,
    query_status             TEXT NOT NULL,
    timeout_seconds          INTEGER,
    raw_response_metadata    TEXT,
    error_message            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_evidence_queries_candidate_time
    ON evidence_queries(candidate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_evidence_queries_provider_time
    ON evidence_queries(provider_name, created_at DESC);

CREATE TABLE IF NOT EXISTS evidence_snapshots (
    evidence_snapshot_id     TEXT PRIMARY KEY,
    candidate_id             TEXT NOT NULL,
    alert_id                 TEXT,
    snapshot_time            TIMESTAMPTZ NOT NULL,
    evidence_state           TEXT NOT NULL,
    provider_summary         TEXT NOT NULL,
    confidence_modifier      DOUBLE PRECISION,
    cache_key                TEXT,
    freshness_seconds        INTEGER,
    metadata_json            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    first_delivery_at        TIMESTAMPTZ,
    last_delivery_at         TIMESTAMPTZ,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    attempted_at             TIMESTAMPTZ NOT NULL,
    completed_at             TIMESTAMPTZ,
    error_message            TEXT,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    follow_up_at             TIMESTAMPTZ,
    created_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_analyst_feedback_alert_time
    ON analyst_feedback(alert_id, created_at DESC);

CREATE OR REPLACE VIEW canonical_trades AS
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
) ranked
WHERE dedupe_rank = 1;
