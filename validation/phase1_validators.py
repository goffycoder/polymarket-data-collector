"""Phase 1 validation helpers and executable checks for Person 2.

This module keeps validation logic isolated from collector ingestion code so
Person 2 can verify correctness without introducing coupling to Person 1's
implementation details.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from ast import literal_eval
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from string import hexdigits
from typing import Any

from validation.phase1_report import ValidationSummary
from validation.phase1_semantics import (
    derive_fresh_wallet_flags,
    derive_trade_episode_linkage,
    derive_wallet_first_seen,
)


PHASE1_AGGREGATED_CHECKS = {
    "both_side_coverage": "both_side_asset_coverage",
    "duplicate_validation": "duplicate_trade_inflation",
    "wallet_integrity": "wallet_integrity",
    "condition_integrity": "condition_integrity",
}


@dataclass(slots=True)
class FieldRule:
    """Define one field-level expectation in the wallet-aware trade contract."""

    name: str
    field_type: str
    nullable: bool
    severity: str
    description: str
    allowed_values: tuple[str, ...] = ()


@dataclass(slots=True)
class UniverseRule:
    """Describe how Phase 1 validation is scoped to the approved market universe."""

    source: str
    policy_format: str
    enforcement_mode: str
    zero_leakage_required: bool
    selector_version: str
    tag_ids_field: str = "tag_ids"
    tag_labels_field: str = "tags"
    event_slug_field: str = "slug"
    market_slug_field: str = "slug"


@dataclass(slots=True)
class SemanticsRule:
    """Capture one non-field semantic contract such as fresh-wallet logic."""

    definition: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ThresholdRule:
    """Store named threshold values used by later validator implementations."""

    values: dict[str, Any]


@dataclass(slots=True)
class DedupeRule:
    """Store canonical duplicate-resolution policy from the validation contract."""

    row_identity_field: str
    dedupe_key_field: str
    strongest_key_field: str
    fallback_key_fields: tuple[str, ...]
    source_priority_field: str
    source_priority_rank: dict[str, int]


@dataclass(slots=True)
class Phase1ValidationContract:
    """Typed representation of the Task 1 validation contract."""

    version: int
    phase: str
    owner: str
    source_of_truth: str
    contract_document: str
    universe: UniverseRule
    trade_table_name: str
    required_fields: dict[str, FieldRule]
    thresholds: ThresholdRule
    dedupe: DedupeRule
    semantics: dict[str, SemanticsRule]
    report_formats: tuple[str, ...]
    sample_failure_limit: int
    include_reason_codes: bool


@dataclass(slots=True)
class ValidationRuntime:
    """Carry runtime inputs shared across all validation checks."""

    contract: Phase1ValidationContract
    db_path: Path
    config_path: Path
    approved_market_ids: set[str] | None = None
    approved_market_scope_attempted: bool = False


def _read_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file into a dictionary.

    A dedicated helper keeps file I/O separate from contract parsing so later
    tasks can reuse it for report generation or test fixtures.
    """

    with path.open("r", encoding="utf-8") as handle:
        raw_text = handle.read()

    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw_text) or {}
    except ModuleNotFoundError:
        try:
            # The repository declares PyYAML as a dependency, but this fallback
            # keeps validation usable in lean environments as long as the file is
            # JSON-compatible YAML.
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            data = _parse_simple_yaml(raw_text)

    if not isinstance(data, dict):
        raise ValueError(f"Validation contract must deserialize to a mapping: {path}")

    return data


def _parse_simple_yaml(raw_text: str) -> dict[str, Any]:
    """Parse a small YAML subset used by this repository's config files.

    This fallback supports indentation-based mappings and inline list syntax
    such as ``tag_ids: [2, 15]``. It is intentionally narrow and only exists so
    the validation suite can run when PyYAML is unavailable.
    """

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for raw_line in raw_text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            raise ValueError(f"Unsupported YAML line in fallback parser: {raw_line}")

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        value = raw_value.strip()

        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        if not value:
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
            continue

        parent[key] = _parse_simple_yaml_scalar(value)

    return root


def _parse_simple_yaml_scalar(raw_value: str) -> Any:
    """Parse a scalar value for the simple YAML fallback parser."""

    lowered = raw_value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None

    if raw_value.startswith("[") and raw_value.endswith("]"):
        try:
            return list(literal_eval(raw_value))
        except (SyntaxError, ValueError):
            return raw_value

    if raw_value.startswith(("'", '"')) and raw_value.endswith(("'", '"')):
        return raw_value[1:-1]

    try:
        if "." in raw_value:
            return float(raw_value)
        return int(raw_value)
    except ValueError:
        return raw_value


def _parse_field_rules(raw_fields: dict[str, Any]) -> dict[str, FieldRule]:
    """Convert raw field config into typed field rules."""

    parsed: dict[str, FieldRule] = {}

    for field_name, raw_rule in raw_fields.items():
        if not isinstance(raw_rule, dict):
            raise ValueError(f"Field rule must be a mapping: {field_name}")

        parsed[field_name] = FieldRule(
            name=field_name,
            field_type=str(raw_rule["type"]),
            nullable=bool(raw_rule["nullable"]),
            severity=str(raw_rule["severity"]),
            description=str(raw_rule.get("description", "")),
            allowed_values=tuple(str(value) for value in raw_rule.get("allowed_values", [])),
        )

    return parsed


def _parse_semantics(raw_semantics: dict[str, Any]) -> dict[str, SemanticsRule]:
    """Convert semantic config into typed semantic rules."""

    parsed: dict[str, SemanticsRule] = {}

    for rule_name, raw_rule in raw_semantics.items():
        if not isinstance(raw_rule, dict):
            raise ValueError(f"Semantic rule must be a mapping: {rule_name}")

        definition = str(raw_rule.get("definition", ""))
        details = {key: value for key, value in raw_rule.items() if key != "definition"}
        parsed[rule_name] = SemanticsRule(definition=definition, details=details)

    return parsed


def _parse_dedupe_rule(raw_dedupe: dict[str, Any]) -> DedupeRule:
    """Convert dedupe config into a typed canonical-resolution contract."""

    if not raw_dedupe:
        raise ValueError("Validation contract must define dedupe policy settings")

    return DedupeRule(
        row_identity_field=str(raw_dedupe["row_identity_field"]),
        dedupe_key_field=str(raw_dedupe["dedupe_key_field"]),
        strongest_key_field=str(raw_dedupe["strongest_key_field"]),
        fallback_key_fields=tuple(str(value) for value in raw_dedupe.get("fallback_key_fields", [])),
        source_priority_field=str(raw_dedupe["source_priority_field"]),
        source_priority_rank={
            str(source): int(rank)
            for source, rank in dict(raw_dedupe.get("source_priority_rank", {})).items()
        },
    )


def load_phase1_validation_contract(
    config_path: str | Path = "config/phase1_validation.yaml",
) -> Phase1ValidationContract:
    """Load the Task 1 validation contract from YAML.

    The returned object is intentionally narrow and typed so later validation
    code can depend on a stable interface instead of reading YAML directly.
    """

    path = Path(config_path)
    raw = _read_yaml(path)

    metadata = raw.get("metadata", {})
    universe = raw.get("universe", {})
    trade_entity = raw.get("trade_entity", {})
    dedupe = raw.get("dedupe", {})
    outputs = raw.get("outputs", {})

    contract = Phase1ValidationContract(
        version=int(raw["version"]),
        phase=str(raw["phase"]),
        owner=str(metadata["owner"]),
        source_of_truth=str(metadata["source_of_truth"]),
        contract_document=str(metadata["contract_document"]),
        universe=UniverseRule(
            source=str(universe["source"]),
            policy_format=str(universe.get("policy_format", "legacy")),
            enforcement_mode=str(universe["enforcement_mode"]),
            zero_leakage_required=bool(universe["zero_leakage_required"]),
            selector_version=str(universe["selector_version"]),
            tag_ids_field=str(universe.get("tag_ids_field", "tag_ids")),
            tag_labels_field=str(universe.get("tag_labels_field", "tags")),
            event_slug_field=str(universe.get("event_slug_field", "slug")),
            market_slug_field=str(universe.get("market_slug_field", "slug")),
        ),
        trade_table_name=str(trade_entity["table_name"]),
        required_fields=_parse_field_rules(trade_entity.get("required_fields", {})),
        thresholds=ThresholdRule(values=dict(raw.get("thresholds", {}))),
        dedupe=_parse_dedupe_rule(dict(dedupe)),
        semantics=_parse_semantics(dict(raw.get("semantics", {}))),
        report_formats=tuple(str(value) for value in outputs.get("report_formats", [])),
        sample_failure_limit=int(outputs.get("sample_failure_limit", 0)),
        include_reason_codes=bool(outputs.get("include_reason_codes", False)),
    )

    validate_contract_structure(contract)
    return contract


def validate_contract_structure(contract: Phase1ValidationContract) -> None:
    """Check the internal consistency of the Task 1 contract structure.

    This is intentionally limited to contract-shape checks. It does not validate
    the database schema or execute any Phase 1 data-quality queries yet.
    """

    if contract.version < 1:
        raise ValueError("Validation contract version must be >= 1")

    if not contract.required_fields:
        raise ValueError("Validation contract must define at least one required field")

    if contract.universe.enforcement_mode not in {"approved_only"}:
        raise ValueError(
            "Unsupported enforcement mode for Task 1 contract: "
            f"{contract.universe.enforcement_mode}"
        )

    if contract.sample_failure_limit < 0:
        raise ValueError("Sample failure limit cannot be negative")

    if not contract.dedupe.dedupe_key_field:
        raise ValueError("Contract must define a canonical dedupe_key field")

    if not contract.dedupe.source_priority_field:
        raise ValueError("Contract must define a source_priority field")

    for field_rule in contract.required_fields.values():
        if field_rule.severity not in {"error", "warn"}:
            raise ValueError(
                f"Unsupported severity '{field_rule.severity}' for field '{field_rule.name}'"
            )

    if "first_seen_at" not in contract.semantics:
        raise ValueError("Contract must define first_seen_at semantics")

    if "fresh_wallet" not in contract.semantics:
        raise ValueError("Contract must define fresh_wallet semantics")

    if "episode_linkage" not in contract.semantics:
        raise ValueError("Contract must define episode_linkage semantics")


def run_phase1_validation(
    db_path: str | Path = "database/polymarket_state.db",
    config_path: str | Path = "config/phase1_validation.yaml",
) -> ValidationSummary:
    """Execute the current Phase 1 validation suite.

    The suite is designed to degrade gracefully while Person 1's ingestion work
    is still in progress. Missing databases, tables, or columns are reported as
    explicit findings instead of causing unhandled exceptions.
    """

    contract = load_phase1_validation_contract(config_path=config_path)
    runtime = ValidationRuntime(
        contract=contract,
        db_path=Path(db_path),
        config_path=Path(config_path),
    )
    summary = ValidationSummary(
        run_label=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    )
    summary.add(
        "validation_contract_context",
        "pass",
        "info",
        "Loaded the Phase 1 validation contract and run context.",
        metrics={
            "contract_version": contract.version,
            "phase": contract.phase,
            "owner": contract.owner,
            "config_path": str(runtime.config_path),
            "trade_table_name": contract.trade_table_name,
            "selector_version": contract.universe.selector_version,
            "universe_policy_source": contract.universe.source,
        },
    )

    if not runtime.db_path.exists():
        summary.add(
            "database_prerequisite",
            "fail",
            "error",
            f"Database file not found: {runtime.db_path}",
            reason_code="missing_database",
        )
        summary.aggregate_report = _build_phase1_aggregate_report(summary)
        return summary

    conn = sqlite3.connect(runtime.db_path)
    conn.row_factory = sqlite3.Row

    try:
        _validate_schema_prerequisites(conn, runtime, summary)
        _validate_both_side_asset_coverage(conn, runtime, summary)
        _validate_universe_leakage(conn, runtime, summary)
        _validate_duplicate_trade_inflation(conn, runtime, summary)
        _validate_wallet_integrity(conn, runtime, summary)
        _validate_wallet_field_null_rate(conn, runtime, summary)
        _validate_transaction_hash_population(conn, runtime, summary)
        _validate_condition_id_population(conn, runtime, summary)
        _validate_asset_outcome_correctness(conn, runtime, summary)
        _validate_condition_integrity(conn, runtime, summary)
        _validate_first_seen_semantics(conn, runtime, summary)
        _validate_fresh_wallet_semantics(conn, runtime, summary)
        _validate_episode_linkage_semantics(conn, runtime, summary)
    finally:
        conn.close()

    _validate_candidate_review_output(runtime, summary)
    summary.aggregate_report = _build_phase1_aggregate_report(summary)
    return summary


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Return whether a SQLite table exists."""

    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return the set of columns present on a SQLite table."""

    if not _table_exists(conn, table_name):
        return set()

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _load_universe_policy(path: Path) -> dict[str, Any]:
    """Load the approved-universe policy configuration."""

    return _read_yaml(path)


def _parse_tag_values(raw_tags: Any) -> set[str]:
    """Normalize tag labels or tag-id arrays into a comparable string set."""

    return _parse_event_tags(raw_tags)


def _parse_universe_rules(policy_config: dict[str, Any]) -> dict[str, Any]:
    """Return a normalized policy view from either legacy or new config shapes."""

    if "universe_policy" in policy_config and isinstance(policy_config["universe_policy"], dict):
        policy = policy_config["universe_policy"]
        return {
            "include_tag_ids": {str(value) for value in policy.get("include_tag_ids", [])},
            "exclude_tag_ids": {str(value) for value in policy.get("exclude_tag_ids", [])},
            "include_keywords": [str(value).lower() for value in policy.get("include_keywords", [])],
            "exclude_keywords_or_slugs": [
                str(value).lower() for value in policy.get("exclude_keywords_or_slugs", [])
            ],
            "manual_event_slugs": {str(value).lower() for value in policy.get("manual_event_slugs", [])},
            "manual_market_slugs": {str(value).lower() for value in policy.get("manual_market_slugs", [])},
            "min_liquidity": float(policy.get("minimum_liquidity", 0)),
        }

    watchlists = policy_config.get("watchlists", {})
    include_tag_ids: set[str] = set()
    min_liquidity = 0.0
    if isinstance(watchlists, dict):
        for rule in watchlists.values():
            include_tag_ids.update(str(value) for value in rule.get("tag_ids", []))
            min_liquidity = max(min_liquidity, float(rule.get("min_liquidity", 0)))

    return {
        "include_tag_ids": include_tag_ids,
        "exclude_tag_ids": set(),
        "include_keywords": [],
        "exclude_keywords_or_slugs": [],
        "manual_event_slugs": set(),
        "manual_market_slugs": set(),
        "min_liquidity": min_liquidity,
    }


def _parse_event_tags(raw_tags: Any) -> set[str]:
    """Normalize event tag representations into a comparable string set."""

    if raw_tags is None:
        return set()

    if isinstance(raw_tags, (list, tuple, set)):
        return {str(value).strip() for value in raw_tags if str(value).strip()}

    if isinstance(raw_tags, str):
        text = raw_tags.strip()
        if not text:
            return set()

        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {part.strip() for part in text.split(",") if part.strip()}

        if isinstance(parsed, list):
            return {str(value).strip() for value in parsed if str(value).strip()}
        if isinstance(parsed, dict):
            return {str(key).strip() for key in parsed.keys() if str(key).strip()}

    return {str(raw_tags).strip()}


def _resolve_approved_market_ids(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> set[str]:
    """Resolve approved market ids using the universe-policy configuration.

    The implementation prefers stable tag IDs plus explicit slug overrides when
    they are available, and falls back to tag labels for legacy compatibility.
    If the mapping cannot be evaluated from the local schema, the function
    records a finding and returns an empty set so downstream checks can skip
    safely.
    """

    markets_columns = _table_columns(conn, "markets")
    events_columns = _table_columns(conn, "events")

    required_market_columns = {"market_id", "event_id", "liquidity"}
    required_event_columns = {"event_id"}

    if not required_market_columns.issubset(markets_columns) or not required_event_columns.issubset(events_columns):
        summary.add(
            "approved_universe_scope",
            "fail",
            "error",
            "Cannot resolve approved market universe because required events/markets columns are missing.",
            reason_code="universe_schema_missing",
            metrics={
                "markets_columns_present": sorted(markets_columns),
                "events_columns_present": sorted(events_columns),
            },
        )
        return set()

    if not Path(runtime.contract.universe.source).exists():
        summary.add(
            "approved_universe_scope",
            "fail",
            "error",
            f"Universe-policy file not found: {runtime.contract.universe.source}",
            reason_code="universe_policy_file_missing",
        )
        return set()

    try:
        policy_config = _load_universe_policy(Path(runtime.contract.universe.source))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        summary.add(
            "approved_universe_scope",
            "fail",
            "error",
            f"Could not load universe-policy configuration: {exc}",
            reason_code="universe_policy_load_failed",
        )
        return set()

    policy_rules = _parse_universe_rules(policy_config)
    if not policy_rules["include_tag_ids"] and not policy_rules["manual_event_slugs"] and not policy_rules["manual_market_slugs"]:
        summary.add(
            "approved_universe_scope",
            "fail",
            "error",
            "Universe-policy configuration is empty or malformed.",
            reason_code="universe_policy_missing",
        )
        return set()

    approved_ids: set[str] = set()
    event_tag_field = runtime.contract.universe.tag_ids_field or "tag_ids"
    if event_tag_field not in events_columns:
        event_tag_field = runtime.contract.universe.tag_labels_field or "tags"
    event_slug_field = runtime.contract.universe.event_slug_field or "slug"
    market_slug_field = runtime.contract.universe.market_slug_field or "slug"

    selected_event_slug = f"e.{event_slug_field}" if event_slug_field in events_columns else "NULL"
    selected_market_slug = f"m.{market_slug_field}" if market_slug_field in markets_columns else "NULL"
    selected_event_tags = f"e.{event_tag_field}" if event_tag_field in events_columns else "NULL"

    rows = conn.execute(
        f"""
        SELECT
            m.market_id,
            m.liquidity,
            {selected_market_slug} AS market_slug,
            {selected_event_slug} AS event_slug,
            {selected_event_tags} AS event_tags
        FROM markets m
        JOIN events e ON e.event_id = m.event_id
        """
    ).fetchall()

    for row in rows:
        event_tags = _parse_tag_values(row["event_tags"])
        market_liquidity = float(row["liquidity"] or 0)
        event_slug = str(row["event_slug"] or "").lower()
        market_slug = str(row["market_slug"] or "").lower()
        exclusion_tokens = policy_rules["exclude_keywords_or_slugs"]

        if event_slug in policy_rules["manual_event_slugs"] or market_slug in policy_rules["manual_market_slugs"]:
            approved_ids.add(str(row["market_id"]))
            continue

        if event_tags.intersection(policy_rules["exclude_tag_ids"]):
            continue
        if any(token and (token in event_slug or token in market_slug) for token in exclusion_tokens):
            continue

        if event_tags.intersection(policy_rules["include_tag_ids"]) and market_liquidity >= policy_rules["min_liquidity"]:
            approved_ids.add(str(row["market_id"]))
            continue
        if any(token and (token in event_slug or token in market_slug) for token in policy_rules["include_keywords"]) and market_liquidity >= policy_rules["min_liquidity"]:
            approved_ids.add(str(row["market_id"]))

    if not approved_ids:
        summary.add(
            "approved_universe_scope",
            "fail",
            "error",
            "Approved universe resolved to zero markets with the current universe-policy rules.",
            reason_code="approved_universe_empty",
            metrics={
                "selector_version": runtime.contract.universe.selector_version,
                "universe_policy_source": runtime.contract.universe.source,
            },
        )
    else:
        summary.add(
            "approved_universe_scope",
            "pass",
            "info",
            "Approved market universe resolved successfully.",
            metrics={
                "approved_market_count": len(approved_ids),
                "selector_version": runtime.contract.universe.selector_version,
                "universe_policy_source": runtime.contract.universe.source,
            },
        )

    return approved_ids


def _get_approved_market_ids(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> set[str]:
    """Return the cached approved market universe for the current validation run."""

    if runtime.approved_market_scope_attempted:
        return runtime.approved_market_ids or set()

    runtime.approved_market_ids = _resolve_approved_market_ids(conn, runtime, summary)
    runtime.approved_market_scope_attempted = True
    return runtime.approved_market_ids or set()


def _build_market_scope_filter(
    approved_market_ids: set[str],
    *,
    market_column: str = "market_id",
) -> tuple[str, tuple[str, ...]]:
    """Build a reusable SQL filter clause for the approved market universe."""

    placeholders = ",".join("?" for _ in approved_market_ids)
    return f"{market_column} IN ({placeholders})", tuple(sorted(approved_market_ids))


def _limit_samples(values: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Trim sample payloads so reports stay reproducible and bounded."""

    return values[: max(limit, 0)]


def _metric_ratio_breakdown(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] = (),
) -> dict[str, dict[str, float | int]]:
    """Run a grouped aggregation query and normalize ratio metrics for reporting."""

    rows = conn.execute(query, params).fetchall()
    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        group_key = str(row["group_key"] or "unknown")
        total_rows = int(row["total_rows"] or 0)
        matched_rows = int(row["matched_rows"] or 0)
        breakdown[group_key] = {
            "total_rows": total_rows,
            "matched_rows": matched_rows,
            "ratio": _safe_ratio(matched_rows, total_rows),
        }
    return breakdown


def _latest_reason_code(summary: ValidationSummary, check_name: str) -> str | None:
    """Return the most recent reason code for a named finding, if present."""

    for finding in reversed(summary.findings):
        if finding.check_name == check_name:
            return finding.reason_code
    return None


def _extract_latest_check_findings(
    summary: ValidationSummary,
    check_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Return the latest finding payload for each aggregated Phase 1 check."""

    checks: dict[str, dict[str, Any]] = {}
    for aggregate_name, finding_name in check_map.items():
        latest_finding = next(
            (finding for finding in reversed(summary.findings) if finding.check_name == finding_name),
            None,
        )
        if latest_finding is None:
            checks[aggregate_name] = {
                "status": "fail",
                "reason_code": "missing_check_finding",
                "metrics": {},
            }
            continue

        checks[aggregate_name] = {
            "status": latest_finding.status,
            "reason_code": latest_finding.reason_code,
            "metrics": latest_finding.metrics,
        }

    return checks


def _compute_phase1_overall_status(checks: dict[str, dict[str, Any]]) -> str:
    """Compute the overall Phase 1 aggregate status from tracked check statuses."""

    statuses = [str(payload.get("status", "fail")) for payload in checks.values()]
    if any(status == "fail" for status in statuses):
        return "fail"
    if any(status == "warn" for status in statuses):
        return "warn"
    return "pass"


def _compute_phase1_summary_counts(checks: dict[str, dict[str, Any]]) -> dict[str, int]:
    """Return aggregate pass/warn/fail counts across tracked Phase 1 checks."""

    statuses = [str(payload.get("status", "fail")) for payload in checks.values()]
    return {
        "total_checks": len(statuses),
        "pass_count": sum(1 for status in statuses if status == "pass"),
        "warn_count": sum(1 for status in statuses if status == "warn"),
        "fail_count": sum(1 for status in statuses if status == "fail"),
    }


def _build_phase1_aggregate_report(summary: ValidationSummary) -> dict[str, Any]:
    """Build the unified Phase 1 aggregate report from the current findings."""

    checks = _extract_latest_check_findings(summary, PHASE1_AGGREGATED_CHECKS)
    return {
        "run_label": summary.run_label,
        "overall_status": _compute_phase1_overall_status(checks),
        "checks": checks,
        "summary": _compute_phase1_summary_counts(checks),
    }


def _validate_universe_leakage(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate that trade collection is restricted to the approved universe."""

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "market_id" not in trade_columns:
        summary.add(
            "universe_leakage",
            "fail",
            "error",
            "Cannot validate universe leakage because trade market_id is missing.",
            reason_code="universe_leakage_schema_missing",
        )
        return

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "universe_leakage",
            "fail",
            "error",
            "Universe leakage check could not run because approved-universe scope could not be resolved.",
            reason_code="universe_leakage_scope_missing",
        )
        return

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    leaked_market_count = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT market_id
            FROM {runtime.contract.trade_table_name}
            WHERE market_id IS NOT NULL
              AND NOT ({where_clause})
        )
        """,
        params,
    )
    leaked_rows = conn.execute(
        f"""
        SELECT market_id
        FROM {runtime.contract.trade_table_name}
        WHERE market_id IS NOT NULL
          AND NOT ({where_clause})
        GROUP BY market_id
        ORDER BY market_id ASC
        LIMIT ?
        """,
        params + (runtime.contract.sample_failure_limit,),
    ).fetchall()
    scoped_trade_count = _query_scalar(
        conn,
        f"SELECT COUNT(*) FROM {runtime.contract.trade_table_name} WHERE {where_clause}",
        params,
    )
    status = "pass" if leaked_market_count == 0 else "fail"
    summary.add(
        "universe_leakage",
        status,
        "info" if status == "pass" else "error",
        "Computed trade-market leakage outside the approved universe.",
        reason_code=None if status == "pass" else "universe_leakage_detected",
        metrics={
            "approved_market_count": len(approved_market_ids),
            "approved_trade_count": scoped_trade_count,
            "leaked_market_count": leaked_market_count,
            "leaked_market_samples": [
                {
                    "market_id": str(row["market_id"]),
                    "reason_code": "market_outside_approved_universe",
                }
                for row in leaked_rows
            ],
        },
    )


def _validate_schema_prerequisites(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate the presence of required tables and Phase 1 trade columns."""

    required_tables = {"events", "markets", runtime.contract.trade_table_name}
    missing_tables = sorted(
        table_name for table_name in required_tables if not _table_exists(conn, table_name)
    )

    if missing_tables:
        summary.add(
            "schema_prerequisites",
            "fail",
            "error",
            "Required tables for Phase 1 validation are missing.",
            reason_code="missing_tables",
            metrics={"missing_tables": missing_tables},
        )
        return

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    missing_fields = sorted(
        field_name
        for field_name in runtime.contract.required_fields
        if field_name not in trade_columns
    )

    if missing_fields:
        summary.add(
            "trade_schema_contract",
            "fail",
            "error",
            "Trade table does not yet satisfy the Phase 1 contract field set.",
            reason_code="missing_trade_fields",
            metrics={"missing_fields": missing_fields},
        )
    else:
        summary.add(
            "trade_schema_contract",
            "pass",
            "info",
            "Trade table contains all Phase 1 contract fields.",
        )


def _validate_both_side_asset_coverage(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate both-side asset coverage inside the approved market universe."""

    prerequisites = _validate_both_side_coverage_prerequisites(conn, runtime, summary)
    if prerequisites is None:
        return

    approved_market_ids = prerequisites["approved_market_ids"]
    metadata_metrics = _load_both_side_metadata_metrics(conn, approved_market_ids)
    trade_metrics = _load_both_side_trade_metrics(
        conn,
        runtime.contract.trade_table_name,
        approved_market_ids,
    )
    threshold = float(runtime.contract.thresholds.values["both_side_coverage_min_ratio"])
    source_breakdown = _load_both_side_coverage_by_source(conn, runtime, approved_market_ids)
    failing_markets = _load_both_side_failing_markets(conn, runtime, approved_market_ids)
    result = _build_both_side_coverage_result(
        metadata_metrics,
        trade_metrics,
        source_breakdown,
        failing_markets,
        threshold,
        runtime.contract.sample_failure_limit,
        contract_version=runtime.contract.version,
        selector_version=runtime.contract.universe.selector_version,
        universe_policy_source=runtime.contract.universe.source,
    )
    status = str(result["status"])
    severity = "warn" if status == "warn" else ("info" if status == "pass" else "error")
    summary.add(
        "both_side_asset_coverage",
        status,
        severity,
        "Computed metadata-side and trade-side YES/NO coverage ratios.",
        reason_code=str(result["reason_code"]),
        metrics=dict(result["metrics"], failing_markets_sample=result["failing_markets_sample"]),
    )


def _validate_both_side_coverage_prerequisites(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> dict[str, set[str]] | None:
    """Validate schema and scope prerequisites for the both-side coverage check.

    This helper is intentionally narrow for Task 5 Step 1. It confirms the
    minimum tables and columns needed to run approved-universe both-side
    coverage, while keeping `condition_id` compatibility available through the
    returned column sets when that field is present.
    """

    required_tables = {"events", "markets", runtime.contract.trade_table_name}
    missing_tables = sorted(
        table_name for table_name in required_tables if not _table_exists(conn, table_name)
    )
    if missing_tables:
        summary.add(
            "both_side_asset_coverage",
            "fail",
            "error",
            "Cannot evaluate both-side coverage because required tables are missing.",
            reason_code="coverage_schema_missing",
            metrics={"missing_tables": missing_tables},
        )
        return None

    markets_columns = _table_columns(conn, "markets")
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_market_columns = {"market_id", "event_id", "yes_token_id", "no_token_id"}
    required_trade_columns = {"market_id", "outcome_side", "source"}

    if not required_market_columns.issubset(markets_columns) or not required_trade_columns.issubset(trade_columns):
        summary.add(
            "both_side_asset_coverage",
            "fail",
            "error",
            "Cannot evaluate both-side coverage because required columns are missing.",
            reason_code="coverage_schema_missing",
            metrics={
                "missing_market_columns": sorted(required_market_columns - markets_columns),
                "missing_trade_columns": sorted(required_trade_columns - trade_columns),
                "condition_id_supported": "condition_id" in markets_columns or "condition_id" in trade_columns,
            },
        )
        return None

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "both_side_asset_coverage",
            "fail",
            "error",
            "Both-side coverage could not run because approved market scope could not be resolved.",
            reason_code="coverage_scope_missing",
            metrics={"condition_id_supported": "condition_id" in markets_columns or "condition_id" in trade_columns},
        )
        return None

    return {
        "markets_columns": markets_columns,
        "trade_columns": trade_columns,
        "approved_market_ids": approved_market_ids,
    }


def _load_both_side_metadata_metrics(
    conn: sqlite3.Connection,
    approved_market_ids: set[str],
) -> dict[str, int]:
    """Return approved-universe metadata coverage counts for YES/NO tokens.

    The result is deterministic because it is derived only from the approved
    market scope and aggregate counts over the `markets` table.
    """

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_markets,
            COUNT(
                CASE
                    WHEN yes_token_id IS NOT NULL AND TRIM(CAST(yes_token_id AS TEXT)) != ''
                    THEN 1
                END
            ) AS markets_with_yes,
            COUNT(
                CASE
                    WHEN no_token_id IS NOT NULL AND TRIM(CAST(no_token_id AS TEXT)) != ''
                    THEN 1
                END
            ) AS markets_with_no,
            COUNT(
                CASE
                    WHEN yes_token_id IS NOT NULL AND TRIM(CAST(yes_token_id AS TEXT)) != ''
                     AND no_token_id IS NOT NULL AND TRIM(CAST(no_token_id AS TEXT)) != ''
                    THEN 1
                END
            ) AS markets_with_both_tokens
        FROM markets
        WHERE {where_clause}
        """,
        params,
    ).fetchone()

    return {
        "total_markets": int(row["total_markets"] or 0),
        "markets_with_yes": int(row["markets_with_yes"] or 0),
        "markets_with_no": int(row["markets_with_no"] or 0),
        "markets_with_both_tokens": int(row["markets_with_both_tokens"] or 0),
    }


def _load_both_side_trade_metrics(
    conn: sqlite3.Connection,
    trade_table_name: str,
    approved_market_ids: set[str],
) -> dict[str, int]:
    """Return approved-universe trade-side coverage counts for YES/NO activity.

    The grouping prefers `condition_id` when that field exists on the trade
    table so condition-based ingestion remains compatible, while final counts
    are still reported at the approved-market level. Blank and NULL
    `outcome_side` values are ignored, and side comparisons are normalized to
    uppercase for deterministic matching.
    """

    trade_columns = _table_columns(conn, trade_table_name)
    group_fields = ["market_id"]
    if "condition_id" in trade_columns:
        group_fields.append("condition_id")

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    group_by_expr = ", ".join(group_fields)
    row = conn.execute(
        f"""
        SELECT
            COUNT(DISTINCT market_id) AS markets_with_trades,
            COUNT(DISTINCT CASE WHEN has_yes = 1 THEN market_id END) AS markets_with_yes_trades,
            COUNT(DISTINCT CASE WHEN has_no = 1 THEN market_id END) AS markets_with_no_trades,
            COUNT(
                DISTINCT CASE
                    WHEN has_yes = 1 AND has_no = 1 THEN market_id
                END
            ) AS markets_with_both_trade_sides
        FROM (
            SELECT
                market_id,
                MAX(CASE WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'YES' THEN 1 ELSE 0 END) AS has_yes,
                MAX(CASE WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'NO' THEN 1 ELSE 0 END) AS has_no
            FROM {trade_table_name}
            WHERE {where_clause}
              AND outcome_side IS NOT NULL
              AND TRIM(CAST(outcome_side AS TEXT)) != ''
            GROUP BY {group_by_expr}
        )
        """,
        params,
    ).fetchone()

    return {
        "markets_with_trades": int(row["markets_with_trades"] or 0),
        "markets_with_yes_trades": int(row["markets_with_yes_trades"] or 0),
        "markets_with_no_trades": int(row["markets_with_no_trades"] or 0),
        "markets_with_both_trade_sides": int(row["markets_with_both_trade_sides"] or 0),
    }


def _count_markets_with_both_trade_sides(
    conn: sqlite3.Connection,
    trade_table_name: str,
    approved_market_ids: set[str],
) -> int:
    """Count approved markets that have both YES and NO trade observations."""

    placeholders = ",".join("?" for _ in approved_market_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS market_count
        FROM (
            SELECT market_id
            FROM {trade_table_name}
            WHERE market_id IN ({placeholders})
            GROUP BY market_id
            HAVING COUNT(DISTINCT outcome_side) >= 2
        )
        """,
        tuple(sorted(approved_market_ids)),
    ).fetchone()
    return int(row["market_count"] or 0)


def _load_both_side_coverage_by_source(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
) -> dict[str, dict[str, float | int]]:
    """Return source-level both-side coverage metrics inside the approved universe.

    Side detection is normalized to case-insensitive YES/NO values, blank and
    NULL outcome-side values are ignored, and the grouping prefers
    `condition_id` when that field exists so condition-based ingestion stays
    compatible with market-level coverage reporting.
    """

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "source" not in trade_columns:
        return {}

    group_fields = ["source", "market_id"]
    if "condition_id" in trade_columns:
        group_fields.append("condition_id")

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    group_by_expr = ", ".join(group_fields)
    rows = conn.execute(
        f"""
        SELECT
            source,
            COUNT(DISTINCT market_id) AS market_count,
            COUNT(
                DISTINCT CASE
                    WHEN has_yes = 1 AND has_no = 1 THEN market_id
                END
            ) AS both_side_market_count
        FROM (
            SELECT
                source,
                market_id,
                MAX(
                    CASE
                        WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'YES' THEN 1
                        ELSE 0
                    END
                ) AS has_yes,
                MAX(
                    CASE
                        WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'NO' THEN 1
                        ELSE 0
                    END
                ) AS has_no
            FROM {runtime.contract.trade_table_name}
            WHERE {where_clause}
              AND outcome_side IS NOT NULL
              AND TRIM(CAST(outcome_side AS TEXT)) != ''
            GROUP BY {group_by_expr}
        )
        GROUP BY source
        ORDER BY source ASC
        """,
        params,
    ).fetchall()

    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        source = str(row["source"] or "unknown")
        market_count = int(row["market_count"] or 0)
        both_side_market_count = int(row["both_side_market_count"] or 0)
        breakdown[source] = {
            "markets_with_trades": market_count,
            "markets_with_both_sides": both_side_market_count,
            "coverage_ratio": _safe_ratio(both_side_market_count, market_count),
        }
    return breakdown


def _load_both_side_failing_markets(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
) -> list[dict[str, Any]]:
    """Return deterministic failing-market rows for both-side coverage review.

    The output is scoped to approved markets only, assigns one or more
    deterministic reason codes per market, and is sorted by tier, event_id, and
    market_id so repeated runs produce the same sample order.
    """

    market_columns = _table_columns(conn, "markets")
    event_columns = _table_columns(conn, "events")
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    group_fields = ["market_id"]
    if "condition_id" in trade_columns:
        group_fields.append("condition_id")

    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="m.market_id")
    event_slug_select = "e.slug" if "slug" in event_columns else "NULL"
    market_slug_select = "m.slug" if "slug" in market_columns else "NULL"
    tier_select = "m.tier" if "tier" in market_columns else "NULL"
    group_by_expr = ", ".join(group_fields)

    rows = conn.execute(
        f"""
        SELECT
            m.market_id,
            m.event_id,
            {event_slug_select} AS event_slug,
            {market_slug_select} AS market_slug,
            {tier_select} AS tier,
            m.yes_token_id,
            m.no_token_id,
            COALESCE(ts.has_yes_trade, 0) AS has_yes_trade,
            COALESCE(ts.has_no_trade, 0) AS has_no_trade
        FROM markets m
        JOIN events e
          ON e.event_id = m.event_id
        LEFT JOIN (
            SELECT
                market_id,
                MAX(has_yes) AS has_yes_trade,
                MAX(has_no) AS has_no_trade
            FROM (
                SELECT
                    market_id,
                    MAX(
                        CASE
                            WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'YES' THEN 1
                            ELSE 0
                        END
                    ) AS has_yes,
                    MAX(
                        CASE
                            WHEN UPPER(TRIM(CAST(outcome_side AS TEXT))) = 'NO' THEN 1
                            ELSE 0
                        END
                    ) AS has_no
                FROM {runtime.contract.trade_table_name}
                WHERE market_id IN ({",".join("?" for _ in approved_market_ids)})
                  AND outcome_side IS NOT NULL
                  AND TRIM(CAST(outcome_side AS TEXT)) != ''
                GROUP BY {group_by_expr}
            )
            GROUP BY market_id
        ) ts
          ON ts.market_id = m.market_id
        WHERE {where_clause}
        ORDER BY
            COALESCE({tier_select}, '') ASC,
            m.event_id ASC,
            m.market_id ASC
        """,
        tuple(sorted(approved_market_ids)) + params,
    ).fetchall()

    failing_markets: list[dict[str, Any]] = []
    for row in rows:
        reason_codes: list[str] = []
        if not str(row["yes_token_id"] or "").strip():
            reason_codes.append("missing_yes_token")
        if not str(row["no_token_id"] or "").strip():
            reason_codes.append("missing_no_token")
        if int(row["has_yes_trade"] or 0) == 0:
            reason_codes.append("missing_yes_trade")
        if int(row["has_no_trade"] or 0) == 0:
            reason_codes.append("missing_no_trade")

        if reason_codes:
            failing_markets.append(
                {
                    "market_id": str(row["market_id"]),
                    "event_id": str(row["event_id"]),
                    "event_slug": str(row["event_slug"] or ""),
                    "market_slug": str(row["market_slug"] or ""),
                    "tier": str(row["tier"] or ""),
                    "reason_codes": reason_codes,
                }
            )

    return _limit_samples(failing_markets, runtime.contract.sample_failure_limit)


def _build_both_side_coverage_result(
    metadata_metrics: dict[str, int],
    trade_metrics: dict[str, int],
    source_breakdown: dict[str, dict[str, float | int]],
    failing_markets: list[dict[str, Any]],
    threshold: float,
    sample_limit: int,
    *,
    contract_version: int,
    selector_version: str,
    universe_policy_source: str,
) -> dict[str, Any]:
    """Assemble the final deterministic both-side coverage result payload.

    This helper does not re-run any validation queries. It only combines the
    already-computed inputs, derives safe ratios, assigns the final
    pass/warn/fail status, and returns a stable result structure for reporting.
    """

    total_markets = int(metadata_metrics.get("total_markets", 0))
    markets_with_both_tokens = int(metadata_metrics.get("markets_with_both_tokens", 0))
    markets_with_trades = int(trade_metrics.get("markets_with_trades", 0))
    markets_with_both_trade_sides = int(trade_metrics.get("markets_with_both_trade_sides", 0))

    both_token_ratio = _safe_ratio(markets_with_both_tokens, total_markets)
    both_side_trade_ratio = _safe_ratio(markets_with_both_trade_sides, markets_with_trades)

    if total_markets == 0:
        status = "fail"
        reason_code = "approved_universe_empty"
    elif markets_with_trades == 0:
        status = "warn"
        reason_code = "no_scoped_trade_rows"
    elif both_side_trade_ratio < threshold:
        status = "fail"
        reason_code = "coverage_below_threshold"
    else:
        status = "pass"
        reason_code = "coverage_ok"

    return {
        "status": status,
        "reason_code": reason_code,
        "metrics": {
            "total_markets": total_markets,
            "markets_with_both_tokens": markets_with_both_tokens,
            "both_token_ratio": both_token_ratio,
            "markets_with_trades": markets_with_trades,
            "markets_with_both_trade_sides": markets_with_both_trade_sides,
            "both_side_trade_ratio": both_side_trade_ratio,
            "threshold": float(threshold),
            "source_breakdown": source_breakdown,
            "contract_version": int(contract_version),
            "selector_version": str(selector_version),
            "universe_policy_source": str(universe_policy_source),
        },
        "failing_markets_sample": _limit_samples(failing_markets, sample_limit),
    }


def _validate_duplicate_trade_inflation(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Orchestrate duplicate inflation and dedupe validation for approved trades."""

    prerequisites = _validate_duplicate_inflation_prerequisites(conn, runtime, summary)
    if prerequisites is None:
        return

    approved_universe_empty = bool(prerequisites["approved_universe_empty"])
    scoped_rows: list[dict[str, Any]] = []
    if not approved_universe_empty:
        scoped_rows = _load_scoped_duplicate_validation_rows(
            conn,
            runtime.contract.trade_table_name,
            prerequisites["approved_market_ids"],
            runtime.contract,
        )

    identity_metrics = _load_duplicate_identity_metrics(scoped_rows)
    duplicate_group_metrics = _load_duplicate_group_metrics(scoped_rows)
    invalid_dedupe_key_metrics = _load_invalid_dedupe_key_metrics(scoped_rows)
    transaction_hash_metrics = _load_transaction_hash_population_metrics(scoped_rows)
    source_priority_conflicts = _load_source_priority_conflicts(scoped_rows)
    duplicate_groups_sample = _load_duplicate_groups_sample(
        scoped_rows,
        runtime.contract.sample_failure_limit,
    )

    result = _build_duplicate_inflation_result(
        identity_metrics,
        duplicate_group_metrics,
        invalid_dedupe_key_metrics,
        transaction_hash_metrics,
        source_priority_conflicts,
        duplicate_groups_sample,
        float(runtime.contract.thresholds.values["duplicate_inflation_max_ratio"]),
        float(runtime.contract.thresholds.values["transaction_hash_population_min_ratio"]),
        contract_version=runtime.contract.version,
        selector_version=runtime.contract.universe.selector_version,
        universe_policy_source=runtime.contract.universe.source,
        approved_universe_empty=approved_universe_empty,
    )
    status = str(result["status"])
    severity = "warn" if status == "warn" else ("info" if status == "pass" else "error")
    summary.add(
        "duplicate_trade_inflation",
        status,
        severity,
        "Computed duplicate inflation, dedupe readiness, and source-priority validation inside the approved universe.",
        reason_code=str(result["reason_code"]),
        metrics=dict(
            result["metrics"],
            duplicate_groups_sample=result["duplicate_groups_sample"],
        ),
    )


def _validate_duplicate_inflation_prerequisites(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> dict[str, Any] | None:
    """Validate schema and scope prerequisites for duplicate inflation checks."""

    required_tables = {runtime.contract.trade_table_name}
    missing_tables = sorted(
        table_name for table_name in required_tables if not _table_exists(conn, table_name)
    )
    if missing_tables:
        summary.add(
            "duplicate_trade_inflation",
            "fail",
            "error",
            "Cannot evaluate duplicate inflation because required tables are missing.",
            reason_code="duplicate_schema_missing",
            metrics={"missing_tables": missing_tables},
        )
        return None

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_trade_columns = {
        runtime.contract.dedupe.strongest_key_field,
        runtime.contract.dedupe.dedupe_key_field,
        runtime.contract.dedupe.source_priority_field,
        "condition_id",
        "asset_id",
        "outcome_side",
        "price",
        "size",
        "trade_time",
        "source",
        "market_id",
    }
    if not required_trade_columns.issubset(trade_columns):
        summary.add(
            "duplicate_trade_inflation",
            "fail",
            "error",
            "Cannot evaluate duplicate inflation because required trade columns are missing.",
            reason_code="duplicate_schema_missing",
            metrics={"missing_trade_columns": sorted(required_trade_columns - trade_columns)},
        )
        return None

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if approved_market_ids:
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": approved_market_ids,
            "approved_universe_empty": False,
        }

    if _latest_reason_code(summary, "approved_universe_scope") == "approved_universe_empty":
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": set(),
            "approved_universe_empty": True,
        }

    summary.add(
        "duplicate_trade_inflation",
        "fail",
        "error",
        "Duplicate inflation could not run because approved market scope could not be resolved.",
        reason_code="duplicate_scope_missing",
    )
    return None


def _load_scoped_duplicate_validation_rows(
    conn: sqlite3.Connection,
    trade_table_name: str,
    approved_market_ids: set[str],
    contract: Phase1ValidationContract,
) -> list[dict[str, Any]]:
    """Load deterministic approved-universe trade rows for duplicate validation."""

    if not approved_market_ids:
        return []

    row_identity_field = contract.dedupe.row_identity_field
    dedupe_key_field = contract.dedupe.dedupe_key_field
    strongest_key_field = contract.dedupe.strongest_key_field
    source_priority_field = contract.dedupe.source_priority_field
    where_clause, params = _build_market_scope_filter(approved_market_ids)
    rows = conn.execute(
        f"""
        SELECT
            {row_identity_field} AS row_id,
            market_id,
            condition_id,
            asset_id,
            outcome_side,
            price,
            size,
            trade_time,
            source,
            {source_priority_field} AS source_priority,
            {strongest_key_field} AS transaction_hash,
            {dedupe_key_field} AS dedupe_key
        FROM {trade_table_name}
        WHERE {where_clause}
        ORDER BY market_id ASC, condition_id ASC, trade_time ASC, row_id ASC
        """,
        params,
    ).fetchall()

    return [
        _prepare_duplicate_validation_row(dict(row), contract)
        for row in rows
    ]


def _prepare_duplicate_validation_row(
    raw_row: dict[str, Any],
    contract: Phase1ValidationContract,
) -> dict[str, Any]:
    """Normalize one trade row into the canonical duplicate-validation shape."""

    row_id = _normalize_duplicate_text(raw_row.get("row_id"))
    market_id = _normalize_duplicate_text(raw_row.get("market_id"))
    condition_id = _normalize_duplicate_text(raw_row.get("condition_id"))
    asset_id = _normalize_duplicate_text(raw_row.get("asset_id"))
    outcome_side = _normalize_duplicate_text(raw_row.get("outcome_side"))
    outcome_side_normalized = outcome_side.upper() if outcome_side else None
    price_normalized = _normalize_duplicate_numeric(raw_row.get("price"))
    size_normalized = _normalize_duplicate_numeric(raw_row.get("size"))
    normalized_timestamp = _normalize_duplicate_timestamp(raw_row.get("trade_time"))
    source = _normalize_duplicate_text(raw_row.get("source")) or "unknown"
    source_priority = _normalize_duplicate_int(raw_row.get("source_priority"))
    transaction_hash = _normalize_duplicate_text(raw_row.get("transaction_hash"))
    dedupe_key = _normalize_duplicate_text(raw_row.get("dedupe_key"))

    fallback_parts = [
        condition_id,
        asset_id,
        outcome_side_normalized,
        price_normalized,
        size_normalized,
        normalized_timestamp,
    ]
    fallback_ready = all(part is not None for part in fallback_parts)
    fallback_key = "|".join(str(part) for part in fallback_parts) if fallback_ready else None

    group_key_type: str | None = None
    group_key: str | None = None
    if transaction_hash:
        group_key_type = "transaction_hash"
        group_key = transaction_hash
    elif fallback_key is not None:
        group_key_type = "fallback_key"
        group_key = fallback_key

    expected_source_priority = contract.dedupe.source_priority_rank.get(source)
    source_unranked = expected_source_priority is None
    source_priority_matches = (
        False if source_unranked else source_priority == expected_source_priority
    )

    dedupe_key_missing = dedupe_key is None
    fallback_identity_incomplete = transaction_hash is None and fallback_key is None
    dedupe_key_invalid = (
        dedupe_key_missing
        or fallback_identity_incomplete
        or group_key is None
        or dedupe_key != group_key
    )

    return {
        "row_id": row_id or "",
        "market_id": market_id or "",
        "condition_id": condition_id or "",
        "asset_id": asset_id or "",
        "outcome_side": outcome_side_normalized or "",
        "price": price_normalized or "",
        "size": size_normalized or "",
        "normalized_timestamp": normalized_timestamp or "",
        "source": source,
        "source_priority": source_priority,
        "transaction_hash": transaction_hash,
        "dedupe_key": dedupe_key,
        "group_key_type": group_key_type,
        "group_key": group_key,
        "transaction_hash_present": transaction_hash is not None,
        "dedupe_key_valid": not dedupe_key_invalid,
        "dedupe_key_missing": dedupe_key_missing,
        "fallback_identity_incomplete": fallback_identity_incomplete,
        "source_unranked": source_unranked,
        "expected_source_priority": expected_source_priority,
        "source_priority_matches": source_priority_matches,
    }


def _load_duplicate_identity_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int | float]:
    """Return row-level dedupe and transaction-hash readiness metrics."""

    total_rows = len(scoped_rows)
    valid_dedupe_key_rows = sum(1 for row in scoped_rows if row["dedupe_key_valid"])
    transaction_hash_rows = sum(1 for row in scoped_rows if row["transaction_hash_present"])

    return {
        "total_rows": total_rows,
        "valid_dedupe_key_rows": valid_dedupe_key_rows,
        "transaction_hash_rows": transaction_hash_rows,
        "dedupe_key_population_rate": _safe_ratio(valid_dedupe_key_rows, total_rows),
        "transaction_hash_population_rate": _safe_ratio(transaction_hash_rows, total_rows),
    }


def _load_duplicate_group_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int | float]:
    """Group rows by canonical identity and compute duplicate inflation metrics."""

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in scoped_rows:
        group_key_type = row["group_key_type"]
        group_key = row["group_key"]
        if not group_key_type or not group_key:
            continue
        grouped_rows.setdefault((str(group_key_type), str(group_key)), []).append(row)

    duplicate_groups = [rows for rows in grouped_rows.values() if len(rows) > 1]
    duplicate_groups_count = len(duplicate_groups)
    duplicate_rows = sum(len(rows) - 1 for rows in duplicate_groups)
    total_rows = len(scoped_rows)

    return {
        "duplicate_groups_count": duplicate_groups_count,
        "duplicate_rows": duplicate_rows,
        "duplicate_rate": _safe_ratio(duplicate_rows, total_rows),
    }


def _load_source_priority_conflicts(
    scoped_rows: list[dict[str, Any]],
) -> int:
    """Return the number of duplicate groups with source-priority conflicts."""

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in scoped_rows:
        group_key_type = row["group_key_type"]
        group_key = row["group_key"]
        if not group_key_type or not group_key:
            continue
        grouped_rows.setdefault((str(group_key_type), str(group_key)), []).append(row)

    conflicts = 0
    for rows in grouped_rows.values():
        if len(rows) <= 1:
            continue

        has_conflict = False
        per_source_priorities: dict[str, set[int | None]] = {}
        for row in rows:
            if row["source_unranked"]:
                has_conflict = True
            elif not row["source_priority_matches"]:
                has_conflict = True

            source = str(row["source"])
            per_source_priorities.setdefault(source, set()).add(row["source_priority"])

        if any(len(values) > 1 for values in per_source_priorities.values()):
            has_conflict = True

        if has_conflict:
            conflicts += 1

    return conflicts


def _load_invalid_dedupe_key_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int]:
    """Return counts for missing or invalid dedupe-key readiness."""

    invalid_dedupe_key_count = sum(1 for row in scoped_rows if not row["dedupe_key_valid"])
    missing_dedupe_key_count = sum(1 for row in scoped_rows if row["dedupe_key_missing"])
    fallback_identity_incomplete_count = sum(
        1 for row in scoped_rows if row["fallback_identity_incomplete"]
    )

    return {
        "invalid_dedupe_key_count": invalid_dedupe_key_count,
        "missing_dedupe_key_count": missing_dedupe_key_count,
        "fallback_identity_incomplete_count": fallback_identity_incomplete_count,
    }


def _load_transaction_hash_population_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int | float]:
    """Return transaction-hash population metrics for approved-universe trades."""

    total_rows = len(scoped_rows)
    transaction_hash_rows = sum(1 for row in scoped_rows if row["transaction_hash_present"])
    missing_transaction_hash_count = total_rows - transaction_hash_rows

    return {
        "missing_transaction_hash_count": missing_transaction_hash_count,
        "missing_transaction_hash_rate": _safe_ratio(missing_transaction_hash_count, total_rows),
    }


def _load_duplicate_groups_sample(
    scoped_rows: list[dict[str, Any]],
    sample_limit: int,
) -> list[dict[str, Any]]:
    """Return deterministic duplicate-group sample rows for debugging and handoff."""

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in scoped_rows:
        group_key_type = row["group_key_type"]
        group_key = row["group_key"]
        if not group_key_type or not group_key:
            continue
        grouped_rows.setdefault((str(group_key_type), str(group_key)), []).append(row)

    samples: list[dict[str, Any]] = []
    for (group_key_type, group_key), rows in sorted(
        grouped_rows.items(),
        key=lambda item: (
            item[0][0],
            item[1][0]["market_id"],
            item[1][0]["condition_id"],
            item[0][1],
        ),
    ):
        if len(rows) <= 1:
            continue

        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row["market_id"],
                row["condition_id"],
                row["normalized_timestamp"],
                row["row_id"],
            ),
        )
        representative = sorted_rows[0]
        reason_codes = ["duplicate_group"]
        if any(row["dedupe_key_missing"] for row in rows):
            reason_codes.append("missing_dedupe_key")
        elif any(not row["dedupe_key_valid"] for row in rows):
            reason_codes.append("invalid_dedupe_key")
        if any(row["fallback_identity_incomplete"] for row in rows):
            reason_codes.append("fallback_identity_incomplete")
        if any(row["source_unranked"] for row in rows):
            reason_codes.append("source_not_ranked")
        if any(
            (not row["source_unranked"]) and (not row["source_priority_matches"])
            for row in rows
        ):
            reason_codes.append("source_priority_mismatch")

        per_source_priorities: dict[str, set[int | None]] = {}
        for row in rows:
            per_source_priorities.setdefault(str(row["source"]), set()).add(row["source_priority"])
        if (
            any(len(values) > 1 for values in per_source_priorities.values())
            or "source_not_ranked" in reason_codes
            or "source_priority_mismatch" in reason_codes
        ):
            reason_codes.append("source_priority_conflict")

        source_values = sorted({str(row["source"]) for row in rows})
        source_priority_values = sorted(
            {int(value) for row in rows for value in [row["source_priority"]] if value is not None}
        )
        samples.append(
            {
                "group_key_type": group_key_type,
                "group_key": group_key,
                "group_size": len(rows),
                "market_id": representative["market_id"],
                "condition_id": representative["condition_id"],
                "asset_id": representative["asset_id"],
                "outcome_side": representative["outcome_side"],
                "source_values": source_values,
                "source_priority_values": source_priority_values,
                "reason_codes": reason_codes,
            }
        )

    return _limit_samples(samples, sample_limit)


def _build_duplicate_inflation_result(
    identity_metrics: dict[str, int | float],
    duplicate_group_metrics: dict[str, int | float],
    invalid_dedupe_key_metrics: dict[str, int],
    transaction_hash_metrics: dict[str, int | float],
    source_priority_conflicts: int,
    duplicate_groups_sample: list[dict[str, Any]],
    duplicate_rate_threshold: float,
    transaction_hash_population_min_ratio: float,
    *,
    contract_version: int,
    selector_version: str,
    universe_policy_source: str,
    approved_universe_empty: bool,
) -> dict[str, Any]:
    """Assemble the final deterministic duplicate-validation result payload."""

    total_rows = int(identity_metrics.get("total_rows", 0))
    duplicate_rate = float(duplicate_group_metrics.get("duplicate_rate", 0.0))
    invalid_dedupe_key_count = int(invalid_dedupe_key_metrics.get("invalid_dedupe_key_count", 0))
    missing_transaction_hash_rate = float(
        transaction_hash_metrics.get("missing_transaction_hash_rate", 0.0)
    )
    max_missing_transaction_hash_rate = max(0.0, 1.0 - float(transaction_hash_population_min_ratio))

    if approved_universe_empty:
        status = "fail"
        reason_code = "approved_universe_empty"
    elif total_rows == 0:
        status = "warn"
        reason_code = "no_scoped_trade_rows"
    elif duplicate_rate > float(duplicate_rate_threshold):
        status = "fail"
        reason_code = "duplicate_rate_above_threshold"
    elif source_priority_conflicts > 0:
        status = "fail"
        reason_code = "source_priority_conflict"
    elif invalid_dedupe_key_count > 0:
        status = "fail"
        reason_code = "missing_dedupe_key"
    elif missing_transaction_hash_rate > max_missing_transaction_hash_rate:
        status = "fail"
        reason_code = "missing_transaction_hash_above_threshold"
    else:
        status = "pass"
        reason_code = "duplicate_validation_ok"

    return {
        "status": status,
        "reason_code": reason_code,
        "metrics": {
            "total_rows": total_rows,
            "valid_dedupe_key_rows": int(identity_metrics.get("valid_dedupe_key_rows", 0)),
            "dedupe_key_population_rate": float(identity_metrics.get("dedupe_key_population_rate", 0.0)),
            "transaction_hash_rows": int(identity_metrics.get("transaction_hash_rows", 0)),
            "transaction_hash_population_rate": float(
                identity_metrics.get("transaction_hash_population_rate", 0.0)
            ),
            "duplicate_groups_count": int(duplicate_group_metrics.get("duplicate_groups_count", 0)),
            "duplicate_rows": int(duplicate_group_metrics.get("duplicate_rows", 0)),
            "duplicate_rate": duplicate_rate,
            "invalid_dedupe_key_count": invalid_dedupe_key_count,
            "missing_dedupe_key_count": int(
                invalid_dedupe_key_metrics.get("missing_dedupe_key_count", 0)
            ),
            "fallback_identity_incomplete_count": int(
                invalid_dedupe_key_metrics.get("fallback_identity_incomplete_count", 0)
            ),
            "missing_transaction_hash_count": int(
                transaction_hash_metrics.get("missing_transaction_hash_count", 0)
            ),
            "missing_transaction_hash_rate": missing_transaction_hash_rate,
            "source_priority_conflicts": int(source_priority_conflicts),
            "duplicate_rate_threshold": float(duplicate_rate_threshold),
            "transaction_hash_population_min_ratio": float(transaction_hash_population_min_ratio),
            "contract_version": int(contract_version),
            "selector_version": str(selector_version),
            "universe_policy_source": str(universe_policy_source),
        },
        "duplicate_groups_sample": duplicate_groups_sample,
    }


def _normalize_duplicate_text(value: Any) -> str | None:
    """Normalize a duplicate-validation text field to stripped text or None."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_duplicate_numeric(value: Any) -> str | None:
    """Normalize numeric duplicate-key fields to a stable decimal string."""

    text = _normalize_duplicate_text(value)
    if text is None:
        return None

    try:
        normalized = format(Decimal(text), "f")
    except (InvalidOperation, ValueError):
        return text

    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _normalize_duplicate_timestamp(value: Any) -> str | None:
    """Normalize duplicate timestamps to a stable UTC ISO string when possible."""

    text = _normalize_duplicate_text(value)
    if text is None:
        return None

    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat()
    except (ValueError, TypeError):
        return text


def _normalize_duplicate_int(value: Any) -> int | None:
    """Normalize integer-like duplicate-validation fields to `int` or None."""

    text = _normalize_duplicate_text(value)
    if text is None:
        return None

    try:
        return int(text)
    except ValueError:
        return None


def _load_duplicate_breakdown_by_source(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
    dedupe_key_field: str,
) -> dict[str, dict[str, float | int]]:
    """Return source-level duplicate inflation metrics inside the approved universe."""

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    rows = conn.execute(
        f"""
        SELECT
            source,
            SUM(group_count) AS total_rows,
            SUM(CASE WHEN group_count > 1 THEN group_count - 1 ELSE 0 END) AS duplicate_rows
        FROM (
            SELECT
                source,
                {dedupe_key_field},
                COUNT(*) AS group_count
            FROM {runtime.contract.trade_table_name}
            WHERE {where_clause}
            GROUP BY source, {dedupe_key_field}
        )
        GROUP BY source
        ORDER BY source ASC
        """,
        params,
    ).fetchall()
    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        source = str(row["source"] or "unknown")
        total_rows = int(row["total_rows"] or 0)
        duplicate_rows = int(row["duplicate_rows"] or 0)
        breakdown[source] = {
            "total_rows": total_rows,
            "duplicate_rows": duplicate_rows,
            "inflation_ratio": _safe_ratio(duplicate_rows, total_rows),
        }
    return breakdown


def _load_duplicate_breakdown_by_tier(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
    dedupe_key_field: str,
) -> dict[str, dict[str, float | int]]:
    """Return market-segment duplicate inflation metrics when tier metadata exists."""

    market_columns = _table_columns(conn, "markets")
    if "tier" not in market_columns:
        return {}

    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="t.market_id")
    rows = conn.execute(
        f"""
        SELECT
            tier,
            SUM(group_count) AS total_rows,
            SUM(CASE WHEN group_count > 1 THEN group_count - 1 ELSE 0 END) AS duplicate_rows
        FROM (
            SELECT
                COALESCE(m.tier, 'unknown') AS tier,
                t.{dedupe_key_field} AS dedupe_key,
                COUNT(*) AS group_count
            FROM {runtime.contract.trade_table_name} t
            JOIN markets m ON m.market_id = t.market_id
            WHERE {where_clause}
            GROUP BY COALESCE(m.tier, 'unknown'), t.{dedupe_key_field}
        )
        GROUP BY tier
        ORDER BY tier ASC
        """,
        params,
    ).fetchall()
    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        tier = str(row["tier"] or "unknown")
        total_rows = int(row["total_rows"] or 0)
        duplicate_rows = int(row["duplicate_rows"] or 0)
        breakdown[tier] = {
            "total_rows": total_rows,
            "duplicate_rows": duplicate_rows,
            "inflation_ratio": _safe_ratio(duplicate_rows, total_rows),
        }
    return breakdown


def _load_source_priority_breakdown(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
    source_priority_field: str,
) -> dict[str, dict[str, float | int]]:
    """Return source-priority coherence ratios grouped by source."""

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    breakdown: dict[str, dict[str, float | int]] = {}
    for source_name, expected_rank in sorted(runtime.contract.dedupe.source_priority_rank.items()):
        total_rows = _query_scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {runtime.contract.trade_table_name}
            WHERE {where_clause}
              AND source = ?
            """,
            params + (source_name,),
        )
        if total_rows == 0:
            continue

        matched_rows = _query_scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {runtime.contract.trade_table_name}
            WHERE {where_clause}
              AND source = ?
              AND {source_priority_field} = ?
            """,
            params + (source_name, expected_rank),
        )
        breakdown[source_name] = {
            "total_rows": total_rows,
            "matched_rows": matched_rows,
            "ratio": _safe_ratio(matched_rows, total_rows),
            "expected_priority": expected_rank,
        }
    return breakdown


def _load_unmapped_source_priority_rows(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
) -> list[dict[str, Any]]:
    """Return sources that are present in data but missing from the priority policy."""

    configured_sources = tuple(sorted(runtime.contract.dedupe.source_priority_rank))
    if not configured_sources:
        return []

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    rows = conn.execute(
        f"""
        SELECT source, COUNT(*) AS row_count
        FROM {runtime.contract.trade_table_name}
        WHERE {where_clause}
          AND source NOT IN ({",".join("?" for _ in configured_sources)})
        GROUP BY source
        ORDER BY source ASC
        """,
        params + configured_sources,
    ).fetchall()
    return [
        {
            "source": str(row["source"]),
            "row_count": int(row["row_count"] or 0),
            "reason_code": "source_not_ranked",
        }
        for row in rows
    ]


def _validate_wallet_integrity(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Orchestrate wallet null-rate and wallet-integrity validation."""

    prerequisites = _validate_wallet_integrity_prerequisites(conn, runtime, summary)
    if prerequisites is None:
        return

    approved_universe_empty = bool(prerequisites["approved_universe_empty"])
    scoped_rows: list[dict[str, Any]] = []
    if not approved_universe_empty:
        scoped_rows = _load_scoped_wallet_integrity_rows(
            conn,
            runtime.contract.trade_table_name,
            prerequisites["approved_market_ids"],
            prerequisites["trade_columns"],
        )

    metrics = _load_wallet_null_rate_metrics(scoped_rows)
    source_breakdown = _load_wallet_source_breakdown(scoped_rows)
    failing_rows_sample = _load_wallet_failing_rows_sample(
        scoped_rows,
        runtime.contract.sample_failure_limit,
    )
    proxy_wallet_threshold = float(
        runtime.contract.thresholds.values.get("wallet_field_null_rate_max", {}).get("proxy_wallet", 0.0)
    )
    result = _build_wallet_integrity_result(
        metrics,
        source_breakdown,
        failing_rows_sample,
        proxy_wallet_threshold,
        contract_version=runtime.contract.version,
        selector_version=runtime.contract.universe.selector_version,
        universe_policy_source=runtime.contract.universe.source,
        approved_universe_empty=approved_universe_empty,
    )
    status = str(result["status"])
    severity = "warn" if status == "warn" else ("info" if status == "pass" else "error")
    summary.add(
        "wallet_integrity",
        status,
        severity,
        "Computed wallet null-rate and wallet integrity metrics inside the approved universe.",
        reason_code=str(result["reason_code"]),
        metrics=dict(result["metrics"], failing_rows_sample=result["failing_rows_sample"]),
    )


def _validate_wallet_integrity_prerequisites(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> dict[str, Any] | None:
    """Validate schema and scope prerequisites for wallet-integrity checks."""

    if not _table_exists(conn, runtime.contract.trade_table_name):
        summary.add(
            "wallet_integrity",
            "fail",
            "error",
            "Cannot evaluate wallet integrity because the trades table is missing.",
            reason_code="wallet_schema_missing",
            metrics={"missing_tables": [runtime.contract.trade_table_name]},
        )
        return None

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_trade_columns = {"proxy_wallet", "transaction_hash", "source", "market_id"}
    if not required_trade_columns.issubset(trade_columns):
        summary.add(
            "wallet_integrity",
            "fail",
            "error",
            "Cannot evaluate wallet integrity because required trade columns are missing.",
            reason_code="wallet_schema_missing",
            metrics={
                "missing_trade_columns": sorted(required_trade_columns - trade_columns),
                "condition_id_supported": "condition_id" in trade_columns,
            },
        )
        return None

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if approved_market_ids:
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": approved_market_ids,
            "approved_universe_empty": False,
        }

    if _latest_reason_code(summary, "approved_universe_scope") == "approved_universe_empty":
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": set(),
            "approved_universe_empty": True,
        }

    summary.add(
        "wallet_integrity",
        "fail",
        "error",
        "Wallet integrity could not run because approved market scope could not be resolved.",
        reason_code="wallet_scope_missing",
    )
    return None


def _is_valid_proxy_wallet(value: Any) -> bool:
    """Return whether a proxy wallet matches the locked EVM-style format rule."""

    text = _normalize_duplicate_text(value)
    if text is None:
        return False
    if not text.startswith("0x"):
        return False
    if len(text) != 42:
        return False
    return all(character in hexdigits for character in text[2:])


def _load_scoped_wallet_integrity_rows(
    conn: sqlite3.Connection,
    trade_table_name: str,
    approved_market_ids: set[str],
    trade_columns: set[str],
) -> list[dict[str, Any]]:
    """Load deterministic approved-universe trade rows for wallet validation."""

    if not approved_market_ids:
        return []

    trade_id_select = "trade_id" if "trade_id" in trade_columns else "NULL"
    condition_id_select = "condition_id" if "condition_id" in trade_columns else "NULL"
    where_clause, params = _build_market_scope_filter(approved_market_ids)
    rows = conn.execute(
        f"""
        SELECT
            {trade_id_select} AS trade_id,
            market_id,
            {condition_id_select} AS condition_id,
            source,
            proxy_wallet,
            transaction_hash
        FROM {trade_table_name}
        WHERE {where_clause}
        ORDER BY market_id ASC, condition_id ASC, source ASC, trade_id ASC
        """,
        params,
    ).fetchall()
    return [_prepare_wallet_integrity_row(dict(row)) for row in rows]


def _prepare_wallet_integrity_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    """Normalize one trade row into the wallet-integrity validation shape."""

    trade_id = _normalize_duplicate_text(raw_row.get("trade_id")) or ""
    market_id = _normalize_duplicate_text(raw_row.get("market_id")) or ""
    condition_id = _normalize_duplicate_text(raw_row.get("condition_id")) or ""
    source = _normalize_duplicate_text(raw_row.get("source")) or "unknown"
    proxy_wallet = _normalize_duplicate_text(raw_row.get("proxy_wallet"))
    transaction_hash = _normalize_duplicate_text(raw_row.get("transaction_hash"))

    wallet_missing = proxy_wallet is None
    wallet_valid = False if wallet_missing else _is_valid_proxy_wallet(proxy_wallet)
    invalid_wallet_format = (not wallet_missing) and (not wallet_valid)
    transaction_hash_missing = transaction_hash is None
    wallet_present_txn_missing = wallet_valid and transaction_hash_missing
    wallet_missing_txn_present = wallet_missing and (not transaction_hash_missing)
    wallet_and_txn_mismatch = wallet_present_txn_missing or wallet_missing_txn_present

    return {
        "trade_id": trade_id,
        "market_id": market_id,
        "condition_id": condition_id,
        "source": source,
        "proxy_wallet": proxy_wallet or "",
        "transaction_hash": transaction_hash or "",
        "wallet_missing": wallet_missing,
        "wallet_valid": wallet_valid,
        "invalid_wallet_format": invalid_wallet_format,
        "transaction_hash_missing": transaction_hash_missing,
        "wallet_present_txn_missing": wallet_present_txn_missing,
        "wallet_missing_txn_present": wallet_missing_txn_present,
        "wallet_and_txn_mismatch": wallet_and_txn_mismatch,
    }


def _build_wallet_metric_payload(rows: list[dict[str, Any]]) -> dict[str, int | float]:
    """Return aggregate wallet-integrity metrics for a prepared row set."""

    total_rows = len(rows)
    proxy_wallet_null_count = sum(1 for row in rows if row["wallet_missing"])
    invalid_wallet_format_count = sum(1 for row in rows if row["invalid_wallet_format"])
    transaction_hash_missing_count = sum(1 for row in rows if row["transaction_hash_missing"])
    wallet_and_txn_mismatch_count = sum(1 for row in rows if row["wallet_and_txn_mismatch"])
    valid_wallet_count = sum(1 for row in rows if row["wallet_valid"])
    wallet_present_txn_missing_count = sum(1 for row in rows if row["wallet_present_txn_missing"])
    wallet_missing_txn_present_count = sum(1 for row in rows if row["wallet_missing_txn_present"])

    return {
        "total_rows": total_rows,
        "proxy_wallet_null_count": proxy_wallet_null_count,
        "proxy_wallet_null_rate": _safe_ratio(proxy_wallet_null_count, total_rows),
        "invalid_wallet_format_count": invalid_wallet_format_count,
        "transaction_hash_missing_count": transaction_hash_missing_count,
        "transaction_hash_missing_rate": _safe_ratio(transaction_hash_missing_count, total_rows),
        "wallet_and_txn_mismatch_count": wallet_and_txn_mismatch_count,
        "valid_wallet_count": valid_wallet_count,
        "valid_wallet_rate": _safe_ratio(valid_wallet_count, total_rows),
        "wallet_present_txn_missing_count": wallet_present_txn_missing_count,
        "wallet_missing_txn_present_count": wallet_missing_txn_present_count,
    }


def _load_wallet_null_rate_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int | float]:
    """Return aggregate wallet null-rate and integrity metrics."""

    return _build_wallet_metric_payload(scoped_rows)


def _load_wallet_source_breakdown(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, dict[str, int | float]]:
    """Return wallet-integrity metrics grouped deterministically by source."""

    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in scoped_rows:
        grouped_rows.setdefault(str(row["source"]), []).append(row)

    return {
        source: _build_wallet_metric_payload(grouped_rows[source])
        for source in sorted(grouped_rows)
    }


def _load_wallet_failing_rows_sample(
    scoped_rows: list[dict[str, Any]],
    sample_limit: int,
) -> list[dict[str, Any]]:
    """Return a deterministic sample of wallet-integrity failures."""

    failing_rows: list[dict[str, Any]] = []
    for row in sorted(
        scoped_rows,
        key=lambda item: (
            item["source"],
            item["market_id"],
            item["condition_id"],
            item["trade_id"],
        ),
    ):
        reason_codes: list[str] = []
        if row["wallet_missing"]:
            reason_codes.append("missing_proxy_wallet")
        if row["invalid_wallet_format"]:
            reason_codes.append("invalid_wallet_format")
        if row["wallet_present_txn_missing"]:
            reason_codes.append("wallet_transaction_mismatch")
        if row["wallet_missing_txn_present"]:
            reason_codes.append("partial_wallet_data")

        if reason_codes:
            failing_rows.append(
                {
                    "trade_id": row["trade_id"],
                    "market_id": row["market_id"],
                    "condition_id": row["condition_id"],
                    "source": row["source"],
                    "proxy_wallet": row["proxy_wallet"],
                    "transaction_hash": row["transaction_hash"],
                    "reason_codes": reason_codes,
                }
            )

    return _limit_samples(failing_rows, sample_limit)


def _build_wallet_integrity_result(
    metrics: dict[str, int | float],
    source_breakdown: dict[str, dict[str, int | float]],
    failing_rows_sample: list[dict[str, Any]],
    proxy_wallet_threshold: float,
    *,
    contract_version: int,
    selector_version: str,
    universe_policy_source: str,
    approved_universe_empty: bool,
) -> dict[str, Any]:
    """Assemble the final deterministic wallet-integrity result payload."""

    total_rows = int(metrics.get("total_rows", 0))
    proxy_wallet_null_rate = float(metrics.get("proxy_wallet_null_rate", 0.0))
    invalid_wallet_format_count = int(metrics.get("invalid_wallet_format_count", 0))
    wallet_and_txn_mismatch_count = int(metrics.get("wallet_and_txn_mismatch_count", 0))
    wallet_missing_txn_present_count = int(metrics.get("wallet_missing_txn_present_count", 0))
    wallet_present_txn_missing_count = int(metrics.get("wallet_present_txn_missing_count", 0))

    if approved_universe_empty:
        status = "fail"
        reason_code = "approved_universe_empty"
    elif total_rows == 0:
        status = "warn"
        reason_code = "no_scoped_trade_rows"
    elif proxy_wallet_null_rate > proxy_wallet_threshold:
        status = "fail"
        reason_code = "wallet_null_rate_above_threshold"
    elif invalid_wallet_format_count > 0:
        status = "fail"
        reason_code = "invalid_wallet_format"
    elif wallet_present_txn_missing_count > 0:
        status = "fail"
        reason_code = "wallet_transaction_mismatch"
    elif wallet_missing_txn_present_count > 0:
        status = "warn"
        reason_code = "partial_wallet_data"
    elif wallet_and_txn_mismatch_count > 0:
        status = "fail"
        reason_code = "wallet_transaction_mismatch"
    else:
        status = "pass"
        reason_code = "wallet_validation_ok"

    return {
        "status": status,
        "reason_code": reason_code,
        "metrics": {
            "total_rows": total_rows,
            "proxy_wallet_null_count": int(metrics.get("proxy_wallet_null_count", 0)),
            "proxy_wallet_null_rate": proxy_wallet_null_rate,
            "invalid_wallet_format_count": invalid_wallet_format_count,
            "transaction_hash_missing_count": int(metrics.get("transaction_hash_missing_count", 0)),
            "transaction_hash_missing_rate": float(metrics.get("transaction_hash_missing_rate", 0.0)),
            "wallet_and_txn_mismatch_count": wallet_and_txn_mismatch_count,
            "valid_wallet_count": int(metrics.get("valid_wallet_count", 0)),
            "valid_wallet_rate": float(metrics.get("valid_wallet_rate", 0.0)),
            "wallet_present_txn_missing_count": wallet_present_txn_missing_count,
            "wallet_missing_txn_present_count": wallet_missing_txn_present_count,
            "threshold": float(proxy_wallet_threshold),
            "source_breakdown": source_breakdown,
            "contract_version": int(contract_version),
            "selector_version": str(selector_version),
            "universe_policy_source": str(universe_policy_source),
        },
        "failing_rows_sample": failing_rows_sample,
    }


def _validate_condition_integrity(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Orchestrate condition_id integrity and consistency validation."""

    prerequisites = _validate_condition_integrity_prerequisites(conn, runtime, summary)
    if prerequisites is None:
        return

    approved_universe_empty = bool(prerequisites["approved_universe_empty"])
    scoped_rows: list[dict[str, Any]] = []
    if not approved_universe_empty:
        scoped_rows = _load_condition_integrity_rows(
            conn,
            runtime.contract.trade_table_name,
            prerequisites["approved_market_ids"],
            prerequisites["trade_columns"],
        )

    metrics = _load_condition_integrity_metrics(scoped_rows)
    source_breakdown = _load_condition_source_breakdown(scoped_rows)
    failing_conditions_sample = _load_condition_failing_samples(
        scoped_rows,
        runtime.contract.sample_failure_limit,
    )
    result = _build_condition_integrity_result(
        metrics,
        source_breakdown,
        failing_conditions_sample,
        contract_version=runtime.contract.version,
        selector_version=runtime.contract.universe.selector_version,
        universe_policy_source=runtime.contract.universe.source,
        approved_universe_empty=approved_universe_empty,
    )
    status = str(result["status"])
    severity = "warn" if status == "warn" else ("info" if status == "pass" else "error")
    summary.add(
        "condition_integrity",
        status,
        severity,
        "Computed condition_id integrity, mapping consistency, and per-condition coverage inside the approved universe.",
        reason_code=str(result["reason_code"]),
        metrics=dict(
            result["metrics"],
            failing_conditions_sample=result["failing_conditions_sample"],
        ),
    )


def _validate_condition_integrity_prerequisites(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> dict[str, Any] | None:
    """Validate schema and scope prerequisites for condition-integrity checks."""

    required_tables = {runtime.contract.trade_table_name, "markets"}
    missing_tables = sorted(
        table_name for table_name in required_tables if not _table_exists(conn, table_name)
    )
    if missing_tables:
        summary.add(
            "condition_integrity",
            "fail",
            "error",
            "Cannot evaluate condition integrity because required tables are missing.",
            reason_code="condition_schema_missing",
            metrics={"missing_tables": missing_tables},
        )
        return None

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    market_columns = _table_columns(conn, "markets")
    required_trade_columns = {"condition_id", "market_id", "asset_id", "outcome_side", "source"}
    required_market_columns = {"market_id", "condition_id", "yes_token_id", "no_token_id"}

    if not required_trade_columns.issubset(trade_columns) or not required_market_columns.issubset(market_columns):
        summary.add(
            "condition_integrity",
            "fail",
            "error",
            "Cannot evaluate condition integrity because required columns are missing.",
            reason_code="condition_schema_missing",
            metrics={
                "missing_trade_columns": sorted(required_trade_columns - trade_columns),
                "missing_market_columns": sorted(required_market_columns - market_columns),
            },
        )
        return None

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if approved_market_ids:
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": approved_market_ids,
            "approved_universe_empty": False,
        }

    if _latest_reason_code(summary, "approved_universe_scope") == "approved_universe_empty":
        return {
            "trade_columns": trade_columns,
            "approved_market_ids": set(),
            "approved_universe_empty": True,
        }

    summary.add(
        "condition_integrity",
        "fail",
        "error",
        "Condition integrity could not run because approved market scope could not be resolved.",
        reason_code="condition_scope_missing",
    )
    return None


def _load_condition_integrity_rows(
    conn: sqlite3.Connection,
    trade_table_name: str,
    approved_market_ids: set[str],
    trade_columns: set[str],
) -> list[dict[str, Any]]:
    """Load deterministic approved-universe trade/market rows for condition validation."""

    if not approved_market_ids:
        return []

    trade_id_select = "t.trade_id" if "trade_id" in trade_columns else "NULL"
    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="t.market_id")
    rows = conn.execute(
        f"""
        SELECT
            {trade_id_select} AS trade_id,
            t.condition_id AS trade_condition_id,
            t.market_id AS market_id,
            t.asset_id AS asset_id,
            t.outcome_side AS outcome_side,
            t.source AS source,
            m.condition_id AS market_condition_id,
            m.yes_token_id AS expected_yes_token_id,
            m.no_token_id AS expected_no_token_id
        FROM {trade_table_name} t
        JOIN markets m
          ON m.market_id = t.market_id
        WHERE {where_clause}
        ORDER BY t.condition_id ASC, t.market_id ASC, t.source ASC, trade_id ASC
        """,
        params,
    ).fetchall()

    return [_prepare_condition_integrity_row(dict(row)) for row in rows]


def _prepare_condition_integrity_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    """Normalize one scoped trade/market row into the condition-integrity shape."""

    trade_id = _normalize_duplicate_text(raw_row.get("trade_id")) or ""
    trade_condition_id = _normalize_duplicate_text(raw_row.get("trade_condition_id"))
    market_condition_id = _normalize_duplicate_text(raw_row.get("market_condition_id"))
    market_id = _normalize_duplicate_text(raw_row.get("market_id")) or ""
    asset_id = _normalize_duplicate_text(raw_row.get("asset_id"))
    normalized_outcome_side = (_normalize_duplicate_text(raw_row.get("outcome_side")) or "").upper()
    source = _normalize_duplicate_text(raw_row.get("source")) or "unknown"
    expected_yes_token_id = _normalize_duplicate_text(raw_row.get("expected_yes_token_id")) or ""
    expected_no_token_id = _normalize_duplicate_text(raw_row.get("expected_no_token_id")) or ""

    condition_id_missing = trade_condition_id is None
    condition_market_mismatch = (
        trade_condition_id is not None
        and market_condition_id is not None
        and trade_condition_id != market_condition_id
    )
    valid_outcome_side = normalized_outcome_side in {"YES", "NO"}
    invalid_outcome_side = not valid_outcome_side
    invalid_asset_mapping = False
    if valid_outcome_side:
        expected_asset_id = expected_yes_token_id if normalized_outcome_side == "YES" else expected_no_token_id
        invalid_asset_mapping = asset_id != expected_asset_id

    coverage_condition_id = None
    if not condition_id_missing and not condition_market_mismatch:
        coverage_condition_id = trade_condition_id

    return {
        "trade_id": trade_id,
        "condition_id": trade_condition_id or "",
        "coverage_condition_id": coverage_condition_id or "",
        "market_condition_id": market_condition_id or "",
        "market_id": market_id,
        "asset_id": asset_id or "",
        "outcome_side": normalized_outcome_side,
        "source": source,
        "expected_yes_token_id": expected_yes_token_id,
        "expected_no_token_id": expected_no_token_id,
        "condition_id_missing": condition_id_missing,
        "condition_market_mismatch": condition_market_mismatch,
        "invalid_outcome_side": invalid_outcome_side,
        "invalid_asset_mapping": invalid_asset_mapping,
        "has_yes_trade": bool(coverage_condition_id and normalized_outcome_side == "YES" and not invalid_asset_mapping),
        "has_no_trade": bool(coverage_condition_id and normalized_outcome_side == "NO" and not invalid_asset_mapping),
    }


def _build_condition_metric_payload(rows: list[dict[str, Any]]) -> dict[str, int | float]:
    """Return aggregate condition-integrity metrics for a prepared row set."""

    total_rows = len(rows)
    condition_id_missing_count = sum(1 for row in rows if row["condition_id_missing"])
    condition_market_mismatch_count = sum(1 for row in rows if row["condition_market_mismatch"])
    invalid_asset_mapping_count = sum(1 for row in rows if row["invalid_asset_mapping"])
    invalid_outcome_side_count = sum(1 for row in rows if row["invalid_outcome_side"])

    coverage_map: dict[str, dict[str, bool]] = {}
    for row in rows:
        coverage_condition_id = str(row["coverage_condition_id"])
        if not coverage_condition_id:
            continue
        state = coverage_map.setdefault(coverage_condition_id, {"yes": False, "no": False})
        if row["has_yes_trade"]:
            state["yes"] = True
        if row["has_no_trade"]:
            state["no"] = True

    condition_count = len(coverage_map)
    conditions_with_yes_trades = sum(1 for state in coverage_map.values() if state["yes"])
    conditions_with_no_trades = sum(1 for state in coverage_map.values() if state["no"])
    conditions_with_both_sides = sum(1 for state in coverage_map.values() if state["yes"] and state["no"])
    incomplete_condition_coverage_count = sum(
        1 for state in coverage_map.values() if not (state["yes"] and state["no"])
    )

    return {
        "total_rows": total_rows,
        "condition_id_missing_count": condition_id_missing_count,
        "condition_id_missing_rate": _safe_ratio(condition_id_missing_count, total_rows),
        "condition_market_mismatch_count": condition_market_mismatch_count,
        "invalid_asset_mapping_count": invalid_asset_mapping_count,
        "invalid_outcome_side_count": invalid_outcome_side_count,
        "condition_count": condition_count,
        "conditions_with_yes_trades": conditions_with_yes_trades,
        "conditions_with_no_trades": conditions_with_no_trades,
        "conditions_with_both_sides": conditions_with_both_sides,
        "both_side_coverage_per_condition": _safe_ratio(conditions_with_both_sides, condition_count),
        "incomplete_condition_coverage_count": incomplete_condition_coverage_count,
    }


def _load_condition_integrity_metrics(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, int | float]:
    """Return aggregate condition_id integrity and coverage metrics."""

    return _build_condition_metric_payload(scoped_rows)


def _load_condition_source_breakdown(
    scoped_rows: list[dict[str, Any]],
) -> dict[str, dict[str, int | float]]:
    """Return condition-integrity metrics grouped deterministically by source."""

    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in scoped_rows:
        grouped_rows.setdefault(str(row["source"]), []).append(row)

    return {
        source: _build_condition_metric_payload(grouped_rows[source])
        for source in sorted(grouped_rows)
    }


def _load_condition_failing_samples(
    scoped_rows: list[dict[str, Any]],
    sample_limit: int,
) -> list[dict[str, Any]]:
    """Return deterministic condition-level failing samples grouped by condition and market."""

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in scoped_rows:
        group_key = (str(row["condition_id"]), str(row["market_id"]))
        grouped_rows.setdefault(group_key, []).append(row)

    failing_samples: list[dict[str, Any]] = []
    for (condition_id, market_id), rows in sorted(grouped_rows.items(), key=lambda item: (item[0][0], item[0][1])):
        yes_trade_count = sum(1 for row in rows if row["has_yes_trade"])
        no_trade_count = sum(1 for row in rows if row["has_no_trade"])
        reason_codes: list[str] = []
        if any(row["condition_id_missing"] for row in rows):
            reason_codes.append("condition_id_missing")
        if any(row["condition_market_mismatch"] for row in rows):
            reason_codes.append("condition_market_mismatch")
        if any(row["invalid_asset_mapping"] for row in rows):
            reason_codes.append("invalid_asset_mapping")
        if any(row["invalid_outcome_side"] for row in rows):
            reason_codes.append("invalid_outcome_side")
        if (
            any(str(row["coverage_condition_id"]) for row in rows)
            and (yes_trade_count == 0 or no_trade_count == 0)
        ):
            reason_codes.append("incomplete_condition_coverage")

        if reason_codes:
            failing_samples.append(
                {
                    "condition_id": condition_id,
                    "market_id": market_id,
                    "source_values": sorted({str(row["source"]) for row in rows}),
                    "yes_trade_count": yes_trade_count,
                    "no_trade_count": no_trade_count,
                    "expected_yes_token_id": str(rows[0]["expected_yes_token_id"]),
                    "expected_no_token_id": str(rows[0]["expected_no_token_id"]),
                    "observed_asset_ids": sorted({str(row["asset_id"]) for row in rows if str(row["asset_id"])}),
                    "reason_codes": reason_codes,
                }
            )

    return _limit_samples(failing_samples, sample_limit)


def _build_condition_integrity_result(
    metrics: dict[str, int | float],
    source_breakdown: dict[str, dict[str, int | float]],
    failing_conditions_sample: list[dict[str, Any]],
    *,
    contract_version: int,
    selector_version: str,
    universe_policy_source: str,
    approved_universe_empty: bool,
) -> dict[str, Any]:
    """Assemble the final deterministic condition-integrity result payload."""

    total_rows = int(metrics.get("total_rows", 0))
    condition_id_missing_rate = float(metrics.get("condition_id_missing_rate", 0.0))
    condition_market_mismatch_count = int(metrics.get("condition_market_mismatch_count", 0))
    invalid_asset_mapping_count = int(metrics.get("invalid_asset_mapping_count", 0))
    invalid_outcome_side_count = int(metrics.get("invalid_outcome_side_count", 0))
    incomplete_condition_coverage_count = int(metrics.get("incomplete_condition_coverage_count", 0))

    if approved_universe_empty:
        status = "fail"
        reason_code = "approved_universe_empty"
    elif total_rows == 0:
        status = "warn"
        reason_code = "no_scoped_trade_rows"
    elif condition_id_missing_rate > 0:
        status = "fail"
        reason_code = "condition_id_missing"
    elif condition_market_mismatch_count > 0:
        status = "fail"
        reason_code = "condition_market_mismatch"
    elif invalid_asset_mapping_count > 0:
        status = "fail"
        reason_code = "invalid_asset_mapping"
    elif invalid_outcome_side_count > 0:
        status = "fail"
        reason_code = "invalid_outcome_side"
    elif incomplete_condition_coverage_count > 0:
        status = "warn"
        reason_code = "incomplete_condition_coverage"
    else:
        status = "pass"
        reason_code = "condition_validation_ok"

    return {
        "status": status,
        "reason_code": reason_code,
        "metrics": {
            "total_rows": total_rows,
            "condition_id_missing_count": int(metrics.get("condition_id_missing_count", 0)),
            "condition_id_missing_rate": condition_id_missing_rate,
            "condition_market_mismatch_count": condition_market_mismatch_count,
            "invalid_asset_mapping_count": invalid_asset_mapping_count,
            "invalid_outcome_side_count": invalid_outcome_side_count,
            "condition_count": int(metrics.get("condition_count", 0)),
            "conditions_with_yes_trades": int(metrics.get("conditions_with_yes_trades", 0)),
            "conditions_with_no_trades": int(metrics.get("conditions_with_no_trades", 0)),
            "conditions_with_both_sides": int(metrics.get("conditions_with_both_sides", 0)),
            "both_side_coverage_per_condition": float(metrics.get("both_side_coverage_per_condition", 0.0)),
            "incomplete_condition_coverage_count": incomplete_condition_coverage_count,
            "source_breakdown": source_breakdown,
            "contract_version": int(contract_version),
            "selector_version": str(selector_version),
            "universe_policy_source": str(universe_policy_source),
        },
        "failing_conditions_sample": failing_conditions_sample,
    }


def _validate_candidate_review_output(runtime: ValidationRuntime, summary: ValidationSummary) -> None:
    """Validate that candidate-review output is present and schema-compatible."""

    raw_config = _read_yaml(runtime.config_path)
    candidate_review = raw_config.get("candidate_review", {})
    if not candidate_review.get("required", False):
        return

    report_path = Path(candidate_review.get("report_path", ""))
    required_columns = [str(value) for value in candidate_review.get("required_columns", [])]
    required_non_empty_columns = [
        str(value) for value in candidate_review.get("required_non_empty_columns", [])
    ]
    if not report_path.exists():
        summary.add(
            "candidate_review_output",
            "fail",
            "error",
            f"Candidate-review output is configured but not present yet: {report_path}",
            reason_code="candidate_review_missing",
        )
        return

    try:
        with report_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames or []
            rows = list(reader)
    except OSError as exc:
        summary.add(
            "candidate_review_output",
            "fail",
            "error",
            f"Could not read candidate-review output: {exc}",
            reason_code="candidate_review_read_failed",
        )
        return

    missing_columns = sorted(column for column in required_columns if column not in columns)
    if missing_columns:
        summary.add(
            "candidate_review_output",
            "fail",
            "error",
            "Candidate-review output exists but is missing required columns.",
            reason_code="candidate_review_schema_missing",
            metrics={"missing_columns": missing_columns},
        )
        return

    blank_required_counts = {
        column: sum(1 for row in rows if not str(row.get(column, "")).strip())
        for column in required_non_empty_columns
    }
    if any(count > 0 for count in blank_required_counts.values()):
        summary.add(
            "candidate_review_output",
            "fail",
            "error",
            "Candidate-review output has blank values in required reproducibility or reason-code columns.",
            reason_code="candidate_review_blank_required_values",
            metrics={"blank_required_counts": blank_required_counts},
        )
        return

    summary.add(
        "candidate_review_output",
        "pass",
        "info",
        "Candidate-review output is present, schema-compatible, and reproducible from the configured path.",
        metrics={
            "report_path": str(report_path),
            "row_count": len(rows),
            "required_columns": required_columns,
        },
    )


def _validate_transaction_hash_population(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate transaction_hash population independently of generic null-rate checks."""

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "transaction_hash" not in trade_columns:
        summary.add(
            "transaction_hash_population",
            "fail",
            "error",
            "Cannot evaluate transaction_hash population because the trade table lacks transaction_hash.",
            reason_code="transaction_hash_missing",
        )
        return

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "transaction_hash_population",
            "fail",
            "error",
            "Transaction-hash population could not run because approved market scope could not be resolved.",
            reason_code="transaction_hash_scope_missing",
        )
        return

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    total_rows = _query_scalar(
        conn,
        f"SELECT COUNT(*) FROM {runtime.contract.trade_table_name} WHERE {where_clause}",
        params,
    )
    if total_rows == 0:
        summary.add(
            "transaction_hash_population",
            "warn",
            "warn",
            "Transaction-hash population check skipped because no approved-universe trades are available.",
            reason_code="no_trade_rows",
        )
        return

    populated_rows = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {runtime.contract.trade_table_name}
        WHERE {where_clause}
          AND transaction_hash IS NOT NULL
          AND TRIM(CAST(transaction_hash AS TEXT)) != ''
        """,
        params,
    )
    population_ratio = _safe_ratio(populated_rows, total_rows)
    threshold = float(runtime.contract.thresholds.values["transaction_hash_population_min_ratio"])
    status = "pass" if population_ratio >= threshold else "fail"
    summary.add(
        "transaction_hash_population",
        status,
        "info" if status == "pass" else "error",
        "Computed populated transaction_hash ratio for canonical trades.",
        reason_code=None if status == "pass" else "transaction_hash_population_below_threshold",
        metrics={
            "total_rows": total_rows,
            "populated_rows": populated_rows,
            "population_ratio": population_ratio,
            "threshold": threshold,
        },
    )


def _validate_asset_outcome_correctness(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate asset_id population and outcome-side correctness against market descriptors."""

    markets_columns = _table_columns(conn, "markets")
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_market_columns = {"market_id", "yes_token_id", "no_token_id", "condition_id"}
    required_trade_columns = {"market_id", "condition_id", "asset_id", "outcome_side"}

    if not required_market_columns.issubset(markets_columns) or not required_trade_columns.issubset(trade_columns):
        summary.add(
            "asset_outcome_correctness",
            "fail",
            "error",
            "Cannot validate asset/outcome correctness because required market descriptor or trade columns are missing.",
            reason_code="asset_outcome_schema_missing",
            metrics={
                "missing_market_columns": sorted(required_market_columns - markets_columns),
                "missing_trade_columns": sorted(required_trade_columns - trade_columns),
            },
        )
        return

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "asset_outcome_correctness",
            "fail",
            "error",
            "Asset/outcome correctness could not run because approved market scope could not be resolved.",
            reason_code="asset_outcome_scope_missing",
        )
        return

    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="t.market_id")
    total_rows = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {runtime.contract.trade_table_name} t
        WHERE {where_clause}
        """,
        params,
    )
    if total_rows == 0:
        summary.add(
            "asset_outcome_correctness",
            "warn",
            "warn",
            "Asset/outcome correctness check skipped because no approved-universe trades are available.",
            reason_code="no_trade_rows",
        )
        return

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN t.asset_id IS NOT NULL AND TRIM(CAST(t.asset_id AS TEXT)) != '' THEN 1 END) AS populated_asset_rows,
            COUNT(CASE WHEN t.outcome_side IN ('YES', 'NO') THEN 1 END) AS valid_outcome_rows,
            COUNT(
                CASE
                    WHEN (t.outcome_side = 'YES' AND t.asset_id = m.yes_token_id)
                      OR (t.outcome_side = 'NO' AND t.asset_id = m.no_token_id)
                    THEN 1
                END
            ) AS descriptor_matched_rows,
            COUNT(
                CASE
                    WHEN t.condition_id IS NOT NULL AND m.condition_id IS NOT NULL AND t.condition_id = m.condition_id
                    THEN 1
                END
            ) AS condition_matched_rows
        FROM {runtime.contract.trade_table_name} t
        JOIN markets m ON m.market_id = t.market_id
        WHERE {where_clause}
        """,
        params,
    ).fetchone()

    asset_population_ratio = _safe_ratio(row["populated_asset_rows"], row["total_rows"])
    outcome_validity_ratio = _safe_ratio(row["valid_outcome_rows"], row["total_rows"])
    descriptor_match_ratio = _safe_ratio(row["descriptor_matched_rows"], row["total_rows"])
    condition_match_ratio = _safe_ratio(row["condition_matched_rows"], row["total_rows"])

    thresholds = runtime.contract.thresholds.values
    passes = (
        asset_population_ratio >= float(thresholds["asset_id_population_min_ratio"])
        and outcome_validity_ratio >= float(thresholds["outcome_side_validity_min_ratio"])
        and descriptor_match_ratio >= float(thresholds["outcome_side_validity_min_ratio"])
        and condition_match_ratio >= float(thresholds["condition_id_population_min_ratio"])
    )
    summary.add(
        "asset_outcome_correctness",
        "pass" if passes else "fail",
        "info" if passes else "error",
        "Computed asset-id and outcome-side correctness against market descriptors.",
        reason_code=None if passes else "asset_outcome_mismatch",
        metrics={
            "approved_trade_count": total_rows,
            "asset_population_ratio": asset_population_ratio,
            "outcome_validity_ratio": outcome_validity_ratio,
            "descriptor_match_ratio": descriptor_match_ratio,
            "condition_match_ratio": condition_match_ratio,
        },
    )


def _validate_wallet_field_null_rate(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate configured null-rate thresholds for wallet-linked trade fields."""

    thresholds = runtime.contract.thresholds.values.get("wallet_field_null_rate_max", {})
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "wallet_field_null_rate_scope",
            "fail",
            "error",
            "Wallet null-rate checks could not run because approved market scope could not be resolved.",
            reason_code="wallet_null_rate_scope_missing",
        )
        return

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    total_rows = _query_scalar(
        conn,
        f"SELECT COUNT(*) FROM {runtime.contract.trade_table_name} WHERE {where_clause}",
        params,
    )

    for field_name, max_ratio in thresholds.items():
        if field_name not in trade_columns:
            summary.add(
                f"wallet_null_rate::{field_name}",
                "fail",
                "error",
                f"Wallet field '{field_name}' is missing from the trade table.",
                reason_code="wallet_field_missing",
            )
            continue

        if total_rows == 0:
            summary.add(
                f"wallet_null_rate::{field_name}",
                "warn",
                "warn",
                f"Wallet null-rate check skipped for '{field_name}' because no approved-universe trades are available.",
                reason_code="no_trade_rows",
            )
            continue

        null_rows = _query_scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {runtime.contract.trade_table_name}
            WHERE {where_clause}
              AND ({field_name} IS NULL OR TRIM(CAST({field_name} AS TEXT)) = '')
            """,
            params,
        )
        null_ratio = _safe_ratio(null_rows, total_rows)
        status = "pass" if null_ratio <= float(max_ratio) else "fail"
        severity = "info" if status == "pass" else "error"
        by_source = _load_wallet_null_rate_by_source(conn, runtime, approved_market_ids, field_name)
        by_market_segment = _load_wallet_null_rate_by_tier(
            conn,
            runtime,
            approved_market_ids,
            field_name,
        )

        summary.add(
            f"wallet_null_rate::{field_name}",
            status,
            severity,
            f"Computed null-rate for wallet field '{field_name}'.",
            reason_code=None if status == "pass" else "wallet_null_rate_above_threshold",
            metrics={
                "field_name": field_name,
                "total_rows": total_rows,
                "null_rows": null_rows,
                "null_ratio": null_ratio,
                "by_source": by_source,
                "by_market_segment": by_market_segment,
                "threshold": float(max_ratio),
            },
        )


def _load_wallet_null_rate_by_source(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
    field_name: str,
) -> dict[str, dict[str, float | int]]:
    """Return source-level null-rate metrics for one wallet-linked field."""

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "source" not in trade_columns:
        return {}

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    rows = conn.execute(
        f"""
        SELECT
            source,
            COUNT(*) AS total_rows,
            COUNT(
                CASE
                    WHEN {field_name} IS NULL OR TRIM(CAST({field_name} AS TEXT)) = ''
                    THEN 1
                END
            ) AS null_rows
        FROM {runtime.contract.trade_table_name}
        WHERE {where_clause}
        GROUP BY source
        ORDER BY source ASC
        """,
        params,
    ).fetchall()
    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        source = str(row["source"] or "unknown")
        total_rows = int(row["total_rows"] or 0)
        null_rows = int(row["null_rows"] or 0)
        breakdown[source] = {
            "total_rows": total_rows,
            "null_rows": null_rows,
            "null_ratio": _safe_ratio(null_rows, total_rows),
        }
    return breakdown


def _load_wallet_null_rate_by_tier(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
    field_name: str,
) -> dict[str, dict[str, float | int]]:
    """Return market-segment null-rate metrics when tier metadata exists."""

    market_columns = _table_columns(conn, "markets")
    if "tier" not in market_columns:
        return {}

    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="t.market_id")
    rows = conn.execute(
        f"""
        SELECT
            COALESCE(m.tier, 'unknown') AS tier,
            COUNT(*) AS total_rows,
            COUNT(
                CASE
                    WHEN t.{field_name} IS NULL OR TRIM(CAST(t.{field_name} AS TEXT)) = ''
                    THEN 1
                END
            ) AS null_rows
        FROM {runtime.contract.trade_table_name} t
        JOIN markets m ON m.market_id = t.market_id
        WHERE {where_clause}
        GROUP BY COALESCE(m.tier, 'unknown')
        ORDER BY tier ASC
        """,
        params,
    ).fetchall()
    breakdown: dict[str, dict[str, float | int]] = {}
    for row in rows:
        tier = str(row["tier"] or "unknown")
        total_rows = int(row["total_rows"] or 0)
        null_rows = int(row["null_rows"] or 0)
        breakdown[tier] = {
            "total_rows": total_rows,
            "null_rows": null_rows,
            "null_ratio": _safe_ratio(null_rows, total_rows),
        }
    return breakdown


def _validate_condition_id_population(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate the condition_id population rate on the trade table."""

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "condition_id" not in trade_columns:
        summary.add(
            "condition_id_population",
            "fail",
            "error",
            "Cannot evaluate condition_id population because the trade table lacks condition_id.",
            reason_code="condition_id_missing",
        )
        return

    approved_market_ids = _get_approved_market_ids(conn, runtime, summary)
    if not approved_market_ids:
        summary.add(
            "condition_id_population",
            "fail",
            "error",
            "Condition-id population could not run because approved market scope could not be resolved.",
            reason_code="condition_id_scope_missing",
        )
        return

    where_clause, params = _build_market_scope_filter(approved_market_ids, market_column="t.market_id")
    total_rows = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {runtime.contract.trade_table_name} t
        WHERE {where_clause}
        """,
        params,
    )
    if total_rows == 0:
        summary.add(
            "condition_id_population",
            "warn",
            "warn",
            "Condition-id population check skipped because no approved-universe trades are available.",
            reason_code="no_trade_rows",
        )
        return

    populated_rows = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {runtime.contract.trade_table_name} t
        WHERE {where_clause}
          AND condition_id IS NOT NULL
          AND TRIM(CAST(condition_id AS TEXT)) != ''
        """,
        params,
    )
    population_ratio = _safe_ratio(populated_rows, total_rows)
    threshold = float(runtime.contract.thresholds.values["condition_id_population_min_ratio"])
    status = "pass" if population_ratio >= threshold else "fail"
    severity = "info" if status == "pass" else "error"
    by_source = _load_condition_population_by_source(conn, runtime, approved_market_ids)
    mismatched_rows = _query_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {runtime.contract.trade_table_name} t
        JOIN markets m ON m.market_id = t.market_id
        WHERE {where_clause}
          AND t.condition_id IS NOT NULL
          AND m.condition_id IS NOT NULL
          AND t.condition_id != m.condition_id
        """,
        params,
    )

    summary.add(
        "condition_id_population",
        status,
        severity,
        "Computed populated condition_id ratio for canonical trades.",
        reason_code=None if status == "pass" else "condition_id_population_below_threshold",
        metrics={
            "total_rows": total_rows,
            "populated_rows": populated_rows,
            "population_ratio": population_ratio,
            "by_source": by_source,
            "market_descriptor_mismatch_rows": mismatched_rows,
            "threshold": threshold,
        },
    )


def _load_condition_population_by_source(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    approved_market_ids: set[str],
) -> dict[str, dict[str, float | int]]:
    """Return source-level condition_id population ratios inside the approved universe."""

    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    if "source" not in trade_columns:
        return {}

    where_clause, params = _build_market_scope_filter(approved_market_ids)
    return _metric_ratio_breakdown(
        conn,
        f"""
        SELECT
            source AS group_key,
            COUNT(*) AS total_rows,
            COUNT(
                CASE
                    WHEN condition_id IS NOT NULL AND TRIM(CAST(condition_id AS TEXT)) != ''
                    THEN 1
                END
            ) AS matched_rows
        FROM {runtime.contract.trade_table_name}
        WHERE {where_clause}
        GROUP BY source
        ORDER BY source ASC
        """,
        params,
    )


def _validate_first_seen_semantics(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate that first_seen_at can be derived deterministically."""

    semantic_rule = runtime.contract.semantics["first_seen_at"]
    entity_field = str(semantic_rule.details.get("entity_field", "proxy_wallet"))
    event_time_field = str(semantic_rule.details.get("event_time_field", "trade_time"))
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_columns = {entity_field, event_time_field}

    if not required_columns.issubset(trade_columns):
        summary.add(
            "first_seen_at_semantics",
            "fail",
            "error",
            "Cannot derive first_seen_at because required trade columns are missing.",
            reason_code="first_seen_schema_missing",
            metrics={"missing_columns": sorted(required_columns - trade_columns)},
        )
        return

    records = derive_wallet_first_seen(conn, runtime.contract.trade_table_name, semantic_rule)
    if not records:
        summary.add(
            "first_seen_at_semantics",
            "warn",
            "warn",
            "first_seen_at semantics are configured, but no wallet-backed trade records are available yet.",
            reason_code="no_wallet_records",
        )
        return

    summary.add(
        "first_seen_at_semantics",
        "pass",
        "info",
        "Derived deterministic archive-backed first_seen_at values for wallet entities.",
        metrics={
            "wallet_count": len(records),
            "earliest_first_seen_at": records[0].first_seen_at,
            "latest_first_seen_at": records[-1].first_seen_at,
        },
    )


def _validate_fresh_wallet_semantics(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate that fresh-wallet flags can be computed deterministically."""

    first_seen_rule = runtime.contract.semantics["first_seen_at"]
    fresh_wallet_rule = runtime.contract.semantics["fresh_wallet"]
    entity_field = str(fresh_wallet_rule.details.get("entity_field", "proxy_wallet"))
    event_time_field = str(fresh_wallet_rule.details.get("event_time_field", "trade_time"))
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    required_columns = {entity_field, event_time_field}

    if not required_columns.issubset(trade_columns):
        summary.add(
            "fresh_wallet_semantics",
            "fail",
            "error",
            "Cannot compute fresh-wallet flags because required trade columns are missing.",
            reason_code="fresh_wallet_schema_missing",
            metrics={"missing_columns": sorted(required_columns - trade_columns)},
        )
        return

    records = derive_fresh_wallet_flags(
        conn,
        runtime.contract.trade_table_name,
        first_seen_rule,
        fresh_wallet_rule,
    )
    if not records:
        summary.add(
            "fresh_wallet_semantics",
            "warn",
            "warn",
            "Fresh-wallet semantics are configured, but no eligible wallet records are available yet.",
            reason_code="no_wallet_records",
        )
        return

    confidence_counts: dict[str, int] = {}
    fresh_wallet_count = 0
    for record in records:
        confidence_counts[record.confidence] = confidence_counts.get(record.confidence, 0) + 1
        if record.is_fresh:
            fresh_wallet_count += 1

    summary.add(
        "fresh_wallet_semantics",
        "pass",
        "info",
        "Computed first-version fresh-wallet flags using archive age and trading footprint thresholds.",
        metrics={
            "wallet_count": len(records),
            "fresh_wallet_count": fresh_wallet_count,
            "confidence_counts": confidence_counts,
            "max_age_days": float(fresh_wallet_rule.details.get("max_age_days", 30)),
            "max_trade_count": int(fresh_wallet_rule.details.get("max_trade_count", 3)),
        },
    )


def _validate_episode_linkage_semantics(
    conn: sqlite3.Connection,
    runtime: ValidationRuntime,
    summary: ValidationSummary,
) -> None:
    """Validate that trade-to-market episodes can be linked deterministically."""

    semantic_rule = runtime.contract.semantics["episode_linkage"]
    trade_columns = _table_columns(conn, runtime.contract.trade_table_name)
    join_keys = {str(value) for value in semantic_rule.details.get("join_keys", [])}
    required_columns = join_keys.union(
        {
            str(semantic_rule.details.get("event_time_field", "trade_time")),
            str(semantic_rule.details.get("wallet_key", "proxy_wallet")),
            str(semantic_rule.details.get("outcome_side_field", "outcome_side")),
        }
    )

    if not required_columns.issubset(trade_columns):
        summary.add(
            "episode_linkage_semantics",
            "fail",
            "error",
            "Cannot derive trade-to-market episodes because required columns are missing.",
            reason_code="episode_linkage_schema_missing",
            metrics={"missing_columns": sorted(required_columns - trade_columns)},
        )
        return

    episodes, unresolved_count = derive_trade_episode_linkage(
        conn,
        runtime.contract.trade_table_name,
        semantic_rule,
    )
    if not episodes:
        status = "warn" if unresolved_count == 0 else "fail"
        severity = "warn" if status == "warn" else "error"
        summary.add(
            "episode_linkage_semantics",
            status,
            severity,
            "Episode linkage did not yield any resolved episodes from the current trade set.",
            reason_code="no_resolved_episodes",
            metrics={"unresolved_trade_count": unresolved_count},
        )
        return

    summary.add(
        "episode_linkage_semantics",
        "pass",
        "info",
        "Derived deterministic market episodes from canonical trades.",
        metrics={
            "episode_count": len(episodes),
            "unresolved_trade_count": unresolved_count,
            "max_trade_count": max(episode.trade_count for episode in episodes),
            "max_distinct_wallet_count": max(episode.distinct_wallet_count for episode in episodes),
            "gap_minutes": float(semantic_rule.details.get("gap_minutes", 30)),
        },
    )


def _query_scalar(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> int:
    """Execute a scalar query and normalize missing results to zero."""

    row = conn.execute(query, params).fetchone()
    if row is None:
        return 0
    value = row[0]
    return int(value or 0)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    """Return a numeric ratio while guarding against division by zero."""

    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)
