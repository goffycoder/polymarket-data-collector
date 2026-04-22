from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import REPO_ROOT
from phase7 import sha256_file


OPERATING_MODE_CONTRACT_VERSION = "phase8_v1_operating_mode_v1"
CANONICAL_OPERATING_MODE = "rule_based_plus_shadow_ml"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_head() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _file_artifact(path_value: str, *, kind: str, note: str | None = None) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    exists = path.exists()
    artifact: dict[str, Any] = {
        "kind": kind,
        "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/") if exists or str(path).startswith(str(REPO_ROOT)) else path_value,
        "exists": exists,
        "note": note,
        "sha256": sha256_file(path) if exists and path.is_file() else None,
    }
    if exists and path.is_file():
        artifact["size_bytes"] = path.stat().st_size
    return artifact


def build_v1_operating_mode_manifest() -> dict[str, Any]:
    supporting_evidence = [
        _file_artifact(
            "Documentation/SRS.tex",
            kind="requirements_source",
            note="Defines v1 completeness, stage gates, Phase 6 shadow-first rollout, and Phase 7 as post-v1 advanced research.",
        ),
        _file_artifact(
            "Documentation/phases/phase4.tex",
            kind="phase_doc",
            note="States that Phase 4 remains rule-based and operator-facing.",
        ),
        _file_artifact(
            "Documentation/phases/phase5.tex",
            kind="phase_doc",
            note="Defines the historical validation and conservative paper-trading layer required before strong ranking claims.",
        ),
        _file_artifact(
            "Documentation/person1Phases/phase6_person1.tex",
            kind="supporting_doc",
            note="Explicitly says ML integration begins in shadow mode only and rule-based alerting remains authoritative until evidence is convincing.",
        ),
        _file_artifact(
            "Documentation/person2Phases/phase6_person2.tex",
            kind="supporting_doc",
            note="Frames the model as needing baseline comparisons, calibration, and shadow-mode review before deserving continued shadowing or promotion.",
        ),
        _file_artifact(
            "phase6/training.py",
            kind="runtime_module",
            note="Committed Phase 6 trainer currently fits a linear starter ranker, not the final boosted-tree model described by the full Phase 6 scope.",
        ),
        _file_artifact(
            "phase6/reporting.py",
            kind="runtime_module",
            note="Model card text says thresholds are shadow-only recommendations and identifies the starter ranker as a linear baseline foundation.",
        ),
        _file_artifact(
            "Documentation/person2Phases/phase7_person2.tex",
            kind="supporting_doc",
            note="Defines Phase 7 as advanced research on top of a stable Phase 6 baseline, not as canonical v1 promotion by default.",
        ),
        _file_artifact(
            "phase7/reporting.py",
            kind="runtime_module",
            note="Phase 7 strict-holdout assessment only accepts gains that beat the Phase 6 baseline in both validation and test.",
        ),
        _file_artifact(
            "phase7/observability.py",
            kind="runtime_module",
            note="Phase 7 observability study generates deployment implications that caution against increasing operator trust without stronger evidence.",
        ),
    ]

    return {
        "contract_version": OPERATING_MODE_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "git_commit": _git_head(),
        "decision": {
            "canonical_v1_operating_mode": CANONICAL_OPERATING_MODE,
            "rejected_modes": [
                {
                    "mode": "rule_based_only",
                    "reason": "The repo already contains committed Phase 6 registry, shadow-scoring, and evaluation plumbing that should remain active for v1 learning and auditability.",
                },
                {
                    "mode": "ml_backed_ranking_with_rollback",
                    "reason": "The committed repo state does not justify promotion of ML to decision authority: Phase 6 is shadow-first by design, the current trainer is still a linear starter ranker, and no materialized local evidence packet proves safe promotion.",
                },
            ],
        },
        "authoritative_v1_path": {
            "decision_logic": [
                "phase3 deterministic candidate generation",
                "phase4 evidence enrichment and alert rendering",
                "phase4 suppression and analyst workflow",
            ],
            "validation_contract": [
                "phase5 replay reproducibility",
                "phase5 conservative paper-trading and holdouts",
            ],
            "ml_role": [
                "phase6 feature materialization and model registry",
                "phase6 shadow scoring with traceable provenance",
                "phase6 evaluation and calibration as advisory evidence",
            ],
            "operator_rule": "Rule-based candidate and alert behavior remains authoritative for v1. ML scores may be recorded, compared, and reviewed, but they do not decide whether an alert exists.",
            "rollback_rule": "If ML plumbing is disabled, shadow models can be retired or ignored without changing the authoritative rule-based alert path.",
        },
        "promotion_decision": {
            "phase6_shadow_policy": "allowed_and_recommended",
            "phase6_ml_authority": "not_promoted_to_authoritative_v1",
            "phase7_model_promotion": "not_promoted_to_authoritative_v1",
            "promotion_preconditions_missing": [
                "materialized end-to-end runtime evidence packet in this workspace",
                "stronger proof that ML gains survive operationally relevant replay windows",
                "clear evidence that promotion improves outcomes without degrading explainability or rollback safety",
                "delivery-ready implementation of the full boosted-tree ranker promised by the SRS instead of only the committed linear starter baseline",
            ],
        },
        "phase7_classification": {
            "research_only": [
                "graph-derived feature families",
                "graph-aware advanced ranker artifacts",
                "marked Hawkes or TCN experiments",
                "ablation tables and thesis-quality figures",
                "phase7 research packages and experiment-ledger narratives as headline model claims",
            ],
            "may_influence_canonical_v1_story_but_not_decision_logic": [
                "observability and Goodhart warnings that constrain operator trust and deployment claims",
                "strict-holdout promotion discipline versus the Phase 6 baseline",
                "scale, storage, restore, and long-run dashboard guidance from the operational scale-up track",
                "reproducibility packaging standards for later thesis or defense artifacts",
            ],
            "explicit_non_promotion_rule": "No Phase 7 scoring artifact changes canonical v1 alert generation unless it first clears a separate promotion decision beyond this Task 3 contract.",
        },
        "rationale": [
            "The SRS definition of v1 complete requires one ranker to be evaluated against baselines; it does not require ML to become the authoritative alert path.",
            "The SRS and Phase 6 planning explicitly require shadow mode first.",
            "Phase 4's canonical single-owner plan keeps the alert loop rule-based and operator-facing before ML promotion.",
            "The committed Phase 6 implementation is still a linear starter ranker, and its own reporting code labels thresholds as shadow-only recommendations.",
            "Phase 7 is explicitly framed as advanced research after v1 stability, so it should not silently redefine the canonical v1 operating mode.",
        ],
        "supporting_evidence": supporting_evidence,
    }


def render_v1_operating_mode_markdown(manifest: dict[str, Any]) -> str:
    decision = manifest["decision"]
    authoritative = manifest["authoritative_v1_path"]
    phase7 = manifest["phase7_classification"]

    lines = [
        "# Phase 8 Canonical v1 Operating Mode",
        "",
        f"- Contract version: `{manifest['contract_version']}`",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Git commit: `{manifest['git_commit']}`",
        f"- Canonical mode: `{decision['canonical_v1_operating_mode']}`",
        "",
        "## Authoritative v1 Rule",
        f"- {authoritative['operator_rule']}",
        f"- {authoritative['rollback_rule']}",
        "",
        "## Rejected Modes",
    ]
    for rejected in decision["rejected_modes"]:
        lines.append(f"- `{rejected['mode']}`: {rejected['reason']}")
    lines.extend(["", "## Phase 7 Classification"])
    for item in phase7["research_only"]:
        lines.append(f"- Research-only: {item}")
    for item in phase7["may_influence_canonical_v1_story_but_not_decision_logic"]:
        lines.append(f"- Governance/ops influence only: {item}")
    lines.extend(["", "## Rationale"])
    for item in manifest["rationale"]:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"
