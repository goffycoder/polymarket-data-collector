from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from phase3.detector import PHASE3_DETECTOR_VERSION, Phase3Repository, run_phase3_detector_window
from phase3.state_store import MemoryStateStore


def _candidate_signature(candidate: dict[str, Any]) -> str:
    rule_families = candidate.get("triggering_rules") or candidate.get("rule_families") or []
    if isinstance(rule_families, dict):
        normalized_rules = sorted(rule_families.keys())
    else:
        normalized_rules = sorted(str(rule) for rule in rule_families)
    return "|".join(
        [
            str(candidate.get("market_id") or ""),
            str(candidate.get("episode_end_event_time") or ""),
            ",".join(normalized_rules),
        ]
    )


class CaptureRepository(Phase3Repository):
    def __init__(self):
        super().__init__()
        self.candidates: list[dict[str, Any]] = []

    def register_detector_version(self, *, backend_name: str, notes: str) -> None:  # noqa: D401
        return None

    def persist_candidate(self, candidate: dict[str, Any]) -> None:  # noqa: D401
        self.candidates.append(candidate)


@dataclass(slots=True)
class Phase3ReconciliationReport:
    start: str
    end: str
    persisted_candidate_count: int
    replay_candidate_count: int
    missing_from_replay: list[str]
    extra_in_replay: list[str]
    replay_summary: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def build_phase3_reconciliation_report(*, start: str, end: str) -> Phase3ReconciliationReport:
    persisted_repo = Phase3Repository()
    persisted_candidates = persisted_repo.load_persisted_candidates(start=start, end=end)

    capture_repo = CaptureRepository()
    replay_summary = await run_phase3_detector_window(
        start=start,
        end=end,
        store=MemoryStateStore(),
        repository=capture_repo,
    )
    replay_candidates = capture_repo.candidates

    persisted_signatures = {_candidate_signature(candidate) for candidate in persisted_candidates}
    replay_signatures = {_candidate_signature(candidate) for candidate in replay_candidates}

    return Phase3ReconciliationReport(
        start=start,
        end=end,
        persisted_candidate_count=len(persisted_candidates),
        replay_candidate_count=len(replay_candidates),
        missing_from_replay=sorted(persisted_signatures - replay_signatures),
        extra_in_replay=sorted(replay_signatures - persisted_signatures),
        replay_summary=replay_summary.to_dict(),
    )
