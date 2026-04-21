from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Protocol

from config.settings import PHASE4_EVIDENCE_PROVIDERS, PHASE4_EVIDENCE_TIMEOUT_SECONDS
from phase4.repository import Phase4Repository
from utils.logger import get_logger

log = get_logger("phase4_evidence")


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class EvidenceProviderResult:
    provider_name: str
    provider_query_type: str
    provider_query_text: str
    result_count: int
    query_status: str
    results: list[dict[str, Any]]
    raw_response_metadata: dict[str, Any]
    error_message: str | None = None


class EvidenceProvider(Protocol):
    name: str
    query_type: str

    async def fetch(self, candidate: dict[str, Any]) -> EvidenceProviderResult:
        ...


class NoopEvidenceProvider:
    def __init__(self, *, name: str, query_type: str):
        self.name = name
        self.query_type = query_type

    async def fetch(self, candidate: dict[str, Any]) -> EvidenceProviderResult:
        query_text = self._build_query(candidate)
        return EvidenceProviderResult(
            provider_name=self.name,
            provider_query_type=self.query_type,
            provider_query_text=query_text,
            result_count=0,
            query_status="no_results",
            results=[],
            raw_response_metadata={"provider_mode": "noop"},
        )

    def _build_query(self, candidate: dict[str, Any]) -> str:
        return (
            candidate.get("event_title")
            or candidate.get("question")
            or candidate.get("event_slug")
            or candidate.get("market_id")
            or candidate.get("candidate_id")
            or ""
        )


def build_default_providers() -> list[EvidenceProvider]:
    providers: list[EvidenceProvider] = []
    for provider_name in PHASE4_EVIDENCE_PROVIDERS:
        if provider_name == "noop_news":
            providers.append(NoopEvidenceProvider(name="noop_news", query_type="web_news"))
        elif provider_name == "noop_social":
            providers.append(NoopEvidenceProvider(name="noop_social", query_type="social"))
        else:
            providers.append(NoopEvidenceProvider(name=provider_name, query_type="unknown"))
    return providers


def classify_evidence_state(results: list[EvidenceProviderResult]) -> tuple[str, float]:
    total_results = sum(result.result_count for result in results)
    success_count = sum(1 for result in results if result.query_status == "ok")

    if total_results >= 5:
        return "already_public", -0.25
    if total_results > 0 or success_count > 0:
        return "weakly_public", -0.1
    return "not_publicly_explained", 0.15


@dataclass(slots=True)
class EvidenceWorkerSummary:
    candidates_seen: int = 0
    candidates_processed: int = 0
    evidence_queries_written: int = 0
    evidence_snapshots_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "candidates_seen": self.candidates_seen,
            "candidates_processed": self.candidates_processed,
            "evidence_queries_written": self.evidence_queries_written,
            "evidence_snapshots_written": self.evidence_snapshots_written,
        }


class Phase4EvidenceWorker:
    def __init__(
        self,
        *,
        repository: Phase4Repository,
        providers: list[EvidenceProvider] | None = None,
        timeout_seconds: int = PHASE4_EVIDENCE_TIMEOUT_SECONDS,
    ):
        self.repository = repository
        self.providers = providers or build_default_providers()
        self.timeout_seconds = timeout_seconds
        self.summary = EvidenceWorkerSummary()

    async def process_pending_candidates(self, *, limit: int = 10) -> list[dict[str, Any]]:
        candidates = self.repository.pending_candidates(
            limit=limit,
            include_existing_alerts=True,
        )
        self.summary.candidates_seen += len(candidates)
        outputs: list[dict[str, Any]] = []

        for candidate in candidates:
            outputs.append(await self.process_candidate(candidate))

        return outputs

    async def process_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        provider_results: list[EvidenceProviderResult] = []

        for provider in self.providers:
            started_at = datetime.now(timezone.utc)
            start_clock = perf_counter()
            try:
                result = await asyncio.wait_for(
                    provider.fetch(candidate),
                    timeout=max(1, self.timeout_seconds),
                )
            except asyncio.TimeoutError:
                result = EvidenceProviderResult(
                    provider_name=provider.name,
                    provider_query_type=provider.query_type,
                    provider_query_text=(
                        candidate.get("event_title")
                        or candidate.get("question")
                        or candidate.get("market_id")
                        or ""
                    ),
                    result_count=0,
                    query_status="timeout",
                    results=[],
                    raw_response_metadata={"provider_mode": "timeout"},
                    error_message="provider timeout",
                )
            except Exception as exc:
                result = EvidenceProviderResult(
                    provider_name=provider.name,
                    provider_query_type=provider.query_type,
                    provider_query_text=(
                        candidate.get("event_title")
                        or candidate.get("question")
                        or candidate.get("market_id")
                        or ""
                    ),
                    result_count=0,
                    query_status="error",
                    results=[],
                    raw_response_metadata={"provider_mode": "error"},
                    error_message=str(exc),
                )
            completed_at = datetime.now(timezone.utc)
            latency_ms = (perf_counter() - start_clock) * 1000.0
            self.repository.record_evidence_query(
                candidate_id=str(candidate["candidate_id"]),
                provider_name=result.provider_name,
                provider_query_type=result.provider_query_type,
                provider_query_text=result.provider_query_text,
                request_started_at=_iso(started_at),
                response_completed_at=_iso(completed_at),
                latency_ms=latency_ms,
                result_count=result.result_count,
                query_status=result.query_status,
                timeout_seconds=self.timeout_seconds,
                raw_response_metadata={
                    **result.raw_response_metadata,
                    "result_preview_count": len(result.results),
                },
                error_message=result.error_message,
            )
            self.summary.evidence_queries_written += 1
            provider_results.append(result)

        evidence_state, confidence_modifier = classify_evidence_state(provider_results)
        snapshot_id = self.repository.record_evidence_snapshot(
            candidate_id=str(candidate["candidate_id"]),
            snapshot_time=_iso(datetime.now(timezone.utc)),
            evidence_state=evidence_state,
            provider_summary={
                "providers": [
                    {
                        "provider_name": result.provider_name,
                        "query_type": result.provider_query_type,
                        "query_status": result.query_status,
                        "result_count": result.result_count,
                    }
                    for result in provider_results
                ]
            },
            confidence_modifier=confidence_modifier,
            metadata_json={
                "event_title": candidate.get("event_title"),
                "question": candidate.get("question"),
                "trigger_time": candidate.get("trigger_time"),
            },
            cache_key=str(candidate["candidate_id"]),
            freshness_seconds=0,
        )
        self.summary.candidates_processed += 1
        self.summary.evidence_snapshots_written += 1

        payload = {
            "candidate_id": candidate["candidate_id"],
            "evidence_snapshot_id": snapshot_id,
            "evidence_state": evidence_state,
            "confidence_modifier": confidence_modifier,
            "providers": [result.provider_name for result in provider_results],
        }
        log.info(f"Phase 4 evidence snapshot created: {payload}")
        return payload
