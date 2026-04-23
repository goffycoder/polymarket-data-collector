from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Protocol
from xml.etree import ElementTree

import httpx

from config.settings import (
    PHASE4_EVIDENCE_CACHE_TTL_SECONDS,
    PHASE4_EVIDENCE_PROVIDERS,
    PHASE4_EVIDENCE_TIMEOUT_SECONDS,
    PHASE4_GOOGLE_NEWS_RSS_CEID,
    PHASE4_GOOGLE_NEWS_RSS_COST_USD,
    PHASE4_GOOGLE_NEWS_RSS_DAILY_QUERY_CAP,
    PHASE4_GOOGLE_NEWS_RSS_GL,
    PHASE4_GOOGLE_NEWS_RSS_HL,
    PHASE4_GOOGLE_NEWS_RSS_MAX_RESULTS,
    PHASE4_GOOGLE_NEWS_RSS_MONTHLY_QUERY_CAP,
    PHASE4_GOOGLE_NEWS_RSS_URL,
)
from phase4.repository import Phase4Repository
from utils.http_client import make_client
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
    cache_key: str | None = None
    cache_hit: bool = False
    freshness_seconds: int | None = None


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
        return build_provider_query_text(candidate)


class GoogleNewsRssEvidenceProvider:
    name = "google_news_rss"
    query_type = "web_news"

    def __init__(
        self,
        *,
        repository: Phase4Repository,
        cache_ttl_seconds: int = PHASE4_EVIDENCE_CACHE_TTL_SECONDS,
        daily_query_cap: int = PHASE4_GOOGLE_NEWS_RSS_DAILY_QUERY_CAP,
        monthly_query_cap: int = PHASE4_GOOGLE_NEWS_RSS_MONTHLY_QUERY_CAP,
        estimated_cost_usd: float = PHASE4_GOOGLE_NEWS_RSS_COST_USD,
        max_results: int = PHASE4_GOOGLE_NEWS_RSS_MAX_RESULTS,
        url: str = PHASE4_GOOGLE_NEWS_RSS_URL,
        hl: str = PHASE4_GOOGLE_NEWS_RSS_HL,
        gl: str = PHASE4_GOOGLE_NEWS_RSS_GL,
        ceid: str = PHASE4_GOOGLE_NEWS_RSS_CEID,
    ):
        self.repository = repository
        self.cache_ttl_seconds = max(1, cache_ttl_seconds)
        self.daily_query_cap = max(1, daily_query_cap)
        self.monthly_query_cap = max(self.daily_query_cap, monthly_query_cap)
        self.estimated_cost_usd = max(0.0, float(estimated_cost_usd))
        self.max_results = max(1, max_results)
        self.url = url
        self.hl = hl
        self.gl = gl
        self.ceid = ceid

    async def fetch(self, candidate: dict[str, Any]) -> EvidenceProviderResult:
        query_text = build_provider_query_text(candidate)
        cache_key = f"{self.name}:{query_text.lower()}"
        cached = self.repository.latest_evidence_query_for_cache(
            provider_name=self.name,
            provider_query_type=self.query_type,
            provider_query_text=query_text,
            max_age_seconds=self.cache_ttl_seconds,
        )
        budget_before = self._budget_snapshot()
        if cached is not None:
            cached_metadata = cached.get("raw_response_metadata") or {}
            budget_metadata = self._budget_metadata(
                external_call_made=False,
                estimated_cost_usd=0.0,
                before=budget_before,
            )
            return EvidenceProviderResult(
                provider_name=self.name,
                provider_query_type=self.query_type,
                provider_query_text=query_text,
                result_count=int(cached.get("result_count") or 0),
                query_status=str(cached.get("query_status") or "no_results"),
                results=list(cached_metadata.get("normalized_results") or []),
                raw_response_metadata={
                    "provider_mode": "cache",
                    "cache_hit_query_id": cached.get("evidence_query_id"),
                    "cache_source_created_at": cached.get("created_at"),
                    "cache_ttl_seconds": self.cache_ttl_seconds,
                    "provider_url": self.url,
                    "budget": budget_metadata,
                    "normalized_results": list(cached_metadata.get("normalized_results") or []),
                    "provider_http_status": cached_metadata.get("provider_http_status"),
                    "provider_response_url": cached_metadata.get("provider_response_url"),
                    "provider_result_ids": list(cached_metadata.get("provider_result_ids") or []),
                },
                error_message=str(cached.get("error_message")) if cached.get("error_message") else None,
                cache_key=cache_key,
                cache_hit=True,
                freshness_seconds=int(cached.get("freshness_seconds") or 0),
            )

        if (
            budget_before["day_queries_used"] >= self.daily_query_cap
            or budget_before["month_queries_used"] >= self.monthly_query_cap
        ):
            budget_metadata = self._budget_metadata(
                external_call_made=False,
                estimated_cost_usd=0.0,
                before=budget_before,
                blocked=True,
            )
            return EvidenceProviderResult(
                provider_name=self.name,
                provider_query_type=self.query_type,
                provider_query_text=query_text,
                result_count=0,
                query_status="budget_blocked",
                results=[],
                raw_response_metadata={
                    "provider_mode": "budget_blocked",
                    "provider_url": self.url,
                    "cache_ttl_seconds": self.cache_ttl_seconds,
                    "budget": budget_metadata,
                    "normalized_results": [],
                },
                error_message="provider budget cap reached",
                cache_key=cache_key,
            )

        params = {
            "q": query_text,
            "hl": self.hl,
            "gl": self.gl,
            "ceid": self.ceid,
        }
        async with make_client(timeout=float(PHASE4_EVIDENCE_TIMEOUT_SECONDS)) as client:
            response = await client.get(self.url, params=params)
        parsed_results = self._parse_results(response.text)
        budget_after = self._budget_metadata(
            external_call_made=True,
            estimated_cost_usd=self.estimated_cost_usd,
            before=budget_before,
        )
        normalized_results = parsed_results[: self.max_results]
        query_status = "ok" if normalized_results else "no_results"
        return EvidenceProviderResult(
            provider_name=self.name,
            provider_query_type=self.query_type,
            provider_query_text=query_text,
            result_count=len(normalized_results),
            query_status=query_status,
            results=normalized_results,
            raw_response_metadata={
                "provider_mode": "live",
                "provider_http_status": response.status_code,
                "provider_response_url": str(response.url),
                "provider_url": self.url,
                "provider_params": params,
                "provider_result_ids": [row.get("result_id") for row in normalized_results if row.get("result_id")],
                "cache_ttl_seconds": self.cache_ttl_seconds,
                "budget": budget_after,
                "normalized_results": normalized_results,
            },
            cache_key=cache_key,
        )

    def _budget_snapshot(self) -> dict[str, float | int]:
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        usage = self.repository.provider_budget_usage(
            provider_name=self.name,
            day_start=day_start,
            month_start=month_start,
        )
        return {
            **usage,
            "day_query_cap": self.daily_query_cap,
            "month_query_cap": self.monthly_query_cap,
        }

    def _budget_metadata(
        self,
        *,
        external_call_made: bool,
        estimated_cost_usd: float,
        before: dict[str, float | int],
        blocked: bool = False,
    ) -> dict[str, Any]:
        day_queries_after = int(before["day_queries_used"]) + (1 if external_call_made else 0)
        month_queries_after = int(before["month_queries_used"]) + (1 if external_call_made else 0)
        day_spend_after = float(before["day_spend_usd"]) + estimated_cost_usd
        month_spend_after = float(before["month_spend_usd"]) + estimated_cost_usd
        return {
            "external_call_made": external_call_made,
            "estimated_cost_usd": round(float(estimated_cost_usd), 6),
            "day_queries_before": int(before["day_queries_used"]),
            "day_queries_after": day_queries_after,
            "month_queries_before": int(before["month_queries_used"]),
            "month_queries_after": month_queries_after,
            "day_query_cap": int(before["day_query_cap"]),
            "month_query_cap": int(before["month_query_cap"]),
            "day_spend_before_usd": round(float(before["day_spend_usd"]), 6),
            "day_spend_after_usd": round(day_spend_after, 6),
            "month_spend_before_usd": round(float(before["month_spend_usd"]), 6),
            "month_spend_after_usd": round(month_spend_after, 6),
            "blocked_by_budget": blocked,
        }

    def _parse_results(self, xml_text: str) -> list[dict[str, Any]]:
        root = ElementTree.fromstring(xml_text)
        results: list[dict[str, Any]] = []
        for item in root.findall("./channel/item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            published_at = (item.findtext("pubDate") or "").strip()
            guid = (item.findtext("guid") or link or title).strip()
            source = ""
            source_node = item.find("source")
            if source_node is not None and source_node.text:
                source = source_node.text.strip()
            results.append(
                {
                    "result_id": guid,
                    "title": title,
                    "url": link,
                    "published_at": published_at,
                    "source": source,
                }
            )
        return results[: self.max_results]


def build_provider_query_text(candidate: dict[str, Any]) -> str:
    return (
        candidate.get("event_title")
        or candidate.get("question")
        or candidate.get("event_slug")
        or candidate.get("market_id")
        or candidate.get("candidate_id")
        or ""
    )


def build_default_providers(*, repository: Phase4Repository) -> list[EvidenceProvider]:
    providers: list[EvidenceProvider] = []
    for provider_name in PHASE4_EVIDENCE_PROVIDERS:
        if provider_name == "google_news_rss":
            providers.append(GoogleNewsRssEvidenceProvider(repository=repository))
        elif provider_name == "noop_news":
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
        self.providers = providers or build_default_providers(repository=repository)
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
                    "cache_hit": result.cache_hit,
                    "cache_key": result.cache_key,
                    "freshness_seconds": result.freshness_seconds,
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
                        "cache_hit": result.cache_hit,
                        "freshness_seconds": result.freshness_seconds,
                        "budget": (result.raw_response_metadata or {}).get("budget"),
                        "provider_mode": (result.raw_response_metadata or {}).get("provider_mode"),
                    }
                    for result in provider_results
                ],
                "real_provider_count": sum(
                    1 for result in provider_results if not result.provider_name.startswith("noop_")
                ),
                "cache_hit_count": sum(1 for result in provider_results if result.cache_hit),
                "total_estimated_cost_usd": round(
                    sum(
                        float(((result.raw_response_metadata or {}).get("budget") or {}).get("estimated_cost_usd") or 0.0)
                        for result in provider_results
                    ),
                    6,
                ),
            },
            confidence_modifier=confidence_modifier,
            metadata_json={
                "event_title": candidate.get("event_title"),
                "question": candidate.get("question"),
                "trigger_time": candidate.get("trigger_time"),
            },
            cache_key="|".join(sorted(filter(None, (result.cache_key for result in provider_results)))) or str(candidate["candidate_id"]),
            freshness_seconds=min(
                (
                    int(result.freshness_seconds)
                    for result in provider_results
                    if result.freshness_seconds is not None
                ),
                default=0,
            ),
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
