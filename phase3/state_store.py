from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from config.settings import PHASE3_STATE_BACKEND, REDIS_URL
from utils.logger import get_logger

log = get_logger("phase3_state_store")

try:  # pragma: no cover - import availability depends on local env
    import redis.asyncio as redis_async
except ImportError:  # pragma: no cover - handled by factory fallback
    redis_async = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BaseStateStore:
    backend_name = "base"

    async def get_market_state(self, market_id: str) -> dict[str, Any]:
        raise NotImplementedError

    async def set_market_state(self, market_id: str, state: dict[str, Any]) -> None:
        raise NotImplementedError

    async def get_wallet_first_seen(self, wallet: str) -> str | None:
        raise NotImplementedError

    async def set_wallet_first_seen(self, wallet: str, first_seen_at: str) -> None:
        raise NotImplementedError

    async def get_last_candidate(self, market_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def set_last_candidate(self, market_id: str, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None


class MemoryStateStore(BaseStateStore):
    backend_name = "memory"

    def __init__(self):
        self._market_states: dict[str, dict[str, Any]] = {}
        self._wallet_first_seen: dict[str, str] = {}
        self._last_candidates: dict[str, dict[str, Any]] = {}

    async def get_market_state(self, market_id: str) -> dict[str, Any]:
        return json.loads(json.dumps(self._market_states.get(market_id, {})))

    async def set_market_state(self, market_id: str, state: dict[str, Any]) -> None:
        self._market_states[market_id] = json.loads(json.dumps(state))

    async def get_wallet_first_seen(self, wallet: str) -> str | None:
        return self._wallet_first_seen.get(wallet)

    async def set_wallet_first_seen(self, wallet: str, first_seen_at: str) -> None:
        self._wallet_first_seen.setdefault(wallet, first_seen_at)

    async def get_last_candidate(self, market_id: str) -> dict[str, Any] | None:
        payload = self._last_candidates.get(market_id)
        if payload is None:
            return None
        return json.loads(json.dumps(payload))

    async def set_last_candidate(self, market_id: str, payload: dict[str, Any]) -> None:
        self._last_candidates[market_id] = json.loads(json.dumps(payload))


class RedisStateStore(BaseStateStore):
    backend_name = "redis"

    def __init__(self, client):
        self._client = client

    @staticmethod
    def _market_key(market_id: str) -> str:
        return f"phase3:market_state:{market_id}"

    @staticmethod
    def _wallet_key(wallet: str) -> str:
        return f"phase3:wallet_first_seen:{wallet}"

    @staticmethod
    def _candidate_key(market_id: str) -> str:
        return f"phase3:last_candidate:{market_id}"

    async def get_market_state(self, market_id: str) -> dict[str, Any]:
        raw = await self._client.get(self._market_key(market_id))
        return json.loads(raw) if raw else {}

    async def set_market_state(self, market_id: str, state: dict[str, Any]) -> None:
        await self._client.set(self._market_key(market_id), json.dumps(state, sort_keys=True))

    async def get_wallet_first_seen(self, wallet: str) -> str | None:
        return await self._client.get(self._wallet_key(wallet))

    async def set_wallet_first_seen(self, wallet: str, first_seen_at: str) -> None:
        await self._client.setnx(self._wallet_key(wallet), first_seen_at)

    async def get_last_candidate(self, market_id: str) -> dict[str, Any] | None:
        raw = await self._client.get(self._candidate_key(market_id))
        return json.loads(raw) if raw else None

    async def set_last_candidate(self, market_id: str, payload: dict[str, Any]) -> None:
        await self._client.set(self._candidate_key(market_id), json.dumps(payload, sort_keys=True))

    async def aclose(self) -> None:
        await self._client.aclose()


@dataclass(slots=True)
class StateStoreContext:
    store: BaseStateStore
    backend_name: str
    notes: str


async def create_state_store() -> StateStoreContext:
    desired_backend = (PHASE3_STATE_BACKEND or "redis").strip().lower()
    if desired_backend != "redis":
        return StateStoreContext(
            store=MemoryStateStore(),
            backend_name="memory",
            notes="Configured for in-memory Phase 3 state.",
        )

    if redis_async is None:
        log.warning("Phase 3 Redis backend requested but redis package is not installed; falling back to memory.")
        return StateStoreContext(
            store=MemoryStateStore(),
            backend_name="memory",
            notes="Redis package unavailable; using in-memory fallback.",
        )

    client = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:  # pragma: no cover - depends on local Redis runtime
        log.warning(f"Phase 3 Redis backend unavailable ({exc}); falling back to memory.")
        await client.aclose()
        return StateStoreContext(
            store=MemoryStateStore(),
            backend_name="memory",
            notes=f"Redis connection failed at {_utc_now_iso()}; using in-memory fallback.",
        )

    return StateStoreContext(
        store=RedisStateStore(client),
        backend_name="redis",
        notes=f"Connected to Redis at {REDIS_URL}.",
    )
