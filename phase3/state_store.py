from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE3_STATE_BACKEND, PHASE3_STATE_SQLITE_PATH, REDIS_URL
from utils.logger import get_logger

log = get_logger("phase3_state_store")
REPO_ROOT = Path(__file__).resolve().parent.parent

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


class SQLiteStateStore(BaseStateStore):
    backend_name = "sqlite"

    def __init__(self, db_path: str | Path):
        self._db_path = self._resolve_path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._ensure_schema()

    @staticmethod
    def _resolve_path(db_path: str | Path) -> Path:
        path = Path(str(db_path))
        if not path.is_absolute():
            path = REPO_ROOT / path
        return path

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _ensure_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS phase3_market_state (
                market_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS phase3_wallet_first_seen (
                wallet TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS phase3_last_candidate (
                market_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    async def get_market_state(self, market_id: str) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT state_json FROM phase3_market_state WHERE market_id = ?",
            (market_id,),
        ).fetchone()
        return json.loads(row["state_json"]) if row else {}

    async def set_market_state(self, market_id: str, state: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO phase3_market_state (market_id, state_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (market_id, json.dumps(state, sort_keys=True), _utc_now_iso()),
        )
        self._conn.commit()

    async def get_wallet_first_seen(self, wallet: str) -> str | None:
        row = self._conn.execute(
            "SELECT first_seen_at FROM phase3_wallet_first_seen WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        return None if row is None else str(row["first_seen_at"])

    async def set_wallet_first_seen(self, wallet: str, first_seen_at: str) -> None:
        self._conn.execute(
            """
            INSERT INTO phase3_wallet_first_seen (wallet, first_seen_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(wallet) DO NOTHING
            """,
            (wallet, first_seen_at, _utc_now_iso()),
        )
        self._conn.commit()

    async def get_last_candidate(self, market_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT payload_json FROM phase3_last_candidate WHERE market_id = ?",
            (market_id,),
        ).fetchone()
        return json.loads(row["payload_json"]) if row else None

    async def set_last_candidate(self, market_id: str, payload: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO phase3_last_candidate (market_id, payload_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (market_id, json.dumps(payload, sort_keys=True), _utc_now_iso()),
        )
        self._conn.commit()

    async def aclose(self) -> None:
        self._conn.close()


@dataclass(slots=True)
class StateStoreContext:
    store: BaseStateStore
    backend_name: str
    notes: str


class Phase3StateStoreConfigurationError(RuntimeError):
    """Raised when the requested live Phase 3 state backend is unavailable."""


def _memory_context(notes: str) -> StateStoreContext:
    return StateStoreContext(
        store=MemoryStateStore(),
        backend_name="memory",
        notes=notes,
    )


def _sqlite_context(notes: str) -> StateStoreContext:
    store = SQLiteStateStore(PHASE3_STATE_SQLITE_PATH)
    return StateStoreContext(
        store=store,
        backend_name="sqlite",
        notes=f"{notes} SQLite state path={store.db_path}.",
    )


async def create_state_store(
    *,
    require_backend: str | None = None,
    allow_fallback: bool = True,
) -> StateStoreContext:
    desired_backend = (PHASE3_STATE_BACKEND or "redis").strip().lower()
    required_backend = (require_backend or "").strip().lower() or None
    if required_backend == "durable":
        if desired_backend not in {"redis", "sqlite"}:
            raise Phase3StateStoreConfigurationError(
                "Phase 3 live runtime requires a durable backend ('redis' or 'sqlite'), but "
                f"POLYMARKET_PHASE3_STATE_BACKEND is '{desired_backend}'."
            )
    elif required_backend and desired_backend != required_backend:
        raise Phase3StateStoreConfigurationError(
            f"Phase 3 live runtime requires backend '{required_backend}', but "
            f"POLYMARKET_PHASE3_STATE_BACKEND is '{desired_backend}'."
        )

    if desired_backend == "sqlite":
        return _sqlite_context("Configured for durable SQLite Phase 3 state.")

    if desired_backend != "redis":
        return _memory_context("Configured for in-memory Phase 3 state.")

    if redis_async is None:
        message = "Phase 3 Redis backend requested but redis package is not installed."
        if not allow_fallback:
            raise Phase3StateStoreConfigurationError(message)
        log.warning(f"{message} Falling back to memory.")
        return _memory_context("Redis package unavailable; using in-memory fallback.")

    client = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:  # pragma: no cover - depends on local Redis runtime
        message = f"Phase 3 Redis backend unavailable ({exc})."
        await client.aclose()
        if not allow_fallback:
            raise Phase3StateStoreConfigurationError(message)
        log.warning(f"{message} Falling back to memory.")
        return _memory_context(
            f"Redis connection failed at {_utc_now_iso()}; using in-memory fallback."
        )

    return StateStoreContext(
        store=RedisStateStore(client),
        backend_name="redis",
        notes=f"Connected to Redis at {REDIS_URL}.",
    )
