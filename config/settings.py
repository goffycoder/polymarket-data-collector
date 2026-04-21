from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

DATABASE_URL = os.getenv("POLYMARKET_DATABASE_URL", "").strip()
DB_BACKEND = os.getenv("POLYMARKET_DB_BACKEND", "postgres" if DATABASE_URL else "sqlite").strip().lower()
DB_PATH = os.getenv("POLYMARKET_SQLITE_PATH", str(REPO_ROOT / "database" / "polymarket_state.db"))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


REDIS_URL = os.getenv("POLYMARKET_REDIS_URL", "redis://localhost:6379/0").strip()
PHASE3_STATE_BACKEND = os.getenv("POLYMARKET_PHASE3_STATE_BACKEND", "redis").strip().lower()
PHASE3_FEATURE_SCHEMA_VERSION = os.getenv(
    "POLYMARKET_PHASE3_FEATURE_SCHEMA_VERSION",
    "phase3_v1",
).strip()
PHASE3_DETECTOR_VERSION = os.getenv(
    "POLYMARKET_PHASE3_DETECTOR_VERSION",
    "phase3_detector_v1",
).strip()
PHASE3_WINDOW_SECONDS = _env_int("POLYMARKET_PHASE3_WINDOW_SECONDS", 300)
PHASE3_HISTORY_SECONDS = _env_int("POLYMARKET_PHASE3_HISTORY_SECONDS", 600)
PHASE3_COOLDOWN_SECONDS = _env_int("POLYMARKET_PHASE3_COOLDOWN_SECONDS", 900)
PHASE3_MIN_FRESH_WALLET_COUNT = _env_int("POLYMARKET_PHASE3_MIN_FRESH_WALLET_COUNT", 3)
PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE = _env_float(
    "POLYMARKET_PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE",
    0.35,
)
PHASE3_MIN_DIRECTIONAL_IMBALANCE = _env_float(
    "POLYMARKET_PHASE3_MIN_DIRECTIONAL_IMBALANCE",
    0.65,
)
PHASE3_MIN_CONCENTRATION_RATIO = _env_float(
    "POLYMARKET_PHASE3_MIN_CONCENTRATION_RATIO",
    0.45,
)
PHASE3_MIN_PROBABILITY_VELOCITY = _env_float(
    "POLYMARKET_PHASE3_MIN_PROBABILITY_VELOCITY",
    0.02,
)
PHASE3_MIN_PROBABILITY_ACCELERATION = _env_float(
    "POLYMARKET_PHASE3_MIN_PROBABILITY_ACCELERATION",
    0.005,
)
PHASE3_MIN_VOLUME_ACCELERATION = _env_float(
    "POLYMARKET_PHASE3_MIN_VOLUME_ACCELERATION",
    1.25,
)
PHASE3_MIN_WINDOW_NOTIONAL = _env_float(
    "POLYMARKET_PHASE3_MIN_WINDOW_NOTIONAL",
    250.0,
)
PHASE3_POLL_SECONDS = _env_float("POLYMARKET_PHASE3_POLL_SECONDS", 5.0)
