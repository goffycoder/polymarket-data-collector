from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

DATABASE_URL = os.getenv("POLYMARKET_DATABASE_URL", "").strip()
DB_BACKEND = os.getenv("POLYMARKET_DB_BACKEND", "postgres" if DATABASE_URL else "sqlite").strip().lower()
DB_PATH = os.getenv("POLYMARKET_SQLITE_PATH", str(REPO_ROOT / "database" / "polymarket_state.db"))
