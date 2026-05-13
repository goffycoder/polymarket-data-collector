from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_ENV_PATH = REPO_ROOT / ".env.runtime"
LEGACY_RUNTIME_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_RUNTIME_SECRET_ENV_PATH = REPO_ROOT / ".env.runtime.secrets"
LEGACY_RUNTIME_SECRET_ENV_PATH = REPO_ROOT / ".env.secrets"
SECRET_KEY_NAMES = {
    "POLYMARKET_DATABASE_URL",
    "POLYMARKET_PHASE4_TELEGRAM_BOT_TOKEN",
    "POLYMARKET_PHASE4_TELEGRAM_CHAT_ID",
    "POLYMARKET_PHASE4_DISCORD_WEBHOOK_URL",
}


@dataclass(frozen=True, slots=True)
class RuntimeEnvLoadResult:
    env_file: Path | None
    loaded: bool
    source: str
    secret_env_file: Path | None
    secret_loaded: bool
    secret_source: str
    warnings: tuple[str, ...]
    secret_keys_in_primary_env: tuple[str, ...]
    secret_keys_in_secret_env: tuple[str, ...]


def resolve_runtime_env_file(explicit_path: str | os.PathLike[str] | None = None) -> Path | None:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    configured = os.getenv("POLYMARKET_ENV_FILE", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    if DEFAULT_RUNTIME_ENV_PATH.exists():
        return DEFAULT_RUNTIME_ENV_PATH
    if LEGACY_RUNTIME_ENV_PATH.exists():
        return LEGACY_RUNTIME_ENV_PATH
    return None


def resolve_runtime_secret_env_file(
    explicit_path: str | os.PathLike[str] | None = None,
) -> Path | None:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    configured = os.getenv("POLYMARKET_SECRET_ENV_FILE", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    if DEFAULT_RUNTIME_SECRET_ENV_PATH.exists():
        return DEFAULT_RUNTIME_SECRET_ENV_PATH
    if LEGACY_RUNTIME_SECRET_ENV_PATH.exists():
        return LEGACY_RUNTIME_SECRET_ENV_PATH
    return None


def _dotenv_key_values(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    values = dotenv_values(path)
    return {
        str(key): str(value).strip()
        for key, value in values.items()
        if key is not None and value is not None and str(value).strip()
    }


def _secret_keys_for_file(path: Path | None) -> tuple[str, ...]:
    values = _dotenv_key_values(path)
    return tuple(sorted(key for key in values if key in SECRET_KEY_NAMES))


def load_runtime_env(
    explicit_path: str | os.PathLike[str] | None = None,
    *,
    override: bool = True,
    secret_explicit_path: str | os.PathLike[str] | None = None,
) -> RuntimeEnvLoadResult:
    env_file = resolve_runtime_env_file(explicit_path)
    if env_file is None:
        env_file = None
        loaded = False
        source = "shell_only"
    else:
        if not env_file.exists():
            raise FileNotFoundError(f"Runtime env file not found: {env_file}")
        loaded = load_dotenv(env_file, override=override)
        if env_file.name == ".env.runtime":
            source = "env_runtime_file"
        elif env_file.name == ".env":
            source = "legacy_env_file"
        else:
            source = "explicit_env_file"

    secret_env_file = resolve_runtime_secret_env_file(secret_explicit_path)
    if secret_env_file is None:
        secret_loaded = False
        secret_source = "shell_or_keychain_only"
    else:
        if not secret_env_file.exists():
            raise FileNotFoundError(f"Runtime secret env file not found: {secret_env_file}")
        secret_loaded = load_dotenv(secret_env_file, override=override)
        if secret_env_file.name == ".env.runtime.secrets":
            secret_source = "runtime_secret_file"
        elif secret_env_file.name == ".env.secrets":
            secret_source = "legacy_secret_file"
        else:
            secret_source = "explicit_secret_file"

    secret_keys_in_primary_env = _secret_keys_for_file(env_file)
    secret_keys_in_secret_env = _secret_keys_for_file(secret_env_file)
    warnings: list[str] = []
    if source == "legacy_env_file":
        warnings.append(
            "Legacy .env fallback is still in use. Canonical runtime should prefer .env.runtime plus shell or .env.runtime.secrets."
        )
    if secret_keys_in_primary_env:
        warnings.append(
            "Primary runtime env file contains secret-like keys. Move them to OS environment variables, a keychain-backed loader, or .env.runtime.secrets."
        )
    if secret_source == "legacy_secret_file":
        warnings.append(
            "Legacy .env.secrets fallback is in use. Canonical runtime should prefer .env.runtime.secrets."
        )

    return RuntimeEnvLoadResult(
        env_file=env_file,
        loaded=loaded,
        source=source,
        secret_env_file=secret_env_file,
        secret_loaded=secret_loaded,
        secret_source=secret_source,
        warnings=tuple(warnings),
        secret_keys_in_primary_env=secret_keys_in_primary_env,
        secret_keys_in_secret_env=secret_keys_in_secret_env,
    )
