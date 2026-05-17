"""
utils/http_client.py — Shared async httpx client with retry, backoff, and 429 handling.

Usage:
    from utils.http_client import safe_get, safe_post

    data = await safe_get(client, url)
    data = await safe_post(client, url, json_body)

Returns None on all failure modes (caller checks and logs).
"""
import asyncio
import gzip
import json as _json
import httpx
from config.settings import (
    HTTP_429_BASE_DELAY_SECONDS,
    HTTP_BASE_DELAY_SECONDS,
    HTTP_MAX_CONNECTIONS,
    HTTP_MAX_KEEPALIVE_CONNECTIONS,
    HTTP_RETRIES,
    HTTP_TIMEOUT_SECONDS,
)
from utils.logger import get_logger

log = get_logger("http_client")

# Browser-like headers to avoid Cloudflare bot detection.
# NOTE: Do NOT set Accept-Encoding here — httpx automatically negotiates
# compression and decompresses responses only when it controls that header.
# Setting it manually causes the server to send compressed bytes that httpx
# then treats as raw UTF-8, producing "codec can't decode byte 0x85" errors.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://polymarket.com/",
    "Origin": "https://polymarket.com",
}


def _error_label(exc: BaseException) -> str:
    message = str(exc).strip()
    if message:
        return f"{type(exc).__name__}: {message}"
    return repr(exc)


def make_client(timeout: float | None = None) -> httpx.AsyncClient:
    """Create a shared async client with sensible defaults."""
    return httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        timeout=HTTP_TIMEOUT_SECONDS if timeout is None else timeout,
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=max(1, int(HTTP_MAX_CONNECTIONS)),
            max_keepalive_connections=max(0, int(HTTP_MAX_KEEPALIVE_CONNECTIONS)),
        ),
    )


async def safe_get(
    client: httpx.AsyncClient,
    url: str,
    retries: int | None = None,
    base_delay: float | None = None,
    params: dict = None,
) -> dict | list | None:
    """
    GET with retry/backoff. Returns parsed JSON or None.
    - 429 → wait 30s * attempt, retry
    - 5xx → exponential backoff
    - Timeout → exponential backoff
    """
    retry_count = max(1, int(HTTP_RETRIES if retries is None else retries))
    delay_base = float(HTTP_BASE_DELAY_SECONDS if base_delay is None else base_delay)
    for attempt in range(retry_count):
        try:
            resp = await client.get(url, params=params)

            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    # Fallback: try manual gzip decompress if httpx didn't
                    try:
                        raw = gzip.decompress(resp.content)
                        return _json.loads(raw.decode("utf-8"))
                    except Exception:
                        log.error(f"Failed to decode response from {url}: first bytes={resp.content[:20]}")
                        return None

            if resp.status_code == 429:
                wait = float(HTTP_429_BASE_DELAY_SECONDS) * (attempt + 1)
                log.warning(f"429 Rate limited on {url}. Waiting {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = delay_base * (2 ** attempt)
                log.warning(f"HTTP {resp.status_code} on {url}. Retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue

            # 4xx (non-429) — don't retry
            log.debug(f"HTTP {resp.status_code} on {url} — skipping")
            return None

        except httpx.TimeoutException:
            wait = delay_base * (2 ** attempt)
            log.warning(f"Timeout on {url}. Retrying in {wait:.1f}s (attempt {attempt+1})")
            await asyncio.sleep(wait)

        except Exception as exc:
            wait = delay_base * (2 ** attempt)
            log.warning(f"Error on GET {url}: {_error_label(exc)}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)

    log.error(f"All {retry_count} retries failed for GET {url}")
    return None


async def safe_post(
    client: httpx.AsyncClient,
    url: str,
    json_body: dict | list,
    retries: int | None = None,
    base_delay: float | None = None,
) -> dict | list | None:
    """
    POST with retry/backoff. Returns parsed JSON or None.
    """
    retry_count = max(1, int(HTTP_RETRIES if retries is None else retries))
    delay_base = float(HTTP_BASE_DELAY_SECONDS if base_delay is None else base_delay)
    for attempt in range(retry_count):
        try:
            resp = await client.post(url, json=json_body)

            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    try:
                        raw = gzip.decompress(resp.content)
                        return _json.loads(raw.decode("utf-8"))
                    except Exception:
                        log.error(f"Failed to decode POST response from {url}")
                        return None

            if resp.status_code == 429:
                wait = float(HTTP_429_BASE_DELAY_SECONDS) * (attempt + 1)
                log.warning(f"429 Rate limited on POST {url}. Waiting {wait}s")
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = delay_base * (2 ** attempt)
                log.warning(f"HTTP {resp.status_code} on POST {url}. Retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue

            log.debug(f"HTTP {resp.status_code} on POST {url} — skipping")
            return None

        except httpx.TimeoutException:
            wait = delay_base * (2 ** attempt)
            log.warning(f"Timeout on POST {url}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)

        except Exception as exc:
            wait = delay_base * (2 ** attempt)
            log.warning(f"Error on POST {url}: {_error_label(exc)}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)

    log.error(f"All {retry_count} retries failed for POST {url}")
    return None
