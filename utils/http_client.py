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


def make_client(timeout: float = 15.0) -> httpx.AsyncClient:
    """Create a shared async client with sensible defaults."""
    return httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )


async def safe_get(
    client: httpx.AsyncClient,
    url: str,
    retries: int = 3,
    base_delay: float = 1.0,
    params: dict = None,
) -> dict | list | None:
    """
    GET with retry/backoff. Returns parsed JSON or None.
    - 429 → wait 30s * attempt, retry
    - 5xx → exponential backoff
    - Timeout → exponential backoff
    """
    for attempt in range(retries):
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
                wait = 30 * (attempt + 1)
                log.warning(f"429 Rate limited on {url}. Waiting {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = base_delay * (2 ** attempt)
                log.warning(f"HTTP {resp.status_code} on {url}. Retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue

            # 4xx (non-429) — don't retry
            log.debug(f"HTTP {resp.status_code} on {url} — skipping")
            return None

        except httpx.TimeoutException:
            wait = base_delay * (2 ** attempt)
            log.warning(f"Timeout on {url}. Retrying in {wait:.1f}s (attempt {attempt+1})")
            await asyncio.sleep(wait)

        except (UnicodeDecodeError, Exception) as e:
            wait = base_delay * (2 ** attempt)
            log.warning(f"Error on GET {url}: {e}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)

    log.error(f"All {retries} retries failed for GET {url}")
    return None


async def safe_post(
    client: httpx.AsyncClient,
    url: str,
    json_body: dict | list,
    retries: int = 3,
    base_delay: float = 1.0,
) -> dict | list | None:
    """
    POST with retry/backoff. Returns parsed JSON or None.
    """
    for attempt in range(retries):
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
                wait = 30 * (attempt + 1)
                log.warning(f"429 Rate limited on POST {url}. Waiting {wait}s")
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = base_delay * (2 ** attempt)
                log.warning(f"HTTP {resp.status_code} on POST {url}. Retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue

            log.debug(f"HTTP {resp.status_code} on POST {url} — skipping")
            return None

        except httpx.TimeoutException:
            wait = base_delay * (2 ** attempt)
            log.warning(f"Timeout on POST {url}. Retrying in {wait:.1f}s")
            await asyncio.sleep(wait)

        except Exception as e:
            log.error(f"Unexpected error on POST {url}: {e}")
            return None

    log.error(f"All {retries} retries failed for POST {url}")
    return None
