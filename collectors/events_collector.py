"""
collectors/events_collector.py — Full Gamma events sync.

Paginates /events, captures ALL available fields, writes to the events table.
Marks events absent from the active feed as 'closed'.
Rate: ~0.2s sleep per page → ~5 req/s (well under 500 req/10s limit).
"""
import asyncio
import json
from datetime import datetime, timezone

from database.db_manager import get_conn
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("events_collector")

GAMMA_URL = "https://gamma-api.polymarket.com"
PAGE_SIZE = 100
PAGE_SLEEP = 0.2   # 5 req/s, limit is 50 req/s


def _parse_tags(tag_list: list) -> str:
    """Extract tag labels into a JSON string."""
    return json.dumps([t.get("label", "") for t in tag_list if t.get("label")])


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


async def sync_events() -> set:
    """
    Fetch all active events from Gamma, upsert into DB.
    Returns the set of active event_ids seen this cycle.
    """
    log.info("🌐 Events sync starting...")
    conn = get_conn()
    active_ids: set[str] = set()
    offset = 0
    total_upserted = 0

    async with make_client() as client:
        while True:
            url = f"{GAMMA_URL}/events"
            params = {
                "active": "true",
                "closed": "false",
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            data = await safe_get(client, url, params=params)

            if not data:
                break

            rows = data if isinstance(data, list) else []
            if not rows:
                break

            for event in rows:
                eid = str(event.get("id", ""))
                if not eid:
                    continue

                active_ids.add(eid)
                tag_str = _parse_tags(event.get("tags", []))

                conn.execute("""
                    INSERT INTO events (
                        event_id, title, description, slug, tags,
                        volume, volume_24hr, volume_1wk, volume_1mo,
                        liquidity, open_interest, comment_count, competitive,
                        start_date, end_date, creation_date,
                        neg_risk, featured, restricted,
                        status, last_updated_at
                    ) VALUES (
                        :event_id, :title, :description, :slug, :tags,
                        :volume, :volume_24hr, :volume_1wk, :volume_1mo,
                        :liquidity, :open_interest, :comment_count, :competitive,
                        :start_date, :end_date, :creation_date,
                        :neg_risk, :featured, :restricted,
                        'active', :last_updated_at
                    )
                    ON CONFLICT(event_id) DO UPDATE SET
                        title           = excluded.title,
                        description     = excluded.description,
                        tags            = excluded.tags,
                        volume          = excluded.volume,
                        volume_24hr     = excluded.volume_24hr,
                        volume_1wk      = excluded.volume_1wk,
                        volume_1mo      = excluded.volume_1mo,
                        liquidity       = excluded.liquidity,
                        open_interest   = excluded.open_interest,
                        comment_count   = excluded.comment_count,
                        competitive     = excluded.competitive,
                        end_date        = excluded.end_date,
                        neg_risk        = excluded.neg_risk,
                        status          = 'active',
                        last_updated_at = excluded.last_updated_at
                """, {
                    "event_id":     eid,
                    "title":        event.get("title"),
                    "description":  event.get("description"),
                    "slug":         event.get("slug") or event.get("ticker"),
                    "tags":         tag_str,
                    "volume":       _safe_float(event.get("volume")),
                    "volume_24hr":  _safe_float(event.get("volume24hr")),
                    "volume_1wk":   _safe_float(event.get("volume1wk")),
                    "volume_1mo":   _safe_float(event.get("volume1mo")),
                    "liquidity":    _safe_float(event.get("liquidity") or event.get("liquidityClob")),
                    "open_interest":_safe_float(event.get("openInterest")),
                    "comment_count":_safe_int(event.get("commentCount")),
                    "competitive":  _safe_float(event.get("competitive")),
                    "start_date":   event.get("startDate") or event.get("creationDate"),
                    "end_date":     event.get("endDate"),
                    "creation_date":event.get("creationDate"),
                    "neg_risk":     1 if event.get("negRisk") else 0,
                    "featured":     1 if event.get("featured") else 0,
                    "restricted":   1 if event.get("restricted") else 0,
                    "last_updated_at": datetime.now(timezone.utc).isoformat(),
                })
                total_upserted += 1

            conn.commit()
            log.debug(f"  Events offset={offset}: +{len(rows)} rows (total upserted={total_upserted})")
            offset += PAGE_SIZE

            if len(rows) < PAGE_SIZE:
                break  # Last page

            await asyncio.sleep(PAGE_SLEEP)

    # Mark events not seen this cycle as closed
    if active_ids:
        placeholders = ",".join("?" * len(active_ids))
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(f"""
            UPDATE events
            SET status = 'closed', closed_at = ?
            WHERE status = 'active'
              AND event_id NOT IN ({placeholders})
        """, [now] + list(active_ids))
        conn.commit()

    conn.close()
    log.info(f"✅ Events sync complete: {total_upserted} upserted, {len(active_ids)} active.")
    return active_ids
