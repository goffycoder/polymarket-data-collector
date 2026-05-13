from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.settings import PHASE3_CHECKPOINT_INTERVAL, PHASE3_POLL_SECONDS
from phase3.detector import DEFAULT_PHASE3_SOURCE_SYSTEMS, Phase3Detector, Phase3Repository
from phase3.state_store import BaseStateStore
from utils.event_log import DETECTOR_INPUT_ROOT
from utils.logger import get_logger

log = get_logger("phase3_live_runner")


def _hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _partition_file(source_system: str, dt: datetime) -> Path:
    return (
        DETECTOR_INPUT_ROOT
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


@dataclass(slots=True)
class LiveRunnerSummary:
    processed_envelopes: int = 0
    partitions_scanned: int = 0
    idle_polls: int = 0
    checkpoint_writes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_envelopes": self.processed_envelopes,
            "partitions_scanned": self.partitions_scanned,
            "idle_polls": self.idle_polls,
            "checkpoint_writes": self.checkpoint_writes,
        }


class Phase3LiveRunner:
    def __init__(
        self,
        *,
        store: BaseStateStore,
        repository: Phase3Repository,
        source_systems: list[str] | None = None,
        poll_seconds: float = PHASE3_POLL_SECONDS,
    ):
        self.store = store
        self.repository = repository
        self.source_systems = source_systems or list(DEFAULT_PHASE3_SOURCE_SYSTEMS)
        self.poll_seconds = poll_seconds
        self.detector = Phase3Detector(store=store, repository=repository)
        self.summary = LiveRunnerSummary()

    async def run_forever(self) -> None:
        while True:
            processed = await self.run_once()
            if processed == 0:
                self.summary.idle_polls += 1
            await asyncio.sleep(self.poll_seconds)

    async def run_once(self) -> int:
        processed = 0
        now = datetime.now(timezone.utc)
        partition_hours = [_hour_floor(now - timedelta(hours=1)), _hour_floor(now)]

        for source_system in self.source_systems:
            for hour in partition_hours:
                partition_file = _partition_file(source_system, hour)
                if not partition_file.exists():
                    continue
                self.summary.partitions_scanned += 1
                processed += await self._process_partition(source_system, partition_file)

        self.summary.processed_envelopes += processed
        return processed

    async def _process_partition(self, source_system: str, partition_file: Path) -> int:
        relative_path = partition_file.relative_to(DETECTOR_INPUT_ROOT.parent).as_posix()
        checkpoint = self.repository.get_checkpoint(
            source_system=source_system,
            partition_path=relative_path,
        )
        start_offset = int((checkpoint or {}).get("file_offset") or 0)
        processed = 0
        last_ordering_key = (checkpoint or {}).get("last_ordering_key")
        last_captured_at = (checkpoint or {}).get("last_captured_at")
        checkpoint_interval = max(1, PHASE3_CHECKPOINT_INTERVAL)
        processed_since_checkpoint = 0
        final_offset = start_offset

        def persist_checkpoint(offset: int) -> None:
            self.repository.upsert_checkpoint(
                source_system=source_system,
                partition_path=relative_path,
                file_offset=offset,
                last_ordering_key=last_ordering_key,
                last_captured_at=last_captured_at,
            )
            self.summary.checkpoint_writes += 1

        with partition_file.open("r", encoding="utf-8") as handle:
            handle.seek(start_offset)
            while True:
                position = handle.tell()
                line = handle.readline()
                if not line:
                    final_offset = position
                    break
                line_end = handle.tell()
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    if not line.endswith("\n"):
                        final_offset = position
                        break
                    log.warning(
                        f"Skipping malformed detector-input envelope in {relative_path} "
                        f"at byte {position}: {exc}"
                    )
                    final_offset = line_end
                    continue
                try:
                    await self.detector.handle_envelope(payload)
                except Exception as exc:
                    log.error(
                        f"Phase 3 live runner failed while processing {relative_path} "
                        f"at byte {position}; checkpoint remains at byte {final_offset}: {exc}"
                    )
                    raise
                processed += 1
                processed_since_checkpoint += 1
                final_offset = line_end
                last_ordering_key = payload.get("ordering_key")
                last_captured_at = payload.get("captured_at")
                if processed_since_checkpoint >= checkpoint_interval:
                    persist_checkpoint(final_offset)
                    processed_since_checkpoint = 0

        if final_offset != start_offset or processed:
            persist_checkpoint(final_offset)

        if processed:
            log.info(
                f"Phase 3 live runner processed {processed} envelopes from {relative_path} "
                f"(checkpoint_offset={final_offset})"
            )
        return processed
