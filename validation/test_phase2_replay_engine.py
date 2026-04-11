"""Focused tests for the Phase 2 replay engine."""

from __future__ import annotations

from decimal import Decimal
import random
import unittest

from validation.phase2_replay_engine import (
    _apply_replay_ordering,
    _replay_envelopes,
    _sort_envelopes_deterministically,
    _validate_replay_order,
)


class Phase2ReplayEngineTests(unittest.TestCase):
    """Verify deterministic contract-order replay behavior."""

    def test_replay_sorts_shuffled_input_by_contract_tuple(self) -> None:
        envelopes = [
            self._envelope(
                envelope_id="env-3",
                event_time="2026-04-10T14:00:02.000000Z",
                ingest_time="2026-04-10T14:00:02.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-c",
                record_index=0,
            ),
            self._envelope(
                envelope_id="env-1",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:04.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-a",
                record_index=0,
            ),
            self._envelope(
                envelope_id="env-2",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:03.000000Z",
                source_priority=1,
                source_endpoint="market:last_trade_price",
                raw_event_uuid="raw-b",
                record_index=2,
            ),
        ]
        random.Random(7).shuffle(envelopes)

        replayed, metadata = _replay_envelopes(envelopes)

        self.assertEqual([envelope["envelope_id"] for envelope in replayed], ["env-2", "env-1", "env-3"])
        self.assertEqual(metadata.total_records, 3)
        self.assertEqual(metadata.duplicate_envelope_ids, [])
        self.assertTrue(metadata.ordering_validation_passed)

    def test_replay_order_is_deterministic_across_runs(self) -> None:
        envelopes = [
            self._envelope(
                envelope_id="env-a",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:01.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-z",
                record_index=1,
            ),
            self._envelope(
                envelope_id="env-b",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:01.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-z",
                record_index=0,
            ),
            self._envelope(
                envelope_id="env-c",
                event_time="2026-04-10T13:59:59.000000Z",
                ingest_time="2026-04-10T14:00:05.000000Z",
                source_priority=4,
                source_endpoint="/markets",
                raw_event_uuid="raw-y",
                record_index=0,
            ),
        ]

        first_run = _sort_envelopes_deterministically(envelopes)
        second_run = _apply_replay_ordering(list(reversed(envelopes)))

        self.assertEqual(
            [envelope["envelope_id"] for envelope in first_run],
            [envelope["envelope_id"] for envelope in second_run],
        )
        self.assertTrue(_validate_replay_order(first_run, original_count=3, duplicate_envelope_ids=[]))
        self.assertTrue(_validate_replay_order(second_run, original_count=3, duplicate_envelope_ids=[]))

    def test_replay_reports_duplicate_envelope_ids_without_dropping_records(self) -> None:
        envelopes = [
            self._envelope(
                envelope_id="env-dup",
                event_time="2026-04-10T14:00:00.000000Z",
                ingest_time="2026-04-10T14:00:00.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-1",
                record_index=0,
            ),
            self._envelope(
                envelope_id="env-dup",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:01.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-2",
                record_index=0,
            ),
        ]

        replayed, metadata = _replay_envelopes(envelopes)

        self.assertEqual(len(replayed), 2)
        self.assertEqual(metadata.total_records, 2)
        self.assertEqual(metadata.duplicate_envelope_ids, ["env-dup"])
        self.assertFalse(metadata.ordering_validation_passed)

    def test_replay_validation_fails_when_order_is_not_sorted(self) -> None:
        unsorted_envelopes = [
            self._envelope(
                envelope_id="env-late",
                event_time="2026-04-10T14:00:02.000000Z",
                ingest_time="2026-04-10T14:00:02.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-b",
                record_index=0,
            ),
            self._envelope(
                envelope_id="env-early",
                event_time="2026-04-10T14:00:01.000000Z",
                ingest_time="2026-04-10T14:00:01.000000Z",
                source_priority=3,
                source_endpoint="/trades",
                raw_event_uuid="raw-a",
                record_index=0,
            ),
        ]

        self.assertFalse(_validate_replay_order(unsorted_envelopes, original_count=2, duplicate_envelope_ids=[]))

    def _envelope(
        self,
        *,
        envelope_id: str,
        event_time: str,
        ingest_time: str,
        source_priority: int,
        source_endpoint: str,
        raw_event_uuid: str,
        record_index: int,
    ) -> dict[str, object]:
        """Build one minimal contract-compliant envelope fixture."""

        return {
            "contract_version": "1.0",
            "envelope_id": envelope_id,
            "raw_event_uuid": raw_event_uuid,
            "record_index": record_index,
            "record_type": "trade",
            "source_system": "data_api",
            "source_endpoint": source_endpoint,
            "collector_source": "phase2-test",
            "collector_version": "git:test",
            "raw_schema_version": "raw-envelope/v1",
            "manifest_id": "manifest-1",
            "archive_uri": "archive/part-0001.ndjson",
            "payload_hash": "sha256:test",
            "event_time": event_time,
            "event_time_source": "provider",
            "ingest_time": ingest_time,
            "target_table": "trades",
            "market_id": "market-1",
            "condition_id": "condition-1",
            "event_id": None,
            "asset_id": "asset-1",
            "wallet_id": None,
            "source_event_id": "source-1",
            "transaction_hash": None,
            "watermark_time": None,
            "trade_id": "trade-1",
            "price": Decimal("0.500000000000"),
            "size": Decimal("1.000000000000"),
            "side": "BUY",
            "yes_price": None,
            "no_price": None,
            "best_bid": None,
            "best_ask": None,
            "last_trade_price": None,
            "bids_json": None,
            "asks_json": None,
            "final_price": None,
            "outcome_side": "YES",
            "usdc_notional": Decimal("0.500000000000"),
            "dedupe_key": f"dedupe-{envelope_id}",
            "source_priority": source_priority,
            "spread": None,
            "mid_price": None,
            "depth_bids": None,
            "depth_asks": None,
            "bid_volume": None,
            "ask_volume": None,
            "resolution_outcome": None,
        }


if __name__ == "__main__":
    unittest.main()
