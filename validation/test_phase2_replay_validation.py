"""Focused tests for Phase 2 replay validation."""

from __future__ import annotations

from decimal import Decimal
import unittest

from validation.phase2_archive_reader import ArchiveManifestEntry, RawArchiveRecord
from validation.phase2_envelope_reconstruction import ReconstructionRejection
from validation.phase2_replay_engine import ReplayMetadata
from validation.phase2_replay_validation import _validate_replay_output


class Phase2ReplayValidationTests(unittest.TestCase):
    """Verify replay-vs-original validation and discrepancy classification."""

    def test_validate_replay_output_passes_when_counts_and_manifest_match(self) -> None:
        raw_records = [
            self._raw_record("raw-1", "file-a.ndjson"),
            self._raw_record("raw-2", "file-a.ndjson"),
        ]
        replayed_envelopes = [
            self._envelope("env-1", "raw-1"),
            self._envelope("env-2", "raw-2"),
        ]
        manifest = [
            self._manifest_entry("manifest-a", "file-a.ndjson", 2),
        ]
        replay_metadata = ReplayMetadata(
            total_records=2,
            duplicate_envelope_ids=[],
            ordering_validation_passed=True,
        )

        result = _validate_replay_output(
            replayed_envelopes,
            raw_records,
            manifest,
            replay_metadata=replay_metadata,
        )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["metrics"]["total_raw_records"], 2)
        self.assertEqual(result["metrics"]["total_replayed_envelopes"], 2)
        self.assertEqual(result["metrics"]["missing_envelopes_count"], 0)
        self.assertEqual(result["metrics"]["extra_envelopes_count"], 0)
        self.assertEqual(result["metrics"]["replay_duplicate_envelope_ids"], [])
        self.assertEqual(result["discrepancies"], [])
        self.assertTrue(result["summary"]["manifest_validation_passed"])

    def test_validate_replay_output_fails_for_missing_envelope(self) -> None:
        raw_records = [
            self._raw_record("raw-1", "file-a.ndjson"),
            self._raw_record("raw-2", "file-a.ndjson"),
        ]
        replayed_envelopes = [
            self._envelope("env-1", "raw-1"),
        ]
        manifest = [
            self._manifest_entry("manifest-a", "file-a.ndjson", 2),
        ]

        result = _validate_replay_output(replayed_envelopes, raw_records, manifest)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["metrics"]["missing_envelopes_count"], 1)
        self.assertIn("raw-2", result["metrics"]["missing_envelope_raw_event_uuids"])
        self.assertIn("missing_envelope", {item["reason_code"] for item in result["discrepancies"]})

    def test_validate_replay_output_fails_for_duplicate_envelope_id(self) -> None:
        raw_records = [
            self._raw_record("raw-1", "file-a.ndjson"),
            self._raw_record("raw-2", "file-a.ndjson"),
        ]
        replayed_envelopes = [
            self._envelope("env-dup", "raw-1"),
            self._envelope("env-dup", "raw-2"),
        ]
        manifest = [
            self._manifest_entry("manifest-a", "file-a.ndjson", 2),
        ]
        replay_metadata = ReplayMetadata(
            total_records=2,
            duplicate_envelope_ids=["env-dup"],
            ordering_validation_passed=False,
        )

        result = _validate_replay_output(
            replayed_envelopes,
            raw_records,
            manifest,
            replay_metadata=replay_metadata,
        )

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["metrics"]["replay_duplicate_envelope_ids"], ["env-dup"])
        self.assertIn("duplicate_envelope_id", {item["reason_code"] for item in result["discrepancies"]})

    def test_validate_replay_output_fails_for_manifest_mismatch(self) -> None:
        raw_records = [
            self._raw_record("raw-1", "file-a.ndjson"),
        ]
        replayed_envelopes = [
            self._envelope("env-1", "raw-1"),
        ]
        manifest = [
            self._manifest_entry("manifest-a", "file-a.ndjson", 2),
            self._manifest_entry("manifest-b", "file-b.ndjson", 1),
        ]

        result = _validate_replay_output(replayed_envelopes, raw_records, manifest)

        self.assertEqual(result["status"], "fail")
        self.assertGreater(result["metrics"]["manifest_missing_files_count"], 0)
        self.assertGreater(result["metrics"]["manifest_incomplete_partitions_count"], 0)
        self.assertIn("manifest_mismatch", {item["reason_code"] for item in result["discrepancies"]})

    def test_validate_replay_output_warns_for_reconstruction_failures_without_silent_drop(self) -> None:
        raw_records = [
            self._raw_record("raw-1", "file-a.ndjson"),
        ]
        replayed_envelopes = []
        manifest = [
            self._manifest_entry("manifest-a", "file-a.ndjson", 1),
        ]
        reconstruction_rejections = [
            ReconstructionRejection(
                reason_code="missing_required_field",
                message="missing_required_field: market_id",
                raw_event_uuid="raw-1",
                manifest_id="manifest-a",
                archive_uri="file-a.ndjson",
                record_index=0,
                source_system="data_api",
                source_endpoint="/trades",
            )
        ]

        result = _validate_replay_output(
            replayed_envelopes,
            raw_records,
            manifest,
            reconstruction_rejections=reconstruction_rejections,
        )

        self.assertEqual(result["status"], "warn")
        self.assertEqual(result["metrics"]["reconstruction_failure_count"], 1)
        self.assertEqual(result["metrics"]["missing_envelopes_count"], 0)
        self.assertIn("reconstruction_failure", {item["reason_code"] for item in result["discrepancies"]})

    def _raw_record(self, raw_event_uuid: str, archive_uri: str) -> RawArchiveRecord:
        """Build one raw-record fixture."""

        return RawArchiveRecord(
            raw_event_uuid=raw_event_uuid,
            payload_json={"payload": raw_event_uuid},
            event_time="2026-04-11T14:00:00.000000Z",
            ingest_time="2026-04-11T14:00:01.000000Z",
            event_time_source="provider",
            source_system="data_api",
            source_endpoint="/trades",
            manifest_id="manifest-a",
            archive_uri=archive_uri,
            payload_hash=f"sha256:{raw_event_uuid}",
            collector_source="phase2-test",
            collector_version="git:test",
            raw_schema_version="raw-envelope/v1",
            file_offset=0,
            line_number=1,
        )

    def _manifest_entry(self, manifest_id: str, archive_uri: str, row_count: int) -> ArchiveManifestEntry:
        """Build one manifest-entry fixture."""

        return ArchiveManifestEntry(
            manifest_id=manifest_id,
            archive_uri=archive_uri,
            file_format="ndjson",
            compression="none",
            source_system="data_api",
            source_endpoint="/trades",
            collector_source="phase2-test",
            collector_version="git:test",
            raw_schema_version="raw-envelope/v1",
            window_start="2026-04-11T14:00:00.000000Z",
            window_end="2026-04-11T15:00:00.000000Z",
            row_count=row_count,
            checksum=None,
        )

    def _envelope(self, envelope_id: str, raw_event_uuid: str) -> dict[str, object]:
        """Build one minimal replayed-envelope fixture."""

        return {
            "contract_version": "1.0",
            "envelope_id": envelope_id,
            "raw_event_uuid": raw_event_uuid,
            "record_index": 0,
            "record_type": "trade",
            "source_system": "data_api",
            "source_endpoint": "/trades",
            "collector_source": "phase2-test",
            "collector_version": "git:test",
            "raw_schema_version": "raw-envelope/v1",
            "manifest_id": "manifest-a",
            "archive_uri": "file-a.ndjson",
            "payload_hash": f"sha256:{raw_event_uuid}",
            "event_time": "2026-04-11T14:00:00.000000Z",
            "event_time_source": "provider",
            "ingest_time": "2026-04-11T14:00:01.000000Z",
            "target_table": "trades",
            "market_id": "market-1",
            "condition_id": "condition-1",
            "event_id": None,
            "asset_id": "asset-1",
            "wallet_id": None,
            "source_event_id": f"source-{raw_event_uuid}",
            "transaction_hash": None,
            "watermark_time": None,
            "trade_id": f"trade-{raw_event_uuid}",
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
            "source_priority": 3,
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
