"""Integration-style tests for the Phase 2 replay case study."""

from __future__ import annotations

import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path

from validation.phase2_case_study import run_phase2_replay_case_study


class Phase2CaseStudyTests(unittest.TestCase):
    """Verify the end-to-end Gate 2 proof pipeline is deterministic."""

    def test_run_phase2_replay_case_study_is_deterministic_across_runs(self) -> None:
        with self._workspace_tempdir() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "archive"
            manifest_path = root / "manifest.ndjson"
            archive_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=data_api/"
                "source_endpoint=trades/part-0001.ndjson"
            )

            self._write_ndjson(
                archive_root / archive_uri,
                [
                    {
                        "event_uuid": "raw-1",
                        "payload_json": {
                            "id": "trade-1",
                            "asset": "asset-1",
                            "conditionId": "condition-1",
                            "outcome": "YES",
                            "side": "BUY",
                            "price": "0.25",
                            "size": "4",
                        },
                        "event_time": "2026-04-10T14:05:00Z",
                        "ingest_time": "2026-04-10T14:05:01Z",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "payload_hash": "sha256:raw-1",
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "asset_id": "asset-1",
                    },
                    {
                        "event_uuid": "raw-2",
                        "payload_json": {
                            "id": "trade-2",
                            "asset": "asset-2",
                            "conditionId": "condition-1",
                            "outcome": "NO",
                            "side": "SELL",
                            "price": "0.75",
                            "size": "2",
                        },
                        "event_time": "2026-04-10T14:06:00Z",
                        "ingest_time": "2026-04-10T14:06:01Z",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "payload_hash": "sha256:raw-2",
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "asset_id": "asset-2",
                    },
                ],
            )
            self._write_manifest(
                manifest_path,
                [
                    {
                        "manifest_id": "manifest-a",
                        "archive_uri": archive_uri,
                        "file_format": "ndjson",
                        "compression": "none",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "collector_source": "trades_collector",
                        "collector_version": "git:test",
                        "raw_schema_version": "raw-envelope/v1",
                        "window_start": "2026-04-10T14:00:00.000000Z",
                        "window_end": "2026-04-10T15:00:00.000000Z",
                        "row_count": 2,
                    }
                ],
            )

            first_run = run_phase2_replay_case_study(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00.000000Z",
                end_time="2026-04-10T15:00:00.000000Z",
                sample_size=2,
            )
            second_run = run_phase2_replay_case_study(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00.000000Z",
                end_time="2026-04-10T15:00:00.000000Z",
                sample_size=2,
            )

            self.assertEqual(first_run, second_run)
            self.assertEqual(first_run["validation_report"]["status"], "pass")
            self.assertEqual(first_run["input_summary"]["total_raw_records"], 2)
            self.assertEqual(first_run["replay_summary"]["total_reconstructed_envelopes"], 2)
            self.assertEqual(first_run["replay_summary"]["replay_metadata"]["ordering_validation_passed"], True)
            self.assertEqual(len(first_run["sample_envelopes"]), 2)
            self.assertEqual(first_run["sample_discrepancies"], [])

    def test_run_phase2_replay_case_study_can_write_json_output(self) -> None:
        with self._workspace_tempdir() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "archive"
            manifest_path = root / "manifest.ndjson"
            output_path = root / "outputs" / "case-study.json"
            archive_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=data_api/"
                "source_endpoint=trades/part-0001.ndjson"
            )

            self._write_ndjson(
                archive_root / archive_uri,
                [
                    {
                        "event_uuid": "raw-1",
                        "payload_json": {
                            "id": "trade-1",
                            "asset": "asset-1",
                            "conditionId": "condition-1",
                            "outcome": "YES",
                            "side": "BUY",
                            "price": "0.25",
                            "size": "4",
                        },
                        "event_time": "2026-04-10T14:05:00Z",
                        "ingest_time": "2026-04-10T14:05:01Z",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "payload_hash": "sha256:raw-1",
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "asset_id": "asset-1",
                    }
                ],
            )
            self._write_manifest(
                manifest_path,
                [
                    {
                        "manifest_id": "manifest-a",
                        "archive_uri": archive_uri,
                        "file_format": "ndjson",
                        "compression": "none",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "collector_source": "trades_collector",
                        "collector_version": "git:test",
                        "raw_schema_version": "raw-envelope/v1",
                        "window_start": "2026-04-10T14:00:00.000000Z",
                        "window_end": "2026-04-10T15:00:00.000000Z",
                        "row_count": 1,
                    }
                ],
            )

            result = run_phase2_replay_case_study(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00.000000Z",
                end_time="2026-04-10T15:00:00.000000Z",
                save_output=True,
                output_path=output_path,
            )

            self.assertTrue(output_path.exists())
            saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_payload, result)

    def _write_manifest(self, path: Path, entries: list[dict[str, object]]) -> None:
        """Write one NDJSON manifest file for case-study tests."""

        path.write_text(
            "".join(json.dumps(entry) + "\n" for entry in entries),
            encoding="utf-8",
        )

    def _write_ndjson(self, path: Path, records: list[dict[str, object]]) -> None:
        """Write one NDJSON archive file for case-study tests."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    @contextmanager
    def _workspace_tempdir(self):
        """Create a deterministic workspace-local temporary directory."""

        workspace_tmp = Path.cwd() / ".tmp_phase2_case_study_tests"
        workspace_tmp.mkdir(parents=True, exist_ok=True)
        safe_name = f"case_{uuid.uuid4().hex[:8]}"
        temp_dir = workspace_tmp / safe_name
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            yield str(temp_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
