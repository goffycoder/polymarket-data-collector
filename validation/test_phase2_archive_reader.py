"""Focused tests for the Phase 2 archive reader."""

from __future__ import annotations

import gzip
import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path

from validation.phase2_archive_reader import _load_archive_window


class Phase2ArchiveReaderTests(unittest.TestCase):
    """Verify deterministic loading, metadata attachment, and explicit rejects."""

    def test_load_archive_window_streams_in_archive_uri_then_offset_order(self) -> None:
        with self._workspace_tempdir() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "archive"
            archive_root.mkdir()
            manifest_path = root / "manifest.ndjson"

            second_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=ws_market/"
                "source_endpoint=market_last_trade_price/part-0002.ndjson.gz"
            )
            first_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=data_api/"
                "source_endpoint=trades/part-0001.ndjson"
            )

            first_records = [
                {
                    "event_uuid": "evt-2",
                    "payload_json": {"trade": 2},
                    "event_time": "2026-04-10T14:05:02Z",
                    "ingest_time": "2026-04-10T14:05:03Z",
                    "source_system": "data_api",
                    "source_endpoint": "/trades",
                    "payload_hash": "sha256:two",
                    "collector_source": "trades_collector",
                    "collector_version": "git:test",
                    "schema_version": "raw-envelope/v1",
                },
                {
                    "event_uuid": "evt-3",
                    "payload_json": {"trade": 3},
                    "event_time": "2026-04-10T14:05:04Z",
                    "ingest_time": "2026-04-10T14:05:05Z",
                    "source_system": "data_api",
                    "source_endpoint": "/trades",
                    "payload_hash": "sha256:three",
                    "collector_source": "trades_collector",
                    "collector_version": "git:test",
                    "schema_version": "raw-envelope/v1",
                },
            ]
            second_records = [
                {
                    "event_uuid": "evt-1",
                    "payload_json": {"trade": 1},
                    "event_time": "2026-04-10T14:01:01Z",
                    "ingest_time": "2026-04-10T14:01:02Z",
                    "source_system": "ws_market",
                    "source_endpoint": "market:last_trade_price",
                    "payload_hash": "sha256:one",
                    "collector_source": "ws_listener",
                    "collector_version": "git:test",
                    "schema_version": "raw-envelope/v1",
                }
            ]

            self._write_ndjson(archive_root / first_uri, first_records)
            self._write_ndjson_gz(archive_root / second_uri, second_records)
            self._write_manifest(
                manifest_path,
                [
                    {
                        "manifest_id": "manifest-b",
                        "archive_uri": second_uri,
                        "file_format": "ndjson",
                        "compression": "gzip",
                        "source_system": "ws_market",
                        "source_endpoint": "market:last_trade_price",
                        "collector_source": "ws_listener",
                        "collector_version": "git:test",
                        "raw_schema_version": "raw-envelope/v1",
                        "window_start": "2026-04-10T14:00:00Z",
                        "window_end": "2026-04-10T15:00:00Z",
                        "row_count": 1,
                        "checksum": "sha256:bbb",
                    },
                    {
                        "manifest_id": "manifest-a",
                        "archive_uri": first_uri,
                        "file_format": "ndjson",
                        "compression": "none",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "collector_source": "trades_collector",
                        "collector_version": "git:test",
                        "raw_schema_version": "raw-envelope/v1",
                        "window_start": "2026-04-10T14:00:00Z",
                        "window_end": "2026-04-10T15:00:00Z",
                        "row_count": 2,
                        "checksum": "sha256:aaa",
                    },
                ],
            )

            records_iter, rejected = _load_archive_window(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00Z",
                end_time="2026-04-10T15:00:00Z",
            )
            records = list(records_iter)

            self.assertEqual([record.archive_uri for record in records], [first_uri, first_uri, second_uri])
            self.assertEqual([record.raw_event_uuid for record in records], ["evt-2", "evt-3", "evt-1"])
            self.assertEqual(records[0].file_offset, 0)
            self.assertGreater(records[1].file_offset, records[0].file_offset)
            self.assertEqual(records[2].file_offset, 0)
            self.assertEqual(rejected, [])

    def test_load_archive_window_attaches_manifest_and_collector_metadata(self) -> None:
        with self._workspace_tempdir() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "archive"
            archive_root.mkdir()
            manifest_path = root / "manifest.ndjson"
            archive_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=data_api/"
                "source_endpoint=trades/part-0001.ndjson"
            )

            self._write_ndjson(
                archive_root / archive_uri,
                [
                    {
                        "event_uuid": "evt-10",
                        "payload_json": {"id": 10, "kind": "trade"},
                        "event_time": "2026-04-10T14:10:00Z",
                        "ingest_time": "2026-04-10T14:10:01Z",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "payload_hash": "sha256:ten",
                    }
                ],
            )
            self._write_manifest(
                manifest_path,
                [
                    {
                        "manifest_id": "manifest-10",
                        "archive_uri": archive_uri,
                        "file_format": "ndjson",
                        "compression": "none",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "collector_source": "trades_collector",
                        "collector_version": "git:manifest",
                        "raw_schema_version": "raw-envelope/v1",
                        "window_start": "2026-04-10T14:00:00Z",
                        "window_end": "2026-04-10T15:00:00Z",
                    }
                ],
            )

            records_iter, rejected = _load_archive_window(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00Z",
                end_time="2026-04-10T15:00:00Z",
                source_system="data_api",
                source_endpoint="/trades",
            )
            records = list(records_iter)

            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(record.manifest_id, "manifest-10")
            self.assertEqual(record.archive_uri, archive_uri)
            self.assertEqual(record.payload_hash, "sha256:ten")
            self.assertEqual(record.collector_source, "trades_collector")
            self.assertEqual(record.collector_version, "git:manifest")
            self.assertEqual(record.raw_schema_version, "raw-envelope/v1")
            self.assertEqual(record.event_time, "2026-04-10T14:10:00.000000Z")
            self.assertEqual(record.ingest_time, "2026-04-10T14:10:01.000000Z")
            self.assertEqual(rejected, [])

    def test_load_archive_window_emits_rejections_without_silent_drops(self) -> None:
        with self._workspace_tempdir() as tmpdir:
            root = Path(tmpdir)
            archive_root = root / "archive"
            archive_root.mkdir()
            manifest_path = root / "manifest.ndjson"
            archive_uri = (
                "year=2026/month=04/day=10/hour=14/source_system=data_api/"
                "source_endpoint=trades/part-0001.ndjson"
            )

            archive_path = archive_root / archive_uri
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "event_uuid": "evt-good",
                                "payload_json": {"ok": True},
                                "event_time": "2026-04-10T14:11:00Z",
                                "ingest_time": "2026-04-10T14:11:01Z",
                                "source_system": "data_api",
                                "source_endpoint": "/trades",
                                "payload_hash": "sha256:good",
                            }
                        ),
                        "{not-json}",
                        json.dumps(
                            {
                                "event_uuid": "evt-missing-hash",
                                "payload_json": {"ok": False},
                                "event_time": "2026-04-10T14:12:00Z",
                                "ingest_time": "2026-04-10T14:12:01Z",
                                "source_system": "data_api",
                                "source_endpoint": "/trades",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            self._write_manifest(
                manifest_path,
                [
                    {
                        "manifest_id": "manifest-rejects",
                        "archive_uri": archive_uri,
                        "file_format": "ndjson",
                        "compression": "none",
                        "source_system": "data_api",
                        "source_endpoint": "/trades",
                        "window_start": "2026-04-10T14:00:00Z",
                        "window_end": "2026-04-10T15:00:00Z",
                    }
                ],
            )

            records_iter, rejected = _load_archive_window(
                archive_root,
                manifest_path,
                start_time="2026-04-10T14:00:00Z",
                end_time="2026-04-10T15:00:00Z",
            )
            records = list(records_iter)

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].raw_event_uuid, "evt-good")
            self.assertEqual(len(rejected), 2)
            self.assertEqual({item.reason_code for item in rejected}, {"invalid_raw_json", "missing_required_field"})
            self.assertEqual(len(records) + len(rejected), 3)

    def _write_manifest(self, path: Path, entries: list[dict[str, object]]) -> None:
        """Write one NDJSON manifest file for tests."""

        path.write_text(
            "".join(json.dumps(entry) + "\n" for entry in entries),
            encoding="utf-8",
        )

    def _write_ndjson(self, path: Path, records: list[dict[str, object]]) -> None:
        """Write one plain NDJSON archive file for tests."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    def _write_ndjson_gz(self, path: Path, records: list[dict[str, object]]) -> None:
        """Write one gzipped NDJSON archive file for tests."""

        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, mode="wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record))
                handle.write("\n")

    @contextmanager
    def _workspace_tempdir(self):
        """Create a temporary directory inside the writable workspace."""

        workspace_tmp = Path.cwd() / ".tmp_phase2_archive_reader_tests"
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
