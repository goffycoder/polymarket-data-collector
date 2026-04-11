"""Focused tests for Phase 2 envelope reconstruction."""

from __future__ import annotations

from decimal import Decimal
import unittest

from validation.phase2_archive_reader import RawArchiveRecord
from validation.phase2_envelope_reconstruction import _reconstruct_envelopes


class Phase2EnvelopeReconstructionTests(unittest.TestCase):
    """Verify contract-shaped reconstruction from raw archive records."""

    def test_reconstruct_trade_record_populates_contract_fields(self) -> None:
        raw_record = self._raw_record(
            raw_event_uuid="raw-trade-1",
            source_system="data_api",
            source_endpoint="/trades",
            payload_json={
                "id": "206518773",
                "asset": "0xassetyes",
                "conditionId": "0xcondition",
                "proxyWallet": "0xABCDEF",
                "transactionHash": "0xHASHABC",
                "outcome": "yes",
                "side": "buy",
                "price": "0.63",
                "size": "150",
                "sizeUsdc": "94.5",
            },
            market_id="market-1",
            condition_id="0xcondition",
            asset_id="0xassetyes",
            wallet_id="0xabcdef",
            source_event_id="206518773",
        )

        envelopes_iter, rejected = _reconstruct_envelopes([raw_record])
        envelopes = list(envelopes_iter)

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(rejected, [])
        envelope = envelopes[0]
        self.assertEqual(envelope["record_type"], "trade")
        self.assertEqual(envelope["target_table"], "trades")
        self.assertEqual(envelope["envelope_id"], "data_api:trades:raw-trade-1:0:trade")
        self.assertEqual(envelope["event_time"], "2026-04-10T14:22:31.487000Z")
        self.assertEqual(envelope["event_time_source"], "provider")
        self.assertEqual(envelope["market_id"], "market-1")
        self.assertEqual(envelope["condition_id"], "0xcondition")
        self.assertEqual(envelope["asset_id"], "0xassetyes")
        self.assertEqual(envelope["wallet_id"], "0xabcdef")
        self.assertEqual(envelope["transaction_hash"], "0xhashabc")
        self.assertEqual(envelope["trade_id"], "206518773")
        self.assertEqual(envelope["side"], "BUY")
        self.assertEqual(envelope["outcome_side"], "YES")
        self.assertEqual(envelope["price"], Decimal("0.630000000000"))
        self.assertEqual(envelope["size"], Decimal("150.000000000000"))
        self.assertEqual(envelope["usdc_notional"], Decimal("94.500000000000"))
        self.assertEqual(envelope["dedupe_key"], "0xhashabc")
        self.assertEqual(envelope["source_priority"], 3)
        self.assertIsNone(envelope["yes_price"])
        self.assertIsNone(envelope["bids_json"])

    def test_reconstruct_market_snapshot_and_order_book_fields(self) -> None:
        snapshot_record = self._raw_record(
            raw_event_uuid="raw-market-1",
            source_system="gamma",
            source_endpoint="/markets",
            payload_json={
                "id": "market-2",
                "conditionId": "0xcondition2",
                "outcomes": ["Yes", "No"],
                "outcomePrices": ["0.41", "0.59"],
                "bestBid": "0.40",
                "bestAsk": "0.42",
                "lastTradePrice": "0.41",
                "events": [{"id": "event-22"}],
            },
            market_id="market-2",
            condition_id="0xcondition2",
        )
        order_book_record = self._raw_record(
            raw_event_uuid="raw-book-1",
            source_system="ws_market",
            source_endpoint="market:book",
            payload_json={
                "event_type": "book",
                "asset_id": "0xassetbook",
                "bids": [{"price": "0.61", "size": "12"}, {"price": "0.60", "size": "8"}],
                "asks": [{"price": "0.63", "size": "4"}, {"price": "0.64", "size": "10"}],
            },
            market_id="market-3",
            condition_id="0xcondition3",
            asset_id="0xassetbook",
        )

        envelopes_iter, rejected = _reconstruct_envelopes([snapshot_record, order_book_record])
        envelopes = list(envelopes_iter)

        self.assertEqual(rejected, [])
        self.assertEqual(len(envelopes), 2)

        snapshot = envelopes[0]
        self.assertEqual(snapshot["record_type"], "market_snapshot")
        self.assertEqual(snapshot["target_table"], "snapshots")
        self.assertEqual(snapshot["event_id"], "event-22")
        self.assertEqual(snapshot["yes_price"], Decimal("0.410000000000"))
        self.assertEqual(snapshot["no_price"], Decimal("0.590000000000"))
        self.assertEqual(snapshot["best_bid"], Decimal("0.400000000000"))
        self.assertEqual(snapshot["best_ask"], Decimal("0.420000000000"))
        self.assertEqual(snapshot["spread"], Decimal("0.020000000000"))
        self.assertEqual(snapshot["mid_price"], Decimal("0.410000000000"))

        order_book = envelopes[1]
        self.assertEqual(order_book["record_type"], "order_book_snapshot")
        self.assertEqual(order_book["target_table"], "order_book_snapshots")
        self.assertEqual(order_book["asset_id"], "0xassetbook")
        self.assertEqual(order_book["depth_bids"], 2)
        self.assertEqual(order_book["depth_asks"], 2)
        self.assertEqual(order_book["best_bid"], Decimal("0.610000000000"))
        self.assertEqual(order_book["best_ask"], Decimal("0.630000000000"))
        self.assertEqual(order_book["spread"], Decimal("0.020000000000"))
        self.assertEqual(order_book["bid_volume"], Decimal("20.000000000000"))
        self.assertEqual(order_book["ask_volume"], Decimal("14.000000000000"))
        self.assertEqual(
            order_book["bids_json"],
            [
                {"price": "0.610000000000", "size": "12.000000000000"},
                {"price": "0.600000000000", "size": "8.000000000000"},
            ],
        )

    def test_reconstruct_market_resolution_record(self) -> None:
        raw_record = self._raw_record(
            raw_event_uuid="raw-resolution-1",
            source_system="ws_market",
            source_endpoint="market:market_resolved",
            payload_json={
                "event_type": "market_resolved",
                "market": "0xcondition4",
                "asset_id": "0xassetr",
                "price": "1",
            },
            market_id="market-4",
            condition_id="0xcondition4",
            asset_id="0xassetr",
        )

        envelopes_iter, rejected = _reconstruct_envelopes([raw_record])
        envelopes = list(envelopes_iter)

        self.assertEqual(rejected, [])
        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope["record_type"], "market_resolution")
        self.assertEqual(envelope["final_price"], Decimal("1.000000000000"))
        self.assertEqual(envelope["resolution_outcome"], "YES")
        self.assertEqual(envelope["envelope_id"], "ws_market:market:market_resolved:raw-resolution-1:0:market_resolution")

    def test_reconstruct_list_payload_fans_out_without_reordering(self) -> None:
        raw_record = self._raw_record(
            raw_event_uuid="raw-fanout-1",
            source_system="data_api",
            source_endpoint="/trades",
            payload_json=[
                {
                    "id": "trade-1",
                    "asset": "0xasset1",
                    "conditionId": "0xcondition5",
                    "outcome": "YES",
                    "side": "BUY",
                    "price": "0.10",
                    "size": "1.5",
                },
                {
                    "id": "trade-2",
                    "asset": "0xasset2",
                    "conditionId": "0xcondition5",
                    "outcome": "NO",
                    "side": "SELL",
                    "price": "0.11",
                    "size": "2.5",
                },
            ],
            market_id="market-5",
            condition_id="0xcondition5",
        )

        envelopes_iter, rejected = _reconstruct_envelopes([raw_record])
        envelopes = list(envelopes_iter)

        self.assertEqual(rejected, [])
        self.assertEqual([envelope["trade_id"] for envelope in envelopes], ["trade-1", "trade-2"])
        self.assertEqual([envelope["record_index"] for envelope in envelopes], [0, 1])
        self.assertEqual(
            [envelope["envelope_id"] for envelope in envelopes],
            [
                "data_api:trades:raw-fanout-1:0:trade",
                "data_api:trades:raw-fanout-1:1:trade",
            ],
        )

    def test_reconstruct_emits_rejection_when_required_fields_are_missing(self) -> None:
        raw_record = self._raw_record(
            raw_event_uuid="raw-bad-1",
            source_system="data_api",
            source_endpoint="/trades",
            payload_json={
                "id": "trade-bad",
                "price": "0.44",
                "size": "8",
                "side": "BUY",
                "outcome": "YES",
            },
        )

        envelopes_iter, rejected = _reconstruct_envelopes([raw_record])
        envelopes = list(envelopes_iter)

        self.assertEqual(envelopes, [])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].reason_code, "missing_required_field")
        self.assertEqual(rejected[0].raw_event_uuid, "raw-bad-1")
        self.assertEqual(rejected[0].record_index, 0)

    def _raw_record(
        self,
        *,
        raw_event_uuid: str,
        source_system: str,
        source_endpoint: str,
        payload_json,
        market_id: str | None = None,
        condition_id: str | None = None,
        asset_id: str | None = None,
        wallet_id: str | None = None,
        source_event_id: str | None = None,
    ) -> RawArchiveRecord:
        """Build one raw archive record fixture."""

        return RawArchiveRecord(
            raw_event_uuid=raw_event_uuid,
            payload_json=payload_json,
            event_time="2026-04-10T14:22:31.487000Z",
            ingest_time="2026-04-10T14:22:33.102000Z",
            event_time_source="provider",
            source_system=source_system,
            source_endpoint=source_endpoint,
            manifest_id="manifest-1",
            archive_uri="archive/part-0001.ndjson",
            payload_hash="sha256:test",
            collector_source="phase2-test",
            collector_version="git:test",
            raw_schema_version="raw-envelope/v1",
            file_offset=0,
            line_number=1,
            source_event_id=source_event_id,
            market_id=market_id,
            condition_id=condition_id,
            asset_id=asset_id,
            wallet_id=wallet_id,
        )


if __name__ == "__main__":
    unittest.main()
