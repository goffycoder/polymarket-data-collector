from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1

from config.settings import (
    PHASE5_DIRECTION_MIN_ABS_VELOCITY,
    PHASE5_ENTRY_SLIPPAGE_TICKS,
    PHASE5_EXIT_SLIPPAGE_TICKS,
    PHASE5_FEE_RATE,
    PHASE5_MAX_CONCURRENT_EVENT_FAMILY,
    PHASE5_MAX_CONCURRENT_GLOBAL,
    PHASE5_MAX_HOLDING_HOURS,
    PHASE5_MAX_TRADE_NOTIONAL,
    PHASE5_NEAR_RESOLUTION_MINUTES,
    PHASE5_RISK_STOP_LOSS_FRACTION,
    PHASE5_SIMULATOR_VERSION,
    PHASE5_TICK_SIZE,
)
from phase5.models import EvaluationRow, PaperTradeResult
from phase5.repository import Phase5Repository, SnapshotPoint


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _clamp_price(value: float) -> float:
    return max(0.0, min(1.0, value))


def _trade_id(row: EvaluationRow) -> str:
    raw = f"{row.alert_id or row.candidate_id}:{PHASE5_SIMULATOR_VERSION}"
    return sha1(raw.encode("utf-8")).hexdigest()[:16]


def infer_direction(row: EvaluationRow) -> str | None:
    velocity = row.feature_snapshot.get("probability_velocity")
    try:
        score = float(velocity)
    except (TypeError, ValueError):
        return None
    if abs(score) < PHASE5_DIRECTION_MIN_ABS_VELOCITY:
        return None
    return "YES" if score > 0 else "NO"


def _entry_quote(direction: str, snapshot: SnapshotPoint) -> float | None:
    if direction == "YES":
        candidates = []
        if snapshot.best_ask is not None:
            candidates.append(snapshot.best_ask)
        if snapshot.no_price is not None:
            candidates.append(1.0 - snapshot.no_price)
        if not candidates:
            return None
        return max(candidates)

    candidates = []
    if snapshot.yes_price is not None:
        candidates.append(1.0 - snapshot.yes_price)
    if snapshot.no_price is not None:
        candidates.append(snapshot.no_price)
    if not candidates:
        return None
    return max(candidates)


def _exit_quote(direction: str, snapshot: SnapshotPoint) -> float | None:
    if direction == "YES":
        candidates = []
        if snapshot.best_bid is not None:
            candidates.append(snapshot.best_bid)
        if snapshot.yes_price is not None:
            candidates.append(snapshot.yes_price)
        if not candidates:
            return None
        return min(candidates)

    candidates = []
    if snapshot.no_price is not None:
        candidates.append(snapshot.no_price)
    if snapshot.best_ask is not None:
        candidates.append(1.0 - snapshot.best_ask)
    if not candidates:
        return None
    return min(candidates)


@dataclass(slots=True)
class ActiveTrade:
    event_family_id: str
    exit_time: datetime


class ConservativePaperTrader:
    def __init__(self, *, repository: Phase5Repository):
        self.repository = repository

    def simulate(self, rows: list[EvaluationRow]) -> list[PaperTradeResult]:
        ordered_rows = sorted(
            [row for row in rows if row.alert_id is not None],
            key=lambda row: (
                row.alert_created_at or "",
                row.alert_severity or "",
                row.alert_id or "",
            ),
        )
        results: list[PaperTradeResult] = []
        open_positions: list[ActiveTrade] = []

        for row in ordered_rows:
            alert_time = _parse_iso(row.alert_created_at)
            if alert_time is None:
                results.append(self._skip_result(row, reason="invalid_replay_coverage"))
                continue

            open_positions = [item for item in open_positions if item.exit_time > alert_time]
            same_family_open = sum(1 for item in open_positions if item.event_family_id == row.event_family_id)
            if same_family_open >= PHASE5_MAX_CONCURRENT_EVENT_FAMILY:
                results.append(self._skip_result(row, reason="skipped_event_family_position_limit"))
                continue
            if len(open_positions) >= PHASE5_MAX_CONCURRENT_GLOBAL:
                results.append(self._skip_result(row, reason="skipped_global_position_limit"))
                continue

            result = self._simulate_one(row)
            results.append(result)
            if result.status in {"filled", "resolved"} and result.exit_time is not None:
                exit_dt = _parse_iso(result.exit_time)
                if exit_dt is not None:
                    open_positions.append(
                        ActiveTrade(
                            event_family_id=row.event_family_id,
                            exit_time=exit_dt,
                        )
                    )

        return results

    def _simulate_one(self, row: EvaluationRow) -> PaperTradeResult:
        alert_time = _parse_iso(row.alert_created_at)
        if alert_time is None:
            return self._skip_result(row, reason="invalid_replay_coverage")

        if row.coverage_status == "coverage_insufficient":
            return self._skip_result(row, reason="invalid_replay_coverage")

        resolution_time = _parse_iso(row.resolution_time)
        if resolution_time is not None and resolution_time <= alert_time:
            return self._skip_result(row, reason="skipped_already_resolved")

        market_end = _parse_iso(row.market_end_date)
        if market_end is not None:
            delta_minutes = (market_end - alert_time).total_seconds() / 60.0
            if delta_minutes <= PHASE5_NEAR_RESOLUTION_MINUTES:
                return self._skip_result(row, reason="skipped_near_resolution")

        direction = infer_direction(row)
        if direction is None:
            return self._skip_result(row, reason="skipped_no_directional_mapping")

        end_bound = alert_time + timedelta(hours=max(1, PHASE5_MAX_HOLDING_HOURS))
        series_end = end_bound
        if resolution_time is not None and resolution_time > series_end:
            series_end = resolution_time

        series = list(self.repository.load_snapshot_series(row.market_id, _iso(alert_time), _iso(series_end)))
        if not series:
            return self._skip_result(row, reason="skipped_insufficient_market_data", direction=direction)

        entry_snapshot = None
        entry_time = None
        for point in series:
            point_dt = _parse_iso(point.captured_at)
            if point_dt is None or point_dt < alert_time:
                continue
            quote = _entry_quote(direction, point)
            if quote is None:
                continue
            entry_snapshot = point
            entry_time = point_dt
            break

        if entry_snapshot is None or entry_time is None:
            return self._skip_result(row, reason="expired_no_fill", direction=direction)

        raw_entry = _entry_quote(direction, entry_snapshot)
        if raw_entry is None:
            return self._skip_result(row, reason="skipped_insufficient_execution_quote", direction=direction)
        entry_price = _clamp_price(raw_entry + (PHASE5_TICK_SIZE * PHASE5_ENTRY_SLIPPAGE_TICKS))

        quantity = PHASE5_MAX_TRADE_NOTIONAL / entry_price if entry_price > 0 else 0.0
        if quantity <= 0:
            return self._skip_result(row, reason="skipped_insufficient_execution_quote", direction=direction)

        exit_deadline = entry_time + timedelta(hours=max(1, PHASE5_MAX_HOLDING_HOURS))
        stop_price = entry_price * max(0.0, 1.0 - PHASE5_RISK_STOP_LOSS_FRACTION)

        exit_price: float | None = None
        exit_time: datetime | None = None
        status = "filled"
        notes: list[str] = []

        for point in series:
            point_dt = _parse_iso(point.captured_at)
            if point_dt is None or point_dt <= entry_time:
                continue
            if resolution_time is not None and point_dt >= resolution_time:
                break

            candidate_exit = _exit_quote(direction, point)
            if candidate_exit is None:
                continue
            candidate_exit = _clamp_price(candidate_exit - (PHASE5_TICK_SIZE * PHASE5_EXIT_SLIPPAGE_TICKS))
            if candidate_exit <= stop_price:
                exit_price = candidate_exit
                exit_time = point_dt
                notes.append("risk_stop")
                break
            if point_dt >= exit_deadline:
                exit_price = candidate_exit
                exit_time = point_dt
                notes.append("time_stop")
                break

        if exit_time is None:
            if resolution_time is not None and row.resolution_outcome is not None:
                exit_time = resolution_time
                exit_price = 1.0 if row.resolution_outcome == direction else 0.0
                status = "resolved"
                notes.append("resolution_exit")
            else:
                later_quotes = [point for point in series if (_parse_iso(point.captured_at) or alert_time) >= exit_deadline]
                if later_quotes:
                    point = later_quotes[0]
                    candidate_exit = _exit_quote(direction, point)
                    if candidate_exit is not None:
                        exit_price = _clamp_price(candidate_exit - (PHASE5_TICK_SIZE * PHASE5_EXIT_SLIPPAGE_TICKS))
                        exit_time = _parse_iso(point.captured_at)
                        notes.append("time_stop")

        if exit_time is None or exit_price is None:
            return self._skip_result(row, reason="expired_unexitable", direction=direction)

        fee_paid = (PHASE5_MAX_TRADE_NOTIONAL * PHASE5_FEE_RATE) * 2.0
        pnl = (quantity * (exit_price - entry_price)) - fee_paid
        holding_seconds = max(0.0, (exit_time - entry_time).total_seconds())

        return PaperTradeResult(
            paper_trade_id=_trade_id(row),
            alert_id=row.alert_id,
            candidate_id=row.candidate_id,
            market_id=row.market_id,
            event_family_id=row.event_family_id,
            decision_timestamp=row.alert_created_at or row.candidate_trigger_time,
            direction=direction,
            status=status,
            skip_reason=None,
            entry_time=_iso(entry_time),
            exit_time=_iso(exit_time),
            entry_price=round(entry_price, 6),
            exit_price=round(exit_price, 6),
            fee_paid=round(fee_paid, 6),
            pnl_bounded=round(pnl, 6),
            holding_seconds=holding_seconds,
            notes=notes,
        )

    def _skip_result(self, row: EvaluationRow, *, reason: str, direction: str | None = None) -> PaperTradeResult:
        return PaperTradeResult(
            paper_trade_id=_trade_id(row),
            alert_id=row.alert_id,
            candidate_id=row.candidate_id,
            market_id=row.market_id,
            event_family_id=row.event_family_id,
            decision_timestamp=row.alert_created_at or row.candidate_trigger_time,
            direction=direction,
            status="skipped",
            skip_reason=reason,
        )

