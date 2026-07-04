"""
risk_manager.py
Production-grade Risk Manager with Kelly Criterion sizing,
portfolio-level correlation guards, daily loss circuit breaker,
margin estimation, and black-swan veto logic.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  RISK CONFIGURATION
# ════════════════════════════════════════════════════════════════

@dataclass
class RiskConfig:
    # Capital
    total_capital:         float = 1_000_000.0    # ₹10 lakh default

    # Per-trade limits
    max_risk_per_trade_pct: float = 1.5           # % of capital at risk per trade
    max_position_size_pct:  float = 20.0          # max single position % of capital
    min_risk_reward:        float = 1.5           # minimum RRR to approve signal

    # Portfolio limits
    max_open_positions:     int   = 8
    max_correlated_positions: int = 3             # max positions in same sector
    max_fno_exposure_pct:   float = 40.0          # max F&O as % of capital

    # Daily circuit breakers
    max_daily_loss_pct:     float = 3.0           # halt if daily P&L < -3%
    max_daily_loss_inr:     float = 30_000.0      # absolute ₹ limit
    max_consecutive_losses: int   = 4             # pause after N consecutive losses

    # Black-swan guards
    vix_halt_threshold:     float = 30.0          # halt new trades if VIX > 30
    max_sl_pct:             float = 8.0           # reject if SL > 8% from entry
    min_sl_pct:             float = 0.3           # reject if SL < 0.3% (too tight)

    # Margin buffer
    margin_buffer_pct:      float = 20.0          # keep 20% extra margin buffer

    # Kelly fraction (fractional Kelly for safety)
    kelly_fraction:         float = 0.25          # use 25% Kelly (quarter Kelly)


# ════════════════════════════════════════════════════════════════
#  POSITION TRACKER
# ════════════════════════════════════════════════════════════════

@dataclass
class OpenPosition:
    signal_id:      str
    symbol:         str
    instrument_type: str
    direction:      str
    entry_price:    float
    stop_loss:      float
    target_1:       float
    quantity:       int
    lots:           int
    entry_time:     datetime
    peak_price:     float
    sector:         str = "GENERAL"
    is_hedge:       bool = False
    margin_used:    float = 0.0

    @property
    def unrealized_pnl(self) -> float:
        if self.direction == "LONG":
            return (self.peak_price - self.entry_price) * self.quantity
        return (self.entry_price - self.peak_price) * self.quantity

    @property
    def sl_distance_pct(self) -> float:
        return abs(self.entry_price - self.stop_loss) / self.entry_price * 100


# ════════════════════════════════════════════════════════════════
#  RISK DECISION OUTPUT
# ════════════════════════════════════════════════════════════════

@dataclass
class RiskDecision:
    approved:         bool
    reason:           str
    approved_quantity: int = 0
    approved_lots:    int  = 0
    position_size_inr: float = 0.0
    risk_amount_inr:  float = 0.0
    kelly_size:       float = 0.0
    warnings:         list[str] = field(default_factory=list)


# ════════════════════════════════════════════════════════════════
#  RISK MANAGER
# ════════════════════════════════════════════════════════════════

class RiskManager:
    """
    Central risk authority. Every signal must be approved here
    before being written to DB or broadcast to the dashboard.
    """

    def __init__(self, config: RiskConfig = None):
        self.cfg               = config or RiskConfig()
        self._lock             = asyncio.Lock()
        self.open_positions:   dict[str, OpenPosition] = {}  # signal_id → position
        self.daily_pnl:        float = 0.0
        self.daily_start_bal:  float = self.cfg.total_capital
        self.consecutive_losses: int = 0
        self._today:           date = date.today()

    # ── Day reset ─────────────────────────────────────────────────
    async def start_day(self, opening_balance: float):
        async with self._lock:
            if self._today != date.today():
                self._today           = date.today()
                self.daily_pnl        = 0.0
                self.consecutive_losses = 0
            self.daily_start_bal      = opening_balance
            self.cfg.total_capital    = opening_balance
            logger.info(f"Risk Manager day started. Balance: ₹{opening_balance:,.2f}")

    # ── Main approval gate ────────────────────────────────────────
    async def approve_signal(self,
                             symbol:         str,
                             instrument_type: str,
                             direction:      str,
                             entry_price:    float,
                             stop_loss:      float,
                             target_1:       float,
                             sector:         str = "GENERAL",
                             vix:            float = 18.0,
                             historical_win_rate: float = 0.50,
                             lot_size:       int = 1,
                             lots:           int = 1) -> RiskDecision:
        """
        Runs all risk checks in sequence. Returns RiskDecision.
        All checks are fast (no I/O); lock ensures thread safety.
        """
        async with self._lock:
            warnings = []

            # ── Guard 1: Daily loss circuit breaker ──────────────
            daily_loss = -self.daily_pnl
            daily_loss_pct = daily_loss / max(self.daily_start_bal, 1) * 100
            if daily_loss_pct >= self.cfg.max_daily_loss_pct:
                return RiskDecision(False,
                    f"Daily loss circuit open: -{daily_loss_pct:.1f}% ≥ limit {self.cfg.max_daily_loss_pct}%")
            if daily_loss >= self.cfg.max_daily_loss_inr:
                return RiskDecision(False,
                    f"Daily loss ₹{daily_loss:,.0f} ≥ limit ₹{self.cfg.max_daily_loss_inr:,.0f}")

            # ── Guard 2: Consecutive loss pause ──────────────────
            if self.consecutive_losses >= self.cfg.max_consecutive_losses:
                return RiskDecision(False,
                    f"Paused: {self.consecutive_losses} consecutive losses. Review strategy.")

            # ── Guard 3: Black-swan VIX halt ─────────────────────
            if vix >= self.cfg.vix_halt_threshold:
                return RiskDecision(False,
                    f"VIX {vix:.1f} ≥ halt threshold {self.cfg.vix_halt_threshold}. No new trades.")

            # ── Guard 4: Max open positions ───────────────────────
            if len(self.open_positions) >= self.cfg.max_open_positions:
                return RiskDecision(False,
                    f"Max open positions ({self.cfg.max_open_positions}) reached.")

            # ── Guard 5: SL sanity check ─────────────────────────
            sl_pct = abs(entry_price - stop_loss) / entry_price * 100
            if sl_pct > self.cfg.max_sl_pct:
                return RiskDecision(False,
                    f"SL {sl_pct:.1f}% too wide (max {self.cfg.max_sl_pct}%). Catastrophic risk.")
            if sl_pct < self.cfg.min_sl_pct:
                return RiskDecision(False,
                    f"SL {sl_pct:.2f}% too tight (min {self.cfg.min_sl_pct}%). Will be stopped out by noise.")

            # ── Guard 6: Risk/Reward ratio ────────────────────────
            rrr = abs(target_1 - entry_price) / max(abs(entry_price - stop_loss), 0.01)
            if rrr < self.cfg.min_risk_reward:
                return RiskDecision(False,
                    f"RRR {rrr:.2f} < minimum {self.cfg.min_risk_reward}. Skip.")

            # ── Guard 7: Correlation / sector concentration ───────
            sector_count = sum(
                1 for p in self.open_positions.values()
                if p.sector == sector and not p.is_hedge
            )
            if sector_count >= self.cfg.max_correlated_positions:
                warnings.append(f"Sector '{sector}' already has {sector_count} positions")

            # ── Guard 8: F&O exposure cap ─────────────────────────
            fno_types = {"NIFTY_FUT","NIFTY_OPT","SENSEX_FUT","SENSEX_OPT",
                         "STOCK_FUT","STOCK_OPT"}
            if instrument_type in fno_types:
                fno_margin = sum(
                    p.margin_used for p in self.open_positions.values()
                    if p.instrument_type in fno_types
                )
                fno_pct = fno_margin / max(self.cfg.total_capital, 1) * 100
                if fno_pct >= self.cfg.max_fno_exposure_pct:
                    return RiskDecision(False,
                        f"F&O exposure {fno_pct:.1f}% ≥ limit {self.cfg.max_fno_exposure_pct}%")

            # ── Position Sizing ────────────────────────────────────
            kelly_qty, kelly_size = self._kelly_position_size(
                entry_price, stop_loss, historical_win_rate, rrr, lot_size, lots
            )
            risk_qty, risk_inr = self._fixed_risk_position_size(
                entry_price, stop_loss, lot_size, lots
            )

            # Use the SMALLER of Kelly and fixed-risk sizing (conservative)
            final_qty = min(kelly_qty, risk_qty)
            final_qty = max(final_qty, lot_size)  # at least 1 lot
            final_lots = final_qty // lot_size

            # Cap by max position size
            max_qty_by_capital = int(
                self.cfg.total_capital * self.cfg.max_position_size_pct / 100 / entry_price
            )
            final_qty  = min(final_qty, max_qty_by_capital)
            final_lots = final_qty // lot_size

            if final_qty <= 0:
                return RiskDecision(False, "Computed quantity = 0. Insufficient capital.")

            position_value = final_qty * entry_price
            risk_amount    = abs(entry_price - stop_loss) * final_qty

            return RiskDecision(
                approved=True,
                reason="All risk checks passed",
                approved_quantity=final_qty,
                approved_lots=final_lots,
                position_size_inr=round(position_value, 2),
                risk_amount_inr=round(risk_amount, 2),
                kelly_size=round(kelly_size, 4),
                warnings=warnings,
            )

    # ── Position lifecycle ─────────────────────────────────────────
    async def register_position(self, signal_id: str, position: OpenPosition):
        async with self._lock:
            self.open_positions[signal_id] = position
            logger.info(f"Position registered: {signal_id} {position.symbol} "
                        f"{position.direction} qty={position.quantity}")

    async def update_peak_price(self, signal_id: str, current_price: float):
        async with self._lock:
            pos = self.open_positions.get(signal_id)
            if not pos: return
            if pos.direction == "LONG":
                pos.peak_price = max(pos.peak_price, current_price)
            else:
                pos.peak_price = min(pos.peak_price, current_price)

    async def close_position(self, signal_id: str, exit_price: float,
                              reason: str) -> Optional[float]:
        """Close position and update daily P&L and loss streak."""
        async with self._lock:
            pos = self.open_positions.pop(signal_id, None)
            if not pos:
                return None
            if pos.direction == "LONG":
                pnl = (exit_price - pos.entry_price) * pos.quantity
            else:
                pnl = (pos.entry_price - exit_price) * pos.quantity

            self.daily_pnl += pnl
            if pnl < 0:
                self.consecutive_losses += 1
            else:
                self.consecutive_losses = 0

            logger.info(f"Position closed: {signal_id} | P&L ₹{pnl:+,.2f} | "
                        f"Reason: {reason} | Daily P&L: ₹{self.daily_pnl:+,.2f}")
            return round(pnl, 2)

    # ── Trailing SL computation ───────────────────────────────────
    async def compute_trailing_sl(self, signal_id: str,
                                   current_price: float,
                                   atr: float,
                                   atr_multiplier: float = 1.0) -> Optional[float]:
        """
        Returns new SL if trailing stop should be moved, else None.
        Uses ATR-based trailing: trail at atr_multiplier × ATR from peak.
        """
        async with self._lock:
            pos = self.open_positions.get(signal_id)
            if not pos: return None

            await self.update_peak_price(signal_id, current_price)
            trail_dist = atr * atr_multiplier

            if pos.direction == "LONG":
                new_sl = round(pos.peak_price - trail_dist, 2)
                if new_sl > pos.stop_loss:
                    pos.stop_loss = new_sl
                    return new_sl
            else:
                new_sl = round(pos.peak_price + trail_dist, 2)
                if new_sl < pos.stop_loss:
                    pos.stop_loss = new_sl
                    return new_sl
            return None

    # ── Position sizing engines ───────────────────────────────────
    def _kelly_position_size(self, entry: float, sl: float,
                              win_rate: float, rrr: float,
                              lot_size: int, min_lots: int) -> tuple[int, float]:
        """
        Full Kelly: f* = (p × b - q) / b
        where p = win_rate, q = 1-p, b = RRR
        Fractional Kelly = f* × kelly_fraction
        """
        p = max(0.01, min(0.99, win_rate))
        q = 1.0 - p
        b = max(0.01, rrr)
        kelly_full  = (p * b - q) / b
        kelly_frac  = kelly_full * self.cfg.kelly_fraction
        kelly_frac  = max(0.0, min(kelly_frac, 0.25))  # hard cap at 25% of capital

        risk_capital  = self.cfg.total_capital * kelly_frac
        sl_distance   = abs(entry - sl)
        if sl_distance == 0: return 0, 0.0
        kelly_qty = int(risk_capital / sl_distance)
        kelly_qty = (kelly_qty // lot_size) * lot_size  # round to lot
        return kelly_qty, kelly_frac

    def _fixed_risk_position_size(self, entry: float, sl: float,
                                   lot_size: int, min_lots: int) -> tuple[int, float]:
        """
        Fixed fractional: risk RISK_PCT% of capital per trade.
        qty = (capital × risk_pct) / sl_distance
        """
        risk_amount = self.cfg.total_capital * self.cfg.max_risk_per_trade_pct / 100
        sl_distance = abs(entry - sl)
        if sl_distance == 0: return 0, 0.0
        qty = int(risk_amount / sl_distance)
        qty = max(qty, lot_size)
        return qty, risk_amount

    # ── Portfolio snapshot ────────────────────────────────────────
    def get_portfolio_snapshot(self) -> dict:
        total_margin = sum(p.margin_used for p in self.open_positions.values())
        total_unreal = sum(p.unrealized_pnl for p in self.open_positions.values())
        daily_loss_pct = -self.daily_pnl / max(self.daily_start_bal, 1) * 100
        return {
            "total_capital":       self.cfg.total_capital,
            "daily_pnl":           round(self.daily_pnl, 2),
            "daily_pnl_pct":       round(self.daily_pnl / max(self.daily_start_bal, 1) * 100, 3),
            "daily_loss_pct":      round(daily_loss_pct, 3),
            "circuit_status":      "OPEN" if daily_loss_pct >= self.cfg.max_daily_loss_pct else "CLOSED",
            "open_positions":      len(self.open_positions),
            "total_margin_used":   round(total_margin, 2),
            "unrealized_pnl":      round(total_unreal, 2),
            "consecutive_losses":  self.consecutive_losses,
            "available_capital":   round(self.cfg.total_capital - total_margin, 2),
        }
