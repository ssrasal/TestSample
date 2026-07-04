"""
strategy_router.py
Routes incoming market data + sentiment context to the correct
strategy module. Prevents duplicate signals and manages signal
deduplication, cooldown periods, and strategy lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from core.sentiment_engine import SentimentContext, MarketRegime
from core.signal_generator import SignalGenerator, Signal
from risk.risk_manager import RiskManager, RiskDecision, OpenPosition

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  STRATEGY COOLDOWN TRACKER (prevents duplicate signals)
# ════════════════════════════════════════════════════════════════

class CooldownTracker:
    """
    Tracks when a signal was last generated for each symbol/type pair.
    Prevents rapid re-signalling on the same instrument.
    """

    COOLDOWNS = {
        "STOCK":       timedelta(minutes=15),
        "STOCK_OPT":   timedelta(minutes=30),
        "STOCK_FUT":   timedelta(minutes=20),
        "NIFTY_FUT":   timedelta(minutes=10),
        "NIFTY_OPT":   timedelta(minutes=15),
        "SENSEX_FUT":  timedelta(minutes=10),
        "SENSEX_OPT":  timedelta(minutes=15),
    }

    def __init__(self):
        self._last_signal: dict[str, datetime] = {}

    def is_on_cooldown(self, symbol: str, instrument_type: str) -> bool:
        key      = f"{instrument_type}:{symbol}"
        last     = self._last_signal.get(key)
        cooldown = self.COOLDOWNS.get(instrument_type, timedelta(minutes=15))
        return last is not None and (datetime.now() - last) < cooldown

    def mark_signalled(self, symbol: str, instrument_type: str):
        self._last_signal[f"{instrument_type}:{symbol}"] = datetime.now()

    def reset(self, symbol: str, instrument_type: str):
        self._last_signal.pop(f"{instrument_type}:{symbol}", None)


# ════════════════════════════════════════════════════════════════
#  STRATEGY ROUTER
# ════════════════════════════════════════════════════════════════

class StrategyRouter:
    """
    Stateless router that:
    1. Selects the correct strategy based on regime + instrument type
    2. Runs the SignalGenerator
    3. Applies Risk Manager approval
    4. Returns approved Signal or None
    """

    def __init__(self, risk_manager: RiskManager):
        self.risk          = risk_manager
        self.cooldown      = CooldownTracker()
        self._lock         = asyncio.Lock()

    async def route(self,
                    instrument_type: str,
                    symbol:          str,
                    df,                        # pd.DataFrame with indicators
                    context:         SentimentContext,
                    extra_kwargs:    dict = None) -> Optional[Signal]:
        """
        Main routing method. Called by the Engine Worker for each instrument tick.
        Returns approved Signal or None.
        """
        # Cooldown check (fast, no lock needed)
        if self.cooldown.is_on_cooldown(symbol, instrument_type):
            logger.debug(f"Cooldown active for {instrument_type}:{symbol}")
            return None

        generator = SignalGenerator(context, self.risk.cfg.total_capital)
        kwargs    = extra_kwargs or {}

        # ── Route to correct generator ────────────────────────────
        signal = await self._generate(instrument_type, symbol, df, generator, kwargs)

        if signal is None:
            return None

        # ── Risk approval ─────────────────────────────────────────
        decision = await self.risk.approve_signal(
            symbol          = symbol,
            instrument_type = instrument_type,
            direction       = signal.direction.value,
            entry_price     = signal.entry_price_high,
            stop_loss       = signal.stop_loss,
            target_1        = signal.target_1,
            sector          = kwargs.get("sector", "GENERAL"),
            vix             = context.india_vix or 18.0,
            historical_win_rate = kwargs.get("win_rate", 0.50),
            lot_size        = kwargs.get("lot_size", 1),
            lots            = signal.suggested_lots or 1,
        )

        if not decision.approved:
            logger.info(f"Signal REJECTED by Risk: {symbol} — {decision.reason}")
            return None

        if decision.warnings:
            for w in decision.warnings:
                logger.warning(f"Risk warning for {symbol}: {w}")

        # Stamp approved sizing onto signal
        signal.suggested_qty       = decision.approved_quantity
        signal.suggested_lots      = decision.approved_lots
        signal.position_size_inr   = decision.position_size_inr

        # Mark cooldown
        self.cooldown.mark_signalled(symbol, instrument_type)

        logger.info(
            f"Signal APPROVED: {instrument_type} {symbol} {signal.direction.value} "
            f"score={signal.signal_score} confidence={signal.confidence.value} "
            f"qty={decision.approved_quantity} risk=₹{decision.risk_amount_inr:,.0f}"
        )
        return signal

    async def _generate(self, instrument_type: str, symbol: str,
                         df, generator: SignalGenerator,
                         kwargs: dict) -> Optional[Signal]:
        """Dispatch to the correct generator method."""
        try:
            if instrument_type == "STOCK":
                call_type = kwargs.get("call_type", "INTRADAY")
                return generator.generate_equity_signal(symbol, df, call_type)

            elif instrument_type == "NIFTY_FUT":
                return generator.generate_nifty_future_signal(
                    df,
                    spot     = kwargs["spot"],
                    contract = kwargs["contract"],
                    expiry   = kwargs["expiry"],
                    lot_size = kwargs.get("lot_size", 75),
                )

            elif instrument_type == "NIFTY_OPT":
                return generator.generate_nifty_option_signal(
                    df,
                    spot        = kwargs["spot"],
                    option_type = kwargs["option_type"],
                    strike      = kwargs["strike"],
                    premium     = kwargs["premium"],
                    expiry      = kwargs["expiry"],
                    dte         = kwargs["dte"],
                    iv          = kwargs.get("iv", 18.0),
                    lot_size    = kwargs.get("lot_size", 75),
                    greeks      = kwargs.get("greeks"),
                )

            elif instrument_type == "SENSEX_FUT":
                return generator.generate_sensex_future_signal(
                    df,
                    spot     = kwargs["spot"],
                    contract = kwargs["contract"],
                    expiry   = kwargs["expiry"],
                    lot_size = kwargs.get("lot_size", 10),
                )

            elif instrument_type == "SENSEX_OPT":
                # Reuse same pattern as NIFTY_OPT
                return generator.generate_nifty_option_signal(
                    df,
                    spot        = kwargs["spot"],
                    option_type = kwargs["option_type"],
                    strike      = kwargs["strike"],
                    premium     = kwargs["premium"],
                    expiry      = kwargs["expiry"],
                    dte         = kwargs["dte"],
                    iv          = kwargs.get("iv", 18.0),
                    lot_size    = kwargs.get("lot_size", 10),
                )

            elif instrument_type in ("STOCK_FUT", "STOCK_OPT"):
                # Equity derivatives — use same logic with stock-specific params
                return generator.generate_equity_signal(symbol, df, "SWING")

            else:
                logger.warning(f"Unknown instrument_type: {instrument_type}")
                return None

        except Exception as e:
            logger.error(f"Signal generation error for {symbol}: {e}", exc_info=True)
            return None

    # ── Regime-based strategy selector ───────────────────────────
    @staticmethod
    def select_strategy_for_regime(regime: MarketRegime,
                                    instrument_type: str) -> str:
        """
        Returns the strategy name to use based on regime.
        Used for logging / dashboard display.
        """
        MATRIX = {
            (MarketRegime.STRONG_BULL, "STOCK"):      "MOMENTUM_LONG",
            (MarketRegime.BULL,        "STOCK"):      "BREAKOUT_LONG",
            (MarketRegime.SIDEWAYS,    "STOCK"):      "MEAN_REVERSION",
            (MarketRegime.BEAR,        "STOCK"):      "SHORT_MOMENTUM",
            (MarketRegime.STRONG_BEAR, "STOCK"):      "AGGRESSIVE_SHORT",
            (MarketRegime.HIGH_VOL,    "STOCK"):      "VOLATILITY_SQUEEZE",

            (MarketRegime.STRONG_BULL, "NIFTY_OPT"): "BUY_CE_ATM",
            (MarketRegime.BULL,        "NIFTY_OPT"): "BULL_PUT_SPREAD",
            (MarketRegime.SIDEWAYS,    "NIFTY_OPT"): "IRON_CONDOR",
            (MarketRegime.BEAR,        "NIFTY_OPT"): "BEAR_CALL_SPREAD",
            (MarketRegime.STRONG_BEAR, "NIFTY_OPT"): "BUY_PE_ATM",
            (MarketRegime.HIGH_VOL,    "NIFTY_OPT"): "STRADDLE_SELL",

            (MarketRegime.STRONG_BULL, "NIFTY_FUT"):  "TREND_LONG",
            (MarketRegime.BULL,        "NIFTY_FUT"):  "PULLBACK_LONG",
            (MarketRegime.SIDEWAYS,    "NIFTY_FUT"):  "RANGE_TRADE",
            (MarketRegime.BEAR,        "NIFTY_FUT"):  "TREND_SHORT",
            (MarketRegime.STRONG_BEAR, "NIFTY_FUT"):  "AGGRESSIVE_SHORT",
            (MarketRegime.HIGH_VOL,    "NIFTY_FUT"):  "REDUCE_EXPOSURE",
        }
        return MATRIX.get((regime, instrument_type), "DEFAULT_STRATEGY")
