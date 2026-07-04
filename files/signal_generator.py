"""
signal_generator.py — STEP 3 (Part 3)
SignalGenerator: Produces fully-specified trading signals including
Entry, SL, Target, Trailing SL rules, and MANDATORY hedging strategy
for every F&O call.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional

import pandas as pd

from core.sentiment_engine import SentimentContext, MarketRegime, SentimentLabel

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  SIGNAL OUTPUT STRUCTURES
# ════════════════════════════════════════════════════════════════

class SignalDirection(str, Enum):
    LONG    = "LONG"
    SHORT   = "SHORT"
    NEUTRAL = "NEUTRAL"


class SignalConfidence(str, Enum):
    A_PLUS  = "A_PLUS"    # ≥ 8 / 10 signals aligned
    A       = "A"         # ≥ 7 / 10
    B_PLUS  = "B_PLUS"    # ≥ 6 / 10
    B       = "B"         # ≥ 5 / 10
    C       = "C"         # < 5 / 10 — weak, generate but flag


class HedgeStrategy(str, Enum):
    BEAR_CALL_SPREAD    = "BEAR_CALL_SPREAD"
    BULL_PUT_SPREAD     = "BULL_PUT_SPREAD"
    IRON_CONDOR         = "IRON_CONDOR"
    PROTECTIVE_PUT      = "PROTECTIVE_PUT"
    COVERED_CALL        = "COVERED_CALL"
    COLLAR              = "COLLAR"
    STRADDLE            = "STRADDLE"
    STRANGLE            = "STRANGLE"
    SYNTHETIC_LONG      = "SYNTHETIC_LONG"
    SYNTHETIC_SHORT     = "SYNTHETIC_SHORT"
    CALENDAR_SPREAD     = "CALENDAR_SPREAD"
    RATIO_SPREAD        = "RATIO_SPREAD"
    NONE                = "NONE"


@dataclass
class HedgeSpec:
    """Fully specified hedge leg(s)."""
    strategy:           HedgeStrategy
    description:        str
    hedge_strike:       Optional[float] = None
    hedge_option_type:  Optional[str]   = None   # "CE" or "PE"
    hedge_lots:         int              = 1
    hedge_premium_est:  Optional[float] = None
    spread_type:        Optional[str]   = None   # "DEBIT" or "CREDIT"
    max_loss_inr:       Optional[float] = None
    max_profit_inr:     Optional[float] = None
    breakeven_1:        Optional[float] = None
    breakeven_2:        Optional[float] = None
    rationale:          str              = ""


@dataclass
class TrailingSLRule:
    """Machine-readable trailing stop-loss specification."""
    method:             str         # "PERCENTAGE", "ATR", "FIXED_POINTS", "TIME_BASED"
    trigger_profit_pct: float       # activate trailing after this % profit
    trail_distance_pct: Optional[float] = None
    trail_atr_mult:     Optional[float] = None
    trail_points:       Optional[float] = None
    time_exit_minutes_before_close: Optional[int] = None
    human_readable:     str = ""


@dataclass
class Signal:
    """Universal signal output — every field must be populated."""
    # Identity
    instrument_type:   str              # "STOCK","STOCK_OPT","STOCK_FUT","NIFTY_FUT",etc.
    symbol:            str
    direction:         SignalDirection
    confidence:        SignalConfidence
    signal_score:      int              # -10 to +10
    signal_time:       datetime

    # Entry
    entry_price_low:   float
    entry_price_high:  float
    entry_order_type:  str              # "LIMIT","MARKET","SL"
    entry_trigger:     Optional[float]  # for breakout signals

    # Targets
    target_1:          float
    target_2:          Optional[float]
    target_3:          Optional[float]
    target_pct_1:      float            # % move to target 1
    target_pct_2:      Optional[float]

    # Stop Loss
    stop_loss:         float
    sl_pct:            float            # % risk from entry
    risk_reward_1:     float
    risk_reward_2:     Optional[float]

    # Trailing SL
    trailing_sl:       TrailingSLRule

    # Hedge (MANDATORY for F&O)
    hedge:             HedgeSpec

    # Sizing
    suggested_lots:    Optional[int]    = None
    suggested_qty:     Optional[int]    = None
    position_size_inr: Optional[float]  = None

    # Context
    regime:            Optional[MarketRegime]   = None
    sentiment:         Optional[SentimentLabel] = None
    sentiment_score:   Optional[float]          = None
    vix_at_signal:     Optional[float]          = None
    pcr_at_signal:     Optional[float]          = None

    # Indicator snapshot
    indicators:        dict = field(default_factory=dict)

    # Notes
    rationale:         str = ""
    tags:              list[str] = field(default_factory=list)
    expiry_date:       Optional[date] = None

    # Options specifics
    strike_price:      Optional[float] = None
    option_type:       Optional[str]   = None
    days_to_expiry:    Optional[int]   = None
    iv:                Optional[float] = None
    delta:             Optional[float] = None
    theta:             Optional[float] = None


# ════════════════════════════════════════════════════════════════
#  SIGNAL GENERATOR
# ════════════════════════════════════════════════════════════════

class SignalGenerator:
    """
    Generates fully-specified trading signals for all instrument types.
    Every option/futures signal includes a programmatic hedge specification.
    """

    MIN_SCORE_TO_SIGNAL   = 4   # minimum weighted score before generating
    ATR_SL_MULTIPLIER     = 1.5
    ATR_TARGET_MULTIPLIER = 3.0  # 2:1 R:R minimum

    def __init__(self, context: SentimentContext,
                 account_balance: float = 500_000):
        self.ctx             = context
        self.account_balance = account_balance

    # ── MASTER SCORER ────────────────────────────────────────────
    def compute_signal_score(self, df: pd.DataFrame) -> tuple[int, dict]:
        """
        Score from -10 to +10 using 10 independent signals.
        Returns (score, individual_signals_dict).
        """
        if df is None or len(df) < 30:
            return 0, {}

        r   = df.iloc[-1]    # most recent bar
        p   = df.iloc[-2]    # previous bar
        pp  = df.iloc[-3] if len(df) >= 3 else p
        score   = 0
        signals = {}

        # 1. EMA Cross (weight: 2)
        if r["ema_9"] > r["ema_21"] and p["ema_9"] <= p["ema_21"]:
            score += 2; signals["ema_cross"] = "BULLISH_CROSS"
        elif r["ema_9"] < r["ema_21"] and p["ema_9"] >= p["ema_21"]:
            score -= 2; signals["ema_cross"] = "BEARISH_CROSS"
        elif r["ema_9"] > r["ema_21"]:
            score += 1; signals["ema_cross"] = "ABOVE_EMA"
        else:
            score -= 1; signals["ema_cross"] = "BELOW_EMA"

        # 2. RSI (weight: 1)
        rsi = r.get("rsi_14", 50)
        if rsi < 30:
            score += 2; signals["rsi"] = f"OVERSOLD({rsi:.0f})"
        elif rsi > 70:
            score -= 2; signals["rsi"] = f"OVERBOUGHT({rsi:.0f})"
        elif rsi < 45:
            score -= 1; signals["rsi"] = f"WEAK({rsi:.0f})"
        elif rsi > 55:
            score += 1; signals["rsi"] = f"STRONG({rsi:.0f})"
        else:
            signals["rsi"] = f"NEUTRAL({rsi:.0f})"

        # 3. MACD Histogram (weight: 2)
        hist = r.get("macd_hist", 0) or 0
        prev_hist = p.get("macd_hist", 0) or 0
        if hist > 0 and prev_hist <= 0:
            score += 2; signals["macd"] = "BULLISH_CROSS"
        elif hist < 0 and prev_hist >= 0:
            score -= 2; signals["macd"] = "BEARISH_CROSS"
        elif hist > 0 and hist > prev_hist:
            score += 1; signals["macd"] = "INCREASING"
        elif hist < 0 and hist < prev_hist:
            score -= 1; signals["macd"] = "DECREASING"

        # 4. VWAP Position (weight: 1)
        vwap  = r.get("vwap", 0)
        close = r["close"]
        if vwap:
            dev_pct = (close - vwap) / vwap * 100
            if dev_pct > 0.3:
                score += 1; signals["vwap"] = "ABOVE_VWAP"
            elif dev_pct < -0.3:
                score -= 1; signals["vwap"] = "BELOW_VWAP"
            else:
                signals["vwap"] = "AT_VWAP"

        # 5. Supertrend (weight: 1)
        st_dir = r.get("supertrend_dir", 0)
        if st_dir == 1:
            score += 1; signals["supertrend"] = "UP"
        elif st_dir == -1:
            score -= 1; signals["supertrend"] = "DOWN"

        # 6. ADX Strength (weight: 1, only confirms trend signals)
        adx = r.get("adx_14", 0) or 0
        if adx > 25:
            signals["adx"] = f"TRENDING({adx:.0f})"
            score = int(score * 1.1)   # amplify signal in trending market
        elif adx < 15:
            signals["adx"] = f"RANGING({adx:.0f})"
            score = int(score * 0.8)   # dampen in choppy market

        # 7. Volume Confirmation (weight: 1)
        vol_ratio = r.get("volume_ratio", 1.0) or 1.0
        if vol_ratio > 1.5:
            if score > 0:
                score += 1; signals["volume"] = f"HIGH_BULLISH({vol_ratio:.1f}x)"
            else:
                score -= 1; signals["volume"] = f"HIGH_BEARISH({vol_ratio:.1f}x)"
        else:
            signals["volume"] = f"NORMAL({vol_ratio:.1f}x)"

        # 8. Bollinger Band Position (weight: 1)
        bb_pct = r.get("bb_pct")
        if bb_pct is not None:
            if bb_pct < 0.1:
                score += 1; signals["bbands"] = "LOWER_TOUCH"
            elif bb_pct > 0.9:
                score -= 1; signals["bbands"] = "UPPER_TOUCH"
            elif bb_pct < 0.3:
                signals["bbands"] = "LOWER_ZONE"
            elif bb_pct > 0.7:
                signals["bbands"] = "UPPER_ZONE"
            else:
                signals["bbands"] = "MID_ZONE"

        # 9. Sentiment overlay (weight: 1)
        sent = self.ctx.composite_score
        if sent > 0.4 and score > 0:
            score += 1; signals["sentiment"] = "ALIGNED_BULLISH"
        elif sent < -0.4 and score < 0:
            score -= 1; signals["sentiment"] = "ALIGNED_BEARISH"
        elif abs(sent) < 0.15:
            signals["sentiment"] = "NEUTRAL"
        elif (sent > 0 and score < 0) or (sent < 0 and score > 0):
            score = int(score * 0.7)   # reduce score when sentiment diverges
            signals["sentiment"] = "DIVERGENCE"

        # Clamp
        score = max(-10, min(10, score))
        return score, signals

    # ── EQUITY SIGNAL ─────────────────────────────────────────────
    def generate_equity_signal(self, symbol: str, df: pd.DataFrame,
                                call_type: str = "INTRADAY") -> Optional[Signal]:
        score, signals = self.compute_signal_score(df)
        if abs(score) < self.MIN_SCORE_TO_SIGNAL:
            return None

        direction = SignalDirection.LONG if score > 0 else SignalDirection.SHORT
        if direction == SignalDirection.LONG  and not self.ctx.allow_long:  return None
        if direction == SignalDirection.SHORT and not self.ctx.allow_short: return None

        r     = df.iloc[-1]
        entry = r["close"]
        atr   = r.get("atr_14", entry * 0.01)

        sl, t1, t2 = self._compute_sl_targets(entry, direction, atr, df)

        # Hedge for swing/positional equity
        if call_type in ("SWING", "POSITIONAL"):
            hedge = self._hedge_equity(symbol, entry, direction, score)
        else:
            hedge = HedgeSpec(strategy=HedgeStrategy.NONE,
                              description="No hedge for intraday equity")

        trailing = self._trailing_sl_equity(atr, call_type)
        confidence = self._score_to_confidence(abs(score))

        return Signal(
            instrument_type="STOCK", symbol=symbol,
            direction=direction, confidence=confidence,
            signal_score=score, signal_time=datetime.now(),
            entry_price_low=round(entry * 0.998, 2),
            entry_price_high=round(entry * 1.002, 2),
            entry_order_type="LIMIT", entry_trigger=None,
            target_1=t1, target_2=t2, target_3=None,
            target_pct_1=round(abs(t1-entry)/entry*100, 2),
            target_pct_2=round(abs(t2-entry)/entry*100, 2) if t2 else None,
            stop_loss=sl, sl_pct=round(abs(entry-sl)/entry*100, 2),
            risk_reward_1=round(abs(t1-entry)/abs(entry-sl), 2),
            risk_reward_2=round(abs(t2-entry)/abs(entry-sl), 2) if t2 else None,
            trailing_sl=trailing, hedge=hedge,
            suggested_qty=self._position_size_qty(entry, sl),
            position_size_inr=round(self._position_size_qty(entry, sl) * entry, 2),
            regime=self.ctx.regime, sentiment=self.ctx.sentiment,
            sentiment_score=self.ctx.composite_score,
            vix_at_signal=self.ctx.india_vix,
            indicators=self._extract_indicators(r, signals),
            rationale=self._equity_rationale(symbol, direction, score, signals),
            tags=[call_type, direction.value, confidence.value],
        )

    # ── NIFTY FUTURES SIGNAL ──────────────────────────────────────
    def generate_nifty_future_signal(self, df: pd.DataFrame,
                                      spot: float, contract: str,
                                      expiry: date,
                                      lot_size: int = 75) -> Optional[Signal]:
        score, signals = self.compute_signal_score(df)
        if abs(score) < self.MIN_SCORE_TO_SIGNAL + 1:  # stricter for futures
            return None

        direction = SignalDirection.LONG if score > 0 else SignalDirection.SHORT
        if direction == SignalDirection.LONG  and not self.ctx.allow_long:  return None
        if direction == SignalDirection.SHORT and not self.ctx.allow_short: return None

        r   = df.iloc[-1]
        atr = r.get("atr_14", spot * 0.008)
        entry = spot

        sl, t1, t2 = self._compute_sl_targets(entry, direction, atr, df, atr_mult_sl=1.2)
        points_at_risk = abs(entry - sl)
        lots = min(3, max(1, int(self.account_balance * 0.02 / (points_at_risk * lot_size))))

        hedge = self._hedge_nifty_future(spot, direction, self.ctx.india_vix or 18, expiry)
        trailing = self._trailing_sl_futures(atr)
        confidence = self._score_to_confidence(abs(score))

        return Signal(
            instrument_type="NIFTY_FUT", symbol="NIFTY",
            direction=direction, confidence=confidence,
            signal_score=score, signal_time=datetime.now(),
            entry_price_low=round(entry - 5, 2),
            entry_price_high=round(entry + 5, 2),
            entry_order_type="MARKET", entry_trigger=None,
            target_1=t1, target_2=t2, target_3=None,
            target_pct_1=round(abs(t1-entry)/entry*100, 2),
            target_pct_2=round(abs(t2-entry)/entry*100, 2) if t2 else None,
            stop_loss=sl, sl_pct=round(abs(entry-sl)/entry*100, 2),
            risk_reward_1=round(abs(t1-entry)/abs(entry-sl), 2),
            risk_reward_2=None,
            trailing_sl=trailing, hedge=hedge,
            suggested_lots=lots,
            position_size_inr=round(lots * lot_size * entry, 2),
            regime=self.ctx.regime, sentiment=self.ctx.sentiment,
            sentiment_score=self.ctx.composite_score,
            vix_at_signal=self.ctx.india_vix,
            pcr_at_signal=self.ctx.nifty_pcr,
            indicators=self._extract_indicators(r, signals),
            rationale=self._futures_rationale("NIFTY", direction, score, signals),
            tags=["NIFTY_FUT", direction.value, confidence.value],
            expiry_date=expiry,
        )

    # ── NIFTY OPTIONS SIGNAL ──────────────────────────────────────
    def generate_nifty_option_signal(self, df: pd.DataFrame, spot: float,
                                      option_type: str,         # "CE" or "PE"
                                      strike: float, premium: float,
                                      expiry: date, dte: int,
                                      iv: float, lot_size: int = 75,
                                      greeks: dict = None) -> Optional[Signal]:
        score, signals = self.compute_signal_score(df)

        # Options need stronger confirmation
        direction_by_ot = SignalDirection.LONG if option_type == "CE" else SignalDirection.SHORT
        adjusted_score = score if option_type == "CE" else -score  # normalize to underlying direction
        if abs(adjusted_score) < self.MIN_SCORE_TO_SIGNAL + 1: return None
        if option_type == "CE" and not self.ctx.allow_long:  return None
        if option_type == "PE" and not self.ctx.allow_short: return None
        if not self.ctx.allow_options_buy: return None

        # Premium-based entry/SL/target
        entry_low  = round(premium * 0.97, 2)
        entry_high = round(premium * 1.03, 2)
        sl_premium = round(premium * 0.50, 2)   # 50% premium SL
        t1_premium = round(premium * 1.80, 2)   # 80% gain target 1
        t2_premium = round(premium * 2.50, 2)   # 150% gain target 2

        # Time decay SL rule for options
        time_stop = (f"Exit if premium decays below ₹{sl_premium:.2f} OR "
                     f"if {min(30, dte // 3)} days passed with no movement OR "
                     f"15 min before market close on expiry day")

        # MANDATORY hedge for every option trade
        hedge = self._hedge_nifty_option(spot, strike, option_type, premium,
                                          expiry, dte, self.ctx.india_vix or 18)

        trailing = TrailingSLRule(
            method="PERCENTAGE",
            trigger_profit_pct=50.0,
            trail_distance_pct=25.0,
            human_readable=(f"After 50% gain in premium, trail SL at 25% below peak. "
                            f"Hard exit if premium < ₹{sl_premium:.2f}. "
                            f"Time stop: {time_stop}")
        )

        confidence = self._score_to_confidence(abs(score))
        moneyness  = self._moneyness(spot, strike, option_type)
        lots = max(1, min(5, int(self.account_balance * 0.02 / (sl_premium * lot_size + 1))))

        return Signal(
            instrument_type="NIFTY_OPT", symbol=f"NIFTY {strike}{option_type}",
            direction=SignalDirection.LONG,    # buying an option = LONG the contract
            confidence=confidence, signal_score=score, signal_time=datetime.now(),
            entry_price_low=entry_low, entry_price_high=entry_high,
            entry_order_type="LIMIT", entry_trigger=None,
            target_1=t1_premium, target_2=t2_premium, target_3=None,
            target_pct_1=round((t1_premium-premium)/premium*100, 1),
            target_pct_2=round((t2_premium-premium)/premium*100, 1),
            stop_loss=sl_premium,
            sl_pct=round((premium-sl_premium)/premium*100, 1),
            risk_reward_1=round((t1_premium-premium)/(premium-sl_premium), 2),
            risk_reward_2=round((t2_premium-premium)/(premium-sl_premium), 2),
            trailing_sl=trailing, hedge=hedge,
            suggested_lots=lots,
            position_size_inr=round(lots * lot_size * premium, 2),
            regime=self.ctx.regime, sentiment=self.ctx.sentiment,
            sentiment_score=self.ctx.composite_score,
            vix_at_signal=self.ctx.india_vix, pcr_at_signal=self.ctx.nifty_pcr,
            indicators=self._extract_indicators(df.iloc[-1], signals),
            rationale=self._options_rationale("NIFTY", strike, option_type,
                                               premium, score, hedge, dte),
            tags=["NIFTY_OPT", option_type, moneyness, confidence.value],
            expiry_date=expiry, strike_price=strike, option_type=option_type,
            days_to_expiry=dte, iv=iv,
            delta=greeks.get("delta") if greeks else None,
            theta=greeks.get("theta") if greeks else None,
        )

    # ── SENSEX FUTURES SIGNAL ─────────────────────────────────────
    def generate_sensex_future_signal(self, df: pd.DataFrame, spot: float,
                                       contract: str, expiry: date,
                                       lot_size: int = 10) -> Optional[Signal]:
        score, signals = self.compute_signal_score(df)
        if abs(score) < self.MIN_SCORE_TO_SIGNAL + 1: return None
        direction = SignalDirection.LONG if score > 0 else SignalDirection.SHORT
        if direction == SignalDirection.LONG  and not self.ctx.allow_long:  return None
        if direction == SignalDirection.SHORT and not self.ctx.allow_short: return None

        r   = df.iloc[-1]
        atr = r.get("atr_14", spot * 0.008)
        sl, t1, t2 = self._compute_sl_targets(spot, direction, atr, df, 1.2)
        hedge = self._hedge_sensex_future(spot, direction, expiry)
        trailing = self._trailing_sl_futures(atr)
        confidence = self._score_to_confidence(abs(score))
        lots = max(1, min(3, int(self.account_balance * 0.015 / (abs(spot-sl)*lot_size+1))))

        return Signal(
            instrument_type="SENSEX_FUT", symbol="SENSEX",
            direction=direction, confidence=confidence,
            signal_score=score, signal_time=datetime.now(),
            entry_price_low=round(spot - 10, 2),
            entry_price_high=round(spot + 10, 2),
            entry_order_type="MARKET", entry_trigger=None,
            target_1=t1, target_2=t2, target_3=None,
            target_pct_1=round(abs(t1-spot)/spot*100, 2),
            target_pct_2=round(abs(t2-spot)/spot*100, 2) if t2 else None,
            stop_loss=sl, sl_pct=round(abs(spot-sl)/spot*100, 2),
            risk_reward_1=round(abs(t1-spot)/abs(spot-sl), 2),
            risk_reward_2=None, trailing_sl=trailing, hedge=hedge,
            suggested_lots=lots, position_size_inr=round(lots*lot_size*spot,2),
            regime=self.ctx.regime, sentiment=self.ctx.sentiment,
            sentiment_score=self.ctx.composite_score,
            vix_at_signal=self.ctx.india_vix,
            indicators=self._extract_indicators(r, signals),
            rationale=self._futures_rationale("SENSEX", direction, score, signals),
            tags=["SENSEX_FUT", direction.value, confidence.value],
            expiry_date=expiry,
        )

    # ════════════════════════════════════════════════════════════
    #  HEDGE SPECIFICATION BUILDERS (MANDATORY for all F&O)
    # ════════════════════════════════════════════════════════════

    def _hedge_equity(self, symbol: str, entry: float,
                       direction: SignalDirection, score: int) -> HedgeSpec:
        """For swing/positional equity positions, suggest a protective option."""
        if direction == SignalDirection.LONG:
            strike = self._round_to_step(entry * 0.97, 5)
            return HedgeSpec(
                strategy=HedgeStrategy.PROTECTIVE_PUT,
                description=(f"Buy 1 lot {symbol} {strike} PE "
                             f"(expiry: nearest monthly). "
                             f"Cost ≈ 1–2% of position value. "
                             f"Protects against gap-down > 3%."),
                hedge_strike=strike,
                hedge_option_type="PE",
                hedge_lots=1,
                rationale="Protective put caps downside on swing position"
            )
        else:
            strike = self._round_to_step(entry * 1.03, 5)
            return HedgeSpec(
                strategy=HedgeStrategy.COVERED_CALL,
                description=(f"Sell 1 lot {symbol} {strike} CE "
                             f"to offset short selling risk. "
                             f"Use only if holding underlying."),
                hedge_strike=strike,
                hedge_option_type="CE",
                hedge_lots=1,
                rationale="Covered call reduces cost basis on long stock"
            )

    def _hedge_nifty_future(self, spot: float, direction: SignalDirection,
                              vix: float, expiry: date) -> HedgeSpec:
        """
        For Nifty futures, generate a spread hedge based on sentiment.
        LONG futures → buy ATM-50 PE as insurance
        SHORT futures → buy ATM+50 CE as insurance
        In high-vol: use iron condor overlay
        """
        step = 50
        atm  = self._round_to_step(spot, step)

        if vix > 22:
            # High vol: hedge with OTM options on both sides → limited cost
            call_strike = atm + 100
            put_strike  = atm - 100
            return HedgeSpec(
                strategy=HedgeStrategy.IRON_CONDOR,
                description=(f"Sell {atm+50}CE + Buy {call_strike}CE "
                             f"| Sell {atm-50}PE + Buy {put_strike}PE. "
                             f"Net credit ≈ ₹30–50 per lot. "
                             f"Protects futures position in range {put_strike}–{call_strike}."),
                hedge_strike=atm,
                hedge_lots=1,
                spread_type="CREDIT",
                max_loss_inr=round((100 - 40) * 75, 2),    # max loss if range breaks
                max_profit_inr=round(40 * 75, 2),           # net credit × lot size
                breakeven_1=put_strike,
                breakeven_2=call_strike,
                rationale=f"VIX at {vix:.1f} — iron condor captures premium & hedges futures"
            )

        if direction == SignalDirection.LONG:
            hedge_strike = atm - step
            return HedgeSpec(
                strategy=HedgeStrategy.BULL_PUT_SPREAD,
                description=(f"Buy {hedge_strike}PE + Sell {hedge_strike-50}PE "
                             f"on nearest weekly expiry. "
                             f"Net cost ≈ ₹15–25 per lot. "
                             f"Caps downside to {hedge_strike} on long futures."),
                hedge_strike=hedge_strike,
                hedge_option_type="PE",
                hedge_lots=1,
                spread_type="DEBIT",
                max_loss_inr=round(25 * 75, 2),
                max_profit_inr=round((50 - 25) * 75, 2),
                breakeven_1=hedge_strike - 25,
                rationale="Bull put spread costs less than a naked put, defined risk"
            )
        else:
            hedge_strike = atm + step
            return HedgeSpec(
                strategy=HedgeStrategy.BEAR_CALL_SPREAD,
                description=(f"Buy {hedge_strike}CE + Sell {hedge_strike+50}CE "
                             f"on nearest weekly expiry. "
                             f"Net cost ≈ ₹15–25 per lot. "
                             f"Caps upside risk on short futures."),
                hedge_strike=hedge_strike,
                hedge_option_type="CE",
                hedge_lots=1,
                spread_type="DEBIT",
                max_loss_inr=round(25 * 75, 2),
                max_profit_inr=round((50 - 25) * 75, 2),
                breakeven_2=hedge_strike + 25,
                rationale="Bear call spread limits loss if short futures move against"
            )

    def _hedge_nifty_option(self, spot: float, strike: float, opt_type: str,
                             premium: float, expiry: date, dte: int,
                             vix: float) -> HedgeSpec:
        """
        For every option buy, mandatory spread to convert to defined-risk trade.
        CE buy → Bear Call Spread or Bull Call Spread depending on strike
        PE buy → Bull Put Spread or Bear Put Spread
        """
        step = 50
        if opt_type == "CE":
            # Convert naked CE buy → Bull Call Spread by selling further OTM CE
            sell_strike = strike + step * 2
            max_profit  = (sell_strike - strike - premium) * 75
            max_loss    = premium * 75
            return HedgeSpec(
                strategy=HedgeStrategy.BULL_PUT_SPREAD,
                description=(f"Convert to Bull Call Spread: "
                             f"Buy {strike}CE @ ₹{premium:.2f}, "
                             f"Sell {sell_strike}CE (reduces cost, caps profit at {sell_strike}). "
                             f"Net Debit ≈ ₹{premium - 5:.0f}. "
                             f"Max Profit: ₹{max_profit:,.0f} | Max Loss: ₹{max_loss:,.0f}"),
                hedge_strike=sell_strike,
                hedge_option_type="CE",
                hedge_lots=1,
                spread_type="DEBIT",
                max_loss_inr=max_loss,
                max_profit_inr=max_profit,
                breakeven_1=strike + premium - 5,
                rationale="Spread reduces capital at risk by ~30%, maintains upside"
            )
        else:
            sell_strike = strike - step * 2
            max_profit  = (strike - sell_strike - premium) * 75
            max_loss    = premium * 75
            return HedgeSpec(
                strategy=HedgeStrategy.BEAR_CALL_SPREAD,
                description=(f"Convert to Bear Put Spread: "
                             f"Buy {strike}PE @ ₹{premium:.2f}, "
                             f"Sell {sell_strike}PE (reduces cost, caps profit at {sell_strike}). "
                             f"Net Debit ≈ ₹{premium - 5:.0f}. "
                             f"Max Profit: ₹{max_profit:,.0f} | Max Loss: ₹{max_loss:,.0f}"),
                hedge_strike=sell_strike,
                hedge_option_type="PE",
                hedge_lots=1,
                spread_type="DEBIT",
                max_loss_inr=max_loss,
                max_profit_inr=max_profit,
                breakeven_2=strike - premium + 5,
                rationale="Bear put spread reduces theta decay impact"
            )

    def _hedge_sensex_future(self, spot: float, direction: SignalDirection,
                              expiry: date) -> HedgeSpec:
        step = 100  # Sensex moves in 100-point strikes
        atm  = self._round_to_step(spot, step)
        if direction == SignalDirection.LONG:
            hedge_strike = atm - step
            return HedgeSpec(
                strategy=HedgeStrategy.PROTECTIVE_PUT,
                description=(f"Buy Sensex {hedge_strike}PE (1 lot = 10 shares). "
                             f"Cost ≈ ₹200–400 per lot. "
                             f"Protects long futures below {hedge_strike}."),
                hedge_strike=hedge_strike, hedge_option_type="PE", hedge_lots=1,
                max_loss_inr=round(400 * 10, 2),
                rationale="Protective put for Sensex long futures overnight risk"
            )
        else:
            hedge_strike = atm + step
            return HedgeSpec(
                strategy=HedgeStrategy.COLLAR,
                description=(f"Buy Sensex {hedge_strike}CE (1 lot). "
                             f"Cost ≈ ₹200–400 per lot. "
                             f"Caps loss on short futures above {hedge_strike}."),
                hedge_strike=hedge_strike, hedge_option_type="CE", hedge_lots=1,
                max_loss_inr=round(400 * 10, 2),
                rationale="CE hedge caps upside risk on Sensex short futures"
            )

    # ════════════════════════════════════════════════════════════
    #  UTILITY HELPERS
    # ════════════════════════════════════════════════════════════

    def _compute_sl_targets(self, entry: float, direction: SignalDirection,
                             atr: float, df: pd.DataFrame,
                             atr_mult_sl: float = None) -> tuple[float, float, float]:
        mult_sl = atr_mult_sl or self.ATR_SL_MULTIPLIER
        sl_dist = atr * mult_sl
        t1_dist = atr * self.ATR_TARGET_MULTIPLIER
        t2_dist = atr * self.ATR_TARGET_MULTIPLIER * 1.8

        # Also respect recent swing high/low as SL anchor
        r = df.iloc[-1]
        recent_low  = df["low"].rolling(10).min().iloc[-1]
        recent_high = df["high"].rolling(10).max().iloc[-1]

        if direction == SignalDirection.LONG:
            sl_atr    = round(entry - sl_dist, 2)
            sl_swing  = round(recent_low * 0.998, 2)
            sl        = max(sl_atr, sl_swing)   # use tighter of the two
            t1        = round(entry + t1_dist, 2)
            t2        = round(entry + t2_dist, 2)
        else:
            sl_atr    = round(entry + sl_dist, 2)
            sl_swing  = round(recent_high * 1.002, 2)
            sl        = min(sl_atr, sl_swing)
            t1        = round(entry - t1_dist, 2)
            t2        = round(entry - t2_dist, 2)

        return sl, t1, t2

    def _trailing_sl_equity(self, atr: float, call_type: str) -> TrailingSLRule:
        if call_type == "INTRADAY":
            return TrailingSLRule(
                method="ATR", trigger_profit_pct=1.0,
                trail_atr_mult=1.0,
                time_exit_minutes_before_close=15,
                human_readable=(f"After 1% profit, trail SL by 1×ATR({atr:.2f}). "
                               f"Mandatory exit 15 min before market close.")
            )
        else:
            return TrailingSLRule(
                method="PERCENTAGE", trigger_profit_pct=5.0,
                trail_distance_pct=3.0,
                human_readable=(f"After 5% profit, trail SL 3% below current high. "
                               f"Review weekly. Widen SL in high-volatility markets.")
            )

    def _trailing_sl_futures(self, atr: float) -> TrailingSLRule:
        return TrailingSLRule(
            method="ATR", trigger_profit_pct=1.5,
            trail_atr_mult=1.0,
            time_exit_minutes_before_close=5,
            human_readable=(f"After 1.5% futures profit, trail SL at 1×ATR({atr:.2f}) "
                           f"below running high (LONG) or above running low (SHORT). "
                           f"Hard exit 5 min before market close for intraday. "
                           f"For overnight: review hedge every morning pre-market.")
        )

    def _position_size_qty(self, entry: float, sl: float) -> int:
        risk_amount = self.account_balance * 0.015  # risk 1.5% of capital
        sl_distance = abs(entry - sl)
        if sl_distance == 0:
            return 0
        qty = int(risk_amount / sl_distance)
        max_qty_by_capital = int(self.account_balance * 0.20 / entry)
        return max(1, min(qty, max_qty_by_capital))

    @staticmethod
    def _score_to_confidence(abs_score: int) -> SignalConfidence:
        if abs_score >= 8: return SignalConfidence.A_PLUS
        if abs_score >= 7: return SignalConfidence.A
        if abs_score >= 6: return SignalConfidence.B_PLUS
        if abs_score >= 5: return SignalConfidence.B
        return SignalConfidence.C

    @staticmethod
    def _round_to_step(value: float, step: float) -> float:
        return round(round(value / step) * step, 2)

    @staticmethod
    def _moneyness(spot: float, strike: float, opt_type: str) -> str:
        pct = (spot - strike) / spot * 100
        if opt_type == "CE":
            if pct >  3: return "DITM"
            if pct >  1: return "ITM"
            if pct > -1: return "ATM"
            if pct > -3: return "OTM"
            return "DOTM"
        else:
            if pct < -3: return "DITM"
            if pct < -1: return "ITM"
            if pct <  1: return "ATM"
            if pct <  3: return "OTM"
            return "DOTM"

    @staticmethod
    def _extract_indicators(r: pd.Series, signals: dict) -> dict:
        out = {}
        for key in ["rsi_14","macd_hist","ema_9","ema_21","atr_14","adx_14",
                    "volume_ratio","vwap","supertrend_dir","bb_pct"]:
            val = r.get(key)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                out[key] = round(float(val), 4)
        out["signals"] = signals
        return out

    def _equity_rationale(self, symbol, direction, score, signals) -> str:
        top = sorted(signals.items(), key=lambda x: str(x[1]))[:4]
        sig_str = ", ".join(f"{k}={v}" for k, v in top)
        return (f"{symbol} {direction.value} signal (score={score:+d}). "
                f"Key signals: {sig_str}. "
                f"Market: {self.ctx.regime.value}, Sentiment: {self.ctx.sentiment.value} "
                f"({self.ctx.composite_score:+.3f}).")

    def _futures_rationale(self, index, direction, score, signals) -> str:
        return (f"{index} Futures {direction.value} (score={score:+d}). "
                f"VIX={self.ctx.india_vix or 'N/A'}, PCR={self.ctx.nifty_pcr or 'N/A'}. "
                f"Regime: {self.ctx.regime.value}.")

    def _options_rationale(self, index, strike, opt_type, premium, score, hedge, dte) -> str:
        return (f"{index} {strike}{opt_type} @ ₹{premium:.2f} (DTE={dte}). "
                f"Signal score={score:+d}. "
                f"Hedge: {hedge.strategy.value} — {hedge.description[:80]}...")
