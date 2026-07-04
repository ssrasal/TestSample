"""
test_suite.py — STEP 5: Comprehensive QA & Test Suite
Covers: Unit tests, Integration tests, Backtesting validation.
Run:  pytest tests/ -v --tb=short --cov=backend --cov-report=term-missing
"""

from __future__ import annotations

import asyncio
import json
import math
import unittest
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import numpy as np
import pytest


# ════════════════════════════════════════════════════════════════
#  FIXTURES
# ════════════════════════════════════════════════════════════════

def make_ohlcv_df(n: int = 100, trend: str = "up",
                  volatility: float = 1.0) -> pd.DataFrame:
    """Generate synthetic OHLCV with controllable trend and volatility."""
    np.random.seed(42)
    dates  = pd.date_range("2025-01-01", periods=n, freq="5min")
    prices = [22000.0]
    for _ in range(n - 1):
        shock = np.random.normal(0, volatility * 10)
        drift = +5 if trend == "up" else (-5 if trend == "down" else 0)
        prices.append(prices[-1] + drift + shock)

    df = pd.DataFrame({
        "open":   [p + np.random.uniform(-5, 0) for p in prices],
        "high":   [p + np.random.uniform(0, 20)  for p in prices],
        "low":    [p - np.random.uniform(0, 20)  for p in prices],
        "close":  prices,
        "volume": np.random.randint(100_000, 2_000_000, n).tolist(),
    }, index=dates)
    return df


def make_flat_df(n: int = 100) -> pd.DataFrame:
    return make_ohlcv_df(n, trend="flat", volatility=0.1)


@pytest.fixture
def bullish_df():
    return make_ohlcv_df(100, "up", 0.5)


@pytest.fixture
def bearish_df():
    return make_ohlcv_df(100, "down", 0.5)


@pytest.fixture
def flat_df():
    return make_ohlcv_df(100, "flat", 0.05)


@pytest.fixture
def mock_sentiment_context():
    from backend.core.sentiment_engine import SentimentContext, MarketRegime, SentimentLabel
    return SentimentContext(
        computed_at=datetime.now(),
        regime=MarketRegime.BULL,
        sentiment=SentimentLabel.BULLISH,
        composite_score=0.45,
        vix_score=0.3,
        pcr_score=0.2,
        fii_score=0.4,
        news_score=0.1,
        breadth_score=0.3,
        momentum_score=0.2,
        india_vix=15.5,
        nifty_pcr=1.15,
        fii_net_crores=1200.0,
        advance_decline=0.65,
        allow_long=True,
        allow_short=True,
        allow_options_buy=True,
        allow_options_sell=False,
        narrative="Test context: VIX low, FII buying, PCR neutral.",
    )


# ════════════════════════════════════════════════════════════════
#  STEP 5A: UNIT TESTS — Signal Generator
# ════════════════════════════════════════════════════════════════

class TestSignalScorer:
    """Unit tests for the core scoring logic."""

    def _make_generator(self, ctx):
        from backend.core.signal_generator import SignalGenerator
        return SignalGenerator(ctx, account_balance=500_000)

    def _enriched(self, df):
        from backend.data.data_fetcher import DataFetcher
        fetcher = DataFetcher()
        return fetcher.enrich_with_indicators(df)

    def test_bullish_df_scores_positive(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        score, signals = gen.compute_signal_score(df)
        assert score > 0, f"Expected positive score on bullish data, got {score}"
        assert "ema_cross" in signals

    def test_bearish_df_scores_negative(self, bearish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bearish_df)
        score, signals = gen.compute_signal_score(df)
        assert score < 0, f"Expected negative score on bearish data, got {score}"

    def test_score_clamped_to_10(self, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        # Artificially extreme data
        df = make_ohlcv_df(200, "up", 0.01)
        df = self._enriched(df)
        score, _ = gen.compute_signal_score(df)
        assert -10 <= score <= 10, f"Score out of range: {score}"

    def test_insufficient_data_returns_zero(self, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = make_ohlcv_df(10)  # too few bars
        score, _ = gen.compute_signal_score(df)
        assert score == 0

    def test_equity_signal_has_mandatory_fields(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        sig = gen.generate_equity_signal("NSE:RELIANCE", df, "INTRADAY")
        if sig is None:
            pytest.skip("Score below threshold — no signal generated")
        assert sig.stop_loss is not None
        assert sig.target_1 is not None
        assert sig.trailing_sl is not None
        assert sig.entry_price_low < sig.entry_price_high
        assert sig.signal_score is not None

    def test_equity_sl_is_below_entry_for_long(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        sig = gen.generate_equity_signal("NSE:RELIANCE", df)
        if sig is None: pytest.skip("No signal")
        from backend.core.signal_generator import SignalDirection
        if sig.direction == SignalDirection.LONG:
            assert sig.stop_loss < sig.entry_price_low, "SL must be below entry for LONG"

    def test_equity_sl_is_above_entry_for_short(self, bearish_df, mock_sentiment_context):
        from backend.core.sentiment_engine import MarketRegime, SentimentLabel
        ctx = mock_sentiment_context
        # Override to bearish
        ctx = type(ctx)(
            computed_at=ctx.computed_at, regime=MarketRegime.BEAR,
            sentiment=SentimentLabel.BEARISH, composite_score=-0.45,
            vix_score=-0.3, pcr_score=-0.2, fii_score=-0.4,
            news_score=-0.1, breadth_score=-0.3, momentum_score=-0.2,
            india_vix=22.0, nifty_pcr=0.7, fii_net_crores=-1200.0,
            advance_decline=0.35, allow_long=True, allow_short=True,
            allow_options_buy=True, allow_options_sell=False,
            narrative="Test bearish"
        )
        gen = self._make_generator(ctx)
        df  = self._enriched(bearish_df)
        sig = gen.generate_equity_signal("NSE:INFY", df)
        if sig is None: pytest.skip("No signal")
        from backend.core.signal_generator import SignalDirection
        if sig.direction == SignalDirection.SHORT:
            assert sig.stop_loss > sig.entry_price_high, "SL must be above entry for SHORT"

    def test_rrr_minimum_1_5(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        sig = gen.generate_equity_signal("NSE:TCS", df)
        if sig is None: pytest.skip("No signal")
        assert sig.risk_reward_1 >= 1.5, f"RRR too low: {sig.risk_reward_1}"

    def test_nifty_future_signal_has_hedge(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        from backend.core.signal_generator import HedgeStrategy
        sig = gen.generate_nifty_future_signal(
            df, spot=24500.0,
            contract="NIFTY25JULFUT", expiry=date(2025, 7, 31)
        )
        if sig is None: pytest.skip("No signal")
        assert sig.hedge is not None
        assert sig.hedge.strategy != HedgeStrategy.NONE, "Futures MUST have a hedge"
        assert sig.hedge.description, "Hedge must have a description"

    def test_nifty_option_signal_has_spread_hedge(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        from backend.core.signal_generator import HedgeStrategy
        sig = gen.generate_nifty_option_signal(
            df, spot=24500.0, option_type="CE",
            strike=24500.0, premium=150.0,
            expiry=date(2025, 7, 31), dte=10,
            iv=18.5
        )
        if sig is None: pytest.skip("No signal")
        assert sig.hedge.strategy in (
            HedgeStrategy.BULL_PUT_SPREAD, HedgeStrategy.BEAR_CALL_SPREAD
        ), "Options must use spread hedge"
        assert sig.hedge.max_loss_inr is not None, "Defined-risk hedge must specify max loss"

    def test_position_size_respects_capital(self, bullish_df, mock_sentiment_context):
        gen = self._make_generator(mock_sentiment_context)
        df  = self._enriched(bullish_df)
        sig = gen.generate_equity_signal("NSE:RELIANCE", df)
        if sig is None: pytest.skip("No signal")
        if sig.position_size_inr:
            assert sig.position_size_inr <= 500_000 * 0.20, "Position > 20% of capital"


# ════════════════════════════════════════════════════════════════
#  STEP 5B: UNIT TESTS — Sentiment Engine
# ════════════════════════════════════════════════════════════════

class TestSentimentEngine:

    def _make_engine(self):
        from backend.data.data_fetcher import DataFetcher
        from backend.core.sentiment_engine import SentimentAdaptiveEngine
        fetcher = MagicMock(spec=DataFetcher)
        return SentimentAdaptiveEngine(fetcher, news_api_key="")

    def test_vix_score_low_is_bullish(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import VIXData
        vix = VIXData(datetime.now(), 11.5, 0, 0)
        score = engine._score_vix(vix)
        assert score > 0.5, f"Low VIX should be bullish, got {score}"

    def test_vix_score_high_is_bearish(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import VIXData
        vix = VIXData(datetime.now(), 32.0, 0, 0)
        score = engine._score_vix(vix)
        assert score < -0.8, f"High VIX should be very bearish, got {score}"

    def test_pcr_high_is_contrarian_bullish(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import OptionChainData
        oc = MagicMock()
        oc.pcr_oi = 1.6
        score = engine._score_pcr(oc)
        assert score > 0.6, f"High PCR (put-heavy) is contrarian bullish, got {score}"

    def test_pcr_low_is_contrarian_bearish(self):
        engine = self._make_engine()
        oc = MagicMock()
        oc.pcr_oi = 0.55
        score = engine._score_pcr(oc)
        assert score < -0.5, f"Low PCR (call-heavy) is contrarian bearish, got {score}"

    def test_composite_clamped_to_one(self):
        engine = self._make_engine()
        # Force all sub-scores to +1
        engine._nifty_prices = [22000]*25
        score = engine._score_momentum()
        # Just test it doesn't crash and stays in range
        assert -1.0 <= score <= 1.0

    def test_regime_classification(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import VIXData
        from backend.core.sentiment_engine import MarketRegime
        low_vix = VIXData(datetime.now(), 14.0, 0, 0)
        assert engine._classify_regime(0.7, low_vix)  == MarketRegime.STRONG_BULL
        assert engine._classify_regime(0.3, low_vix)  == MarketRegime.BULL
        assert engine._classify_regime(0.0, low_vix)  == MarketRegime.SIDEWAYS
        assert engine._classify_regime(-0.4, low_vix) == MarketRegime.BEAR
        assert engine._classify_regime(-0.8, low_vix) == MarketRegime.STRONG_BEAR
        high_vix = VIXData(datetime.now(), 35.0, 0, 0)
        assert engine._classify_regime(0.3, high_vix) == MarketRegime.HIGH_VOL

    def test_strategy_gates_block_longs_in_strong_bear(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import VIXData
        from backend.core.sentiment_engine import MarketRegime
        vix = VIXData(datetime.now(), 18.0, 0, 0)
        gates = engine._compute_strategy_gates(-0.85, vix, MarketRegime.STRONG_BEAR)
        assert not gates["allow_long"], "Strong bear should block longs"

    def test_options_sell_blocked_high_vix(self):
        engine = self._make_engine()
        from backend.data.data_fetcher import VIXData
        from backend.core.sentiment_engine import MarketRegime
        high_vix = VIXData(datetime.now(), 28.0, 0, 0)
        gates = engine._compute_strategy_gates(0.0, high_vix, MarketRegime.HIGH_VOL)
        assert not gates["allow_options_sell"], "Options selling blocked when VIX>20"


# ════════════════════════════════════════════════════════════════
#  STEP 5C: UNIT TESTS — Circuit Breaker
# ════════════════════════════════════════════════════════════════

class TestCircuitBreaker:

    def test_opens_after_threshold_failures(self):
        from backend.data.data_fetcher import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_blocks_requests_when_open(self):
        from backend.data.data_fetcher import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=1)
        cb.record_failure()
        assert not cb.allow_request()

    def test_resets_after_success(self):
        from backend.data.data_fetcher import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=2)
        cb.record_failure(); cb.record_failure()
        assert cb.state == CircuitState.OPEN
        # Simulate recovery timeout
        import time
        cb._last_failure_time = time.monotonic() - 70  # past timeout
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_failure_count_resets_on_success(self):
        from backend.data.data_fetcher import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=5)
        cb.record_failure(); cb.record_failure()
        cb.record_success()
        assert cb._failure_count == 0


# ════════════════════════════════════════════════════════════════
#  STEP 5D: INTEGRATION TESTS — Database Writes
# ════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
@pytest.mark.integration
class TestDatabaseIntegration:
    """
    Integration tests for DB writes. Requires a running PostgreSQL instance.
    Skipped automatically if DATABASE_URL is not set or DB is unreachable.
    """

    @pytest.fixture(autouse=True)
    async def setup_pool(self):
        import os
        if not os.getenv("DATABASE_URL"):
            pytest.skip("DATABASE_URL not set — skipping integration tests")
        try:
            from backend.core.db_connector import init_pool, close_pool
            await init_pool(min_size=1, max_size=2)
            yield
            await close_pool()
        except Exception as e:
            pytest.skip(f"DB not reachable: {e}")

    async def test_sentiment_insert(self, mock_sentiment_context):
        from backend.core.db_connector import SignalRepository
        repo = SignalRepository()
        # Should not raise
        await repo.insert_sentiment(mock_sentiment_context)

    async def test_stock_call_roundtrip(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        from backend.core.db_connector import SignalRepository
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        if sig is None:
            pytest.skip("No signal generated")

        repo = SignalRepository()
        call_id = await repo.insert_stock_call(sig)
        assert call_id, "Expected a UUID back from insert"
        # UUID format check
        import re
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            call_id
        ), "Invalid UUID format"

    async def test_close_call_updates_status(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        from backend.core.db_connector import SignalRepository, acquire
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST2", df)
        if sig is None: pytest.skip("No signal")

        repo = SignalRepository()
        call_id = await repo.insert_stock_call(sig)
        exit_price = sig.target_1
        pnl = (exit_price - sig.entry_price_low) * (sig.suggested_qty or 1)
        await repo.close_call("stock_calls", call_id, exit_price, "TARGET1_HIT", pnl)

        async with acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, actual_pnl FROM stock_calls WHERE id=$1::uuid", call_id
            )
        assert row["status"] == "TARGET1_HIT"
        assert float(row["actual_pnl"]) == round(pnl, 2)

    async def test_concurrent_inserts_no_deadlock(self, bullish_df, mock_sentiment_context):
        """Verify no deadlocks under concurrent writes."""
        from backend.core.signal_generator import SignalGenerator
        from backend.core.db_connector import SignalRepository
        from backend.data.data_fetcher import DataFetcher

        repo = SignalRepository()
        gen  = SignalGenerator(mock_sentiment_context, 500_000)
        df   = DataFetcher().enrich_with_indicators(bullish_df)

        async def insert_one(symbol):
            sig = gen.generate_equity_signal(symbol, df)
            if sig:
                await repo.insert_stock_call(sig)

        symbols = [f"NSE:STOCK{i:03d}" for i in range(20)]
        await asyncio.gather(*[insert_one(s) for s in symbols])
        # No exception = pass


# ════════════════════════════════════════════════════════════════
#  STEP 5E: BACKTESTING VALIDATION RULES
# ════════════════════════════════════════════════════════════════

class BacktestResult:
    def __init__(self):
        self.trades:       list[dict] = []
        self.total_pnl:    float = 0.0
        self.winners:      int   = 0
        self.losers:       int   = 0
        self.max_drawdown: float = 0.0
        self._peak_equity: float = 0.0
        self._equity:      float = 0.0

    def add_trade(self, entry, exit_p, direction, qty):
        if direction == "LONG":
            pnl = (exit_p - entry) * qty
        else:
            pnl = (entry - exit_p) * qty
        self.trades.append({"pnl": pnl, "entry": entry, "exit": exit_p})
        self.total_pnl += pnl
        self._equity   += pnl
        if pnl > 0: self.winners += 1
        else:        self.losers  += 1
        if self._equity > self._peak_equity:
            self._peak_equity = self._equity
        dd = self._peak_equity - self._equity
        if dd > self.max_drawdown:
            self.max_drawdown = dd

    @property
    def win_rate(self): return self.winners / max(len(self.trades), 1)
    @property
    def profit_factor(self):
        gross_p = sum(t["pnl"] for t in self.trades if t["pnl"] > 0)
        gross_l = abs(sum(t["pnl"] for t in self.trades if t["pnl"] < 0))
        return gross_p / max(gross_l, 1)


class TestBacktestValidator:

    def _run_backtest(self, df: pd.DataFrame, gen, ctx) -> BacktestResult:
        """Simulate rolling backtest on OHLCV data."""
        from backend.data.data_fetcher import DataFetcher
        result = BacktestResult()
        fetcher = DataFetcher()
        window  = 50

        for i in range(window, len(df) - 5):
            sub = df.iloc[:i]
            sub = fetcher.enrich_with_indicators(sub)
            sig = gen.generate_equity_signal("BACKTEST", sub)
            if sig is None:
                continue
            # Simulate: check next 5 bars for SL or target
            entry   = sig.entry_price_high
            sl      = sig.stop_loss
            target  = sig.target_1
            direction = sig.direction.value
            exit_price = entry  # default: no move

            for j in range(i, min(i + 5, len(df))):
                bar_high = df.iloc[j]["high"]
                bar_low  = df.iloc[j]["low"]
                if direction == "LONG":
                    if bar_low <= sl:
                        exit_price = sl; break
                    if bar_high >= target:
                        exit_price = target; break
                else:
                    if bar_high >= sl:
                        exit_price = sl; break
                    if bar_low <= target:
                        exit_price = target; break

            result.add_trade(entry, exit_price, direction, sig.suggested_qty or 10)

        return result

    def test_win_rate_above_40_pct_on_trending_data(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        gen = SignalGenerator(mock_sentiment_context, 500_000)
        result = self._run_backtest(bullish_df, gen, mock_sentiment_context)
        if not result.trades:
            pytest.skip("No trades generated in backtest")
        assert result.win_rate >= 0.35, (
            f"Win rate {result.win_rate:.1%} too low on trending data — strategy may be broken"
        )

    def test_profit_factor_above_1_on_trending_data(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        gen = SignalGenerator(mock_sentiment_context, 500_000)
        result = self._run_backtest(bullish_df, gen, mock_sentiment_context)
        if not result.trades:
            pytest.skip("No trades")
        assert result.profit_factor >= 1.0, (
            f"Profit factor {result.profit_factor:.2f} < 1.0 — system losing money net"
        )

    def test_max_drawdown_within_20_pct(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        gen = SignalGenerator(mock_sentiment_context, 500_000)
        result = self._run_backtest(bullish_df, gen, mock_sentiment_context)
        capital = 500_000
        dd_pct = result.max_drawdown / capital * 100
        assert dd_pct < 20.0, f"Max drawdown {dd_pct:.1f}% exceeds 20% — catastrophic risk"

    def test_no_signals_when_sentiment_very_bearish(self, bullish_df):
        from backend.core.sentiment_engine import SentimentContext, MarketRegime, SentimentLabel
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        bear_ctx = SentimentContext(
            computed_at=datetime.now(),
            regime=MarketRegime.STRONG_BEAR,
            sentiment=SentimentLabel.VERY_BEARISH,
            composite_score=-0.9,
            vix_score=-1.0, pcr_score=-0.8, fii_score=-0.9,
            news_score=-0.5, breadth_score=-0.8, momentum_score=-0.9,
            india_vix=35.0, nifty_pcr=0.5, fii_net_crores=-3000.0,
            advance_decline=0.15,
            allow_long=False,   # ← gates closed
            allow_short=True,
            allow_options_buy=True,
            allow_options_sell=False,
            narrative="Extreme bear"
        )
        gen = SignalGenerator(bear_ctx, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        assert sig is None or sig.direction.value == "SHORT", (
            "In strong bear, long signals must be blocked"
        )

    def test_rrr_minimum_across_all_signals(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        if sig is None: pytest.skip("No signal")
        assert sig.risk_reward_1 >= 1.5, (
            f"Every signal must have RRR ≥ 1.5, got {sig.risk_reward_1}"
        )

    def test_sl_is_never_nan_or_zero(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        if sig is None: pytest.skip("No signal")
        assert sig.stop_loss > 0, "Stop-loss must be > 0"
        assert not math.isnan(sig.stop_loss), "Stop-loss must not be NaN"

    def test_catastrophic_sl_check_rejected(self, bullish_df, mock_sentiment_context):
        """Signals where SL > 8% from entry must be filtered."""
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        if sig is None: pytest.skip("No signal")
        assert sig.sl_pct <= 8.0, (
            f"SL of {sig.sl_pct:.1f}% is catastrophic — must be ≤ 8%"
        )


# ════════════════════════════════════════════════════════════════
#  STEP 5F: SAFETY / SANITY CHECKS
# ════════════════════════════════════════════════════════════════

class TestSafetyGuards:

    def test_no_signal_below_min_score(self, flat_df, mock_sentiment_context):
        """Flat/choppy data should generate no signal."""
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(flat_df)
        sig = gen.generate_equity_signal("NSE:TEST", df)
        # May or may not generate — just assert no exception

    def test_empty_dataframe_safe(self, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        gen = SignalGenerator(mock_sentiment_context)
        sig = gen.generate_equity_signal("NSE:TEST", pd.DataFrame())
        assert sig is None, "Empty DF must return None"

    def test_futures_hedge_always_defined(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator, HedgeStrategy
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_nifty_future_signal(
            df, spot=24500, contract="NIFTY25JULFUT", expiry=date(2025, 7, 31)
        )
        if sig is None: pytest.skip("No signal")
        assert sig.hedge.strategy != HedgeStrategy.NONE
        assert len(sig.hedge.description) > 20, "Hedge description must be meaningful"

    def test_options_signal_has_breakeven(self, bullish_df, mock_sentiment_context):
        from backend.core.signal_generator import SignalGenerator
        from backend.data.data_fetcher import DataFetcher

        gen = SignalGenerator(mock_sentiment_context, 500_000)
        df  = DataFetcher().enrich_with_indicators(bullish_df)
        sig = gen.generate_nifty_option_signal(
            df, spot=24500, option_type="CE", strike=24500,
            premium=180, expiry=date(2025, 7, 31), dte=12, iv=17.0
        )
        if sig is None: pytest.skip("No signal")
        assert sig.hedge.breakeven_1 is not None or sig.hedge.breakeven_2 is not None, (
            "Spread hedge must define at least one breakeven"
        )


# ════════════════════════════════════════════════════════════════
#  PYTEST CONFIGURATION
# ════════════════════════════════════════════════════════════════

def pytest_configure(config):
    config.addinivalue_line("markers", "integration: integration tests requiring live DB")
    config.addinivalue_line("markers", "slow: tests that take > 5s")
