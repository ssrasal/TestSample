"""
db_connector.py — STEP 3 (Part 4)
Async PostgreSQL 18 connection pool using asyncpg.
Provides type-safe repository methods for all 7 call tables.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, date
from typing import Any, Optional
from uuid import UUID

import asyncpg

from core.signal_generator import Signal, HedgeSpec

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://autosignal:autosignal123@localhost:5432/autosignal_db"
)

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


# ════════════════════════════════════════════════════════════════
#  POOL MANAGEMENT
# ════════════════════════════════════════════════════════════════

async def init_pool(min_size: int = 3, max_size: int = 15) -> asyncpg.Pool:
    """Initialize connection pool. Call once at startup."""
    global _pool
    async with _pool_lock:
        if _pool is None:
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=min_size,
                max_size=max_size,
                command_timeout=30,
                max_inactive_connection_lifetime=300,
                server_settings={
                    "application_name": "autosignal_pro",
                    "statement_timeout": "25000",   # 25 sec statement timeout
                },
                init=_init_connection,
            )
            logger.info(f"PostgreSQL pool created. Min={min_size}, Max={max_size}")
    return _pool


async def _init_connection(conn: asyncpg.Connection):
    """Called for each new connection — set custom types."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog"
    )
    # Register UUID as string to avoid conversion overhead
    await conn.set_type_codec(
        "uuid", encoder=str, decoder=str,
        schema="pg_catalog"
    )


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        return await init_pool()
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL pool closed.")


@asynccontextmanager
async def acquire():
    """Context manager for single connection from pool."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


# ════════════════════════════════════════════════════════════════
#  BASE REPOSITORY
# ════════════════════════════════════════════════════════════════

class BaseRepository:
    async def _execute(self, sql: str, *args) -> str:
        async with acquire() as conn:
            return await conn.execute(sql, *args)

    async def _fetchrow(self, sql: str, *args) -> Optional[asyncpg.Record]:
        async with acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def _fetch(self, sql: str, *args) -> list[asyncpg.Record]:
        async with acquire() as conn:
            return await conn.fetch(sql, *args)

    async def _executemany(self, sql: str, data: list[tuple]) -> None:
        async with acquire() as conn:
            await conn.executemany(sql, data)

    @staticmethod
    def _records_to_dicts(records: list[asyncpg.Record]) -> list[dict]:
        return [dict(r) for r in records]


# ════════════════════════════════════════════════════════════════
#  SIGNAL REPOSITORY
# ════════════════════════════════════════════════════════════════

class SignalRepository(BaseRepository):
    """
    Writes generated signals to their respective dedicated tables.
    One method per instrument type — no cross-table confusion.
    """

    # ── Stock Calls ───────────────────────────────────────────────
    async def insert_stock_call(self, sig: Signal) -> str:
        sql = """
            INSERT INTO stock_calls (
                symbol, call_type, direction, confidence, signal_score,
                entry_price_low, entry_price_high, entry_order_type,
                target_1, target_2, stop_loss,
                trailing_sl_rule, risk_reward_ratio,
                suggested_quantity, position_size_inr,
                regime_at_signal, sentiment_at_signal, sentiment_score,
                vix_at_signal, rsi_14, macd_hist, ema_9, ema_21,
                atr_14, adx_14, volume_ratio, vwap, supertrend_dir,
                rationale, tags, signal_time
            ) VALUES (
                $1,$2,$3,$4,$5, $6,$7,$8, $9,$10,$11,
                $12,$13, $14,$15, $16,$17,$18, $19,$20,$21,
                $22,$23, $24,$25,$26,$27,$28, $29,$30,$31
            ) RETURNING id
        """
        ind = sig.indicators
        row = await self._fetchrow(sql,
            sig.symbol, self._get_call_type(sig), sig.direction.value,
            sig.confidence.value, sig.signal_score,
            sig.entry_price_low, sig.entry_price_high, sig.entry_order_type,
            sig.target_1, sig.target_2, sig.stop_loss,
            sig.trailing_sl.human_readable, sig.risk_reward_1,
            sig.suggested_qty, sig.position_size_inr,
            sig.regime.value if sig.regime else None,
            sig.sentiment.value if sig.sentiment else None,
            sig.sentiment_score, sig.vix_at_signal,
            ind.get("rsi_14"), ind.get("macd_hist"),
            ind.get("ema_9"), ind.get("ema_21"),
            ind.get("atr_14"), ind.get("adx_14"),
            ind.get("volume_ratio"), ind.get("vwap"),
            ind.get("supertrend_dir"),
            sig.rationale, sig.tags, sig.signal_time,
        )
        return str(row["id"])

    # ── Nifty Futures Calls ───────────────────────────────────────
    async def insert_nifty_futures_call(self, sig: Signal,
                                         contract: str, expiry: date,
                                         lot_size: int = 75) -> str:
        h = sig.hedge
        sql = """
            INSERT INTO nifty_futures_calls (
                contract_symbol, expiry_date, lot_size, lots, direction,
                entry_price_low, entry_price_high, entry_order_type,
                target_1, target_2, stop_loss, trailing_sl_rule,
                hedge_strategy, hedge_strike, hedge_option_type,
                hedge_lots, hedge_premium, hedge_description,
                is_overnight, nifty_spot, pcr_at_signal, vix_at_signal,
                required_margin, confidence, regime_at_signal,
                sentiment_score, signal_score, rationale, tags, signal_time
            ) VALUES (
                $1,$2,$3,$4,$5, $6,$7,$8, $9,$10,$11,$12,
                $13,$14,$15, $16,$17,$18, $19,$20,$21,$22,
                $23,$24,$25, $26,$27,$28,$29,$30
            ) RETURNING id
        """
        row = await self._fetchrow(sql,
            contract, expiry, lot_size, sig.suggested_lots or 1, sig.direction.value,
            sig.entry_price_low, sig.entry_price_high, sig.entry_order_type,
            sig.target_1, sig.target_2, sig.stop_loss,
            sig.trailing_sl.human_readable,
            h.strategy.value, h.hedge_strike, h.hedge_option_type,
            h.hedge_lots, h.hedge_premium_est, h.description,
            False, sig.entry_price_low,  # nifty_spot approx
            sig.pcr_at_signal, sig.vix_at_signal,
            None,  # required_margin — computed separately
            sig.confidence.value,
            sig.regime.value if sig.regime else None,
            sig.sentiment_score, sig.signal_score,
            sig.rationale, sig.tags, sig.signal_time,
        )
        return str(row["id"])

    # ── Nifty Options Calls ───────────────────────────────────────
    async def insert_nifty_options_call(self, sig: Signal, contract: str,
                                         expiry: date, lot_size: int = 75) -> str:
        h = sig.hedge
        sql = """
            INSERT INTO nifty_options_calls (
                contract_symbol, option_type, strike_price, expiry_date,
                lot_size, lots, direction,
                entry_premium_low, entry_premium_high, entry_order_type,
                target_premium_1, target_premium_2, stop_loss_premium,
                sl_pct_of_premium, trailing_sl_rule, time_stop_rule,
                hedge_strategy, hedge_strike, hedge_option_type,
                hedge_lots, hedge_premium, spread_type,
                max_loss_inr, max_profit_inr,
                breakeven_1, breakeven_2, is_spread, strategy_name,
                nifty_spot, delta, theta, vega, iv,
                iv_rank, days_to_expiry, moneyness,
                confidence, regime_at_signal, sentiment_score,
                pcr_at_signal, vix_at_signal, signal_score,
                rationale, tags, signal_time
            ) VALUES (
                $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,
                $11,$12,$13, $14,$15,$16, $17,$18,$19,
                $20,$21,$22, $23,$24, $25,$26,$27,$28,
                $29,$30,$31,$32,$33, $34,$35,$36,
                $37,$38,$39, $40,$41,$42, $43,$44,$45
            ) RETURNING id
        """
        row = await self._fetchrow(sql,
            contract, sig.option_type, sig.strike_price, expiry,
            lot_size, sig.suggested_lots or 1, sig.direction.value,
            sig.entry_price_low, sig.entry_price_high, sig.entry_order_type,
            sig.target_1, sig.target_2, sig.stop_loss,
            sig.sl_pct, sig.trailing_sl.human_readable, None,
            h.strategy.value, h.hedge_strike, h.hedge_option_type,
            h.hedge_lots, h.hedge_premium_est, h.spread_type,
            h.max_loss_inr, h.max_profit_inr,
            h.breakeven_1, h.breakeven_2, True, sig.symbol,
            sig.entry_price_low,  # nifty_spot approx
            sig.delta, sig.theta, None, sig.iv,
            None, sig.days_to_expiry,
            self._moneyness_tag(sig.tags),
            sig.confidence.value,
            sig.regime.value if sig.regime else None,
            sig.sentiment_score,
            sig.pcr_at_signal, sig.vix_at_signal, sig.signal_score,
            sig.rationale, sig.tags, sig.signal_time,
        )
        return str(row["id"])

    # ── Update Call Status ────────────────────────────────────────
    async def close_call(self, table: str, call_id: str,
                          exit_price: float, status: str, pnl: float) -> None:
        """Mark any call as closed with exit price, status, and realized P&L."""
        sql = f"""
            UPDATE {table}
            SET exit_price=$1, status=$2, actual_pnl=$3,
                exit_time=NOW(), updated_at=NOW()
            WHERE id=$4::uuid
        """
        await self._execute(sql, exit_price, status, pnl, call_id)

    async def update_sl(self, table: str, call_id: str, new_sl: float) -> None:
        """Update SL price (for trailing stop adjustments)."""
        sql = f"""
            UPDATE {table}
            SET stop_loss=$1, updated_at=NOW()
            WHERE id=$2::uuid AND status='ACTIVE'
        """
        await self._execute(sql, new_sl, call_id)

    # ── Sentiment ─────────────────────────────────────────────────
    async def insert_sentiment(self, ctx) -> None:
        sql = """
            INSERT INTO market_sentiment_log (
                computed_at, regime, sentiment, composite_score,
                vix_score, pcr_score, fii_score, news_nlp_score,
                breadth_score, momentum_score,
                india_vix, nifty_pcr, fii_net_crores,
                allow_long, allow_short, allow_options_buy, allow_options_sell,
                raw_inputs
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            ON CONFLICT (computed_at) DO NOTHING
        """
        await self._execute(sql,
            ctx.computed_at, ctx.regime.value, ctx.sentiment.value,
            ctx.composite_score, ctx.vix_score, ctx.pcr_score,
            ctx.fii_score, ctx.news_score, ctx.breadth_score, ctx.momentum_score,
            ctx.india_vix, ctx.nifty_pcr, ctx.fii_net_crores,
            ctx.allow_long, ctx.allow_short, ctx.allow_options_buy, ctx.allow_options_sell,
            json.dumps({"narrative": ctx.narrative}),
        )

    # ── System Events ─────────────────────────────────────────────
    async def log_event(self, event_type: str, message: str,
                         severity: str = "INFO", metadata: dict = None) -> None:
        sql = """
            INSERT INTO system_events(event_type, severity, message, metadata)
            VALUES($1,$2,$3,$4)
        """
        await self._execute(sql, event_type, severity, message,
                            json.dumps(metadata) if metadata else None)

    # ────── Dashboard Queries ──────────────────────────────────────

    async def get_active_signals(self) -> list[dict]:
        rows = await self._fetch("SELECT * FROM v_active_signals ORDER BY signal_time DESC")
        return self._records_to_dicts(rows)

    async def get_instrument_performance(self) -> list[dict]:
        rows = await self._fetch("SELECT * FROM v_instrument_performance ORDER BY total_pnl DESC")
        return self._records_to_dicts(rows)

    async def get_calls_by_table(self, table: str, limit: int = 100,
                                   status: str = None) -> list[dict]:
        where = "WHERE status=$2::call_status" if status else ""
        sql = f"SELECT * FROM {table} {where} ORDER BY signal_time DESC LIMIT $1"
        args = (limit, status) if status else (limit,)
        rows = await self._fetch(sql, *args)
        return self._records_to_dicts(rows)

    async def get_daily_performance(self, days: int = 30) -> list[dict]:
        rows = await self._fetch(
            "SELECT * FROM daily_performance ORDER BY perf_date DESC LIMIT $1", days
        )
        return self._records_to_dicts(rows)

    async def validate_performance(self, table: str = None,
                                    from_date: str = None,
                                    to_date: str = None) -> dict:
        """Comprehensive validation across all call tables."""
        tables = (
            [table] if table else
            ["stock_calls","stock_options_calls","stock_futures_calls",
             "nifty_futures_calls","nifty_options_calls",
             "sensex_futures_calls","sensex_options_calls"]
        )
        results = {}
        for tbl in tables:
            sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE status!='ACTIVE')      AS total_closed,
                    COUNT(*) FILTER (WHERE actual_pnl > 0)        AS winners,
                    COUNT(*) FILTER (WHERE actual_pnl < 0)         AS losers,
                    ROUND(100.0*COUNT(*) FILTER (WHERE actual_pnl>0)
                        /NULLIF(COUNT(*) FILTER(WHERE status!='ACTIVE'),0),2) AS win_rate_pct,
                    COALESCE(SUM(actual_pnl),0)                   AS total_pnl,
                    COALESCE(AVG(actual_pnl) FILTER(WHERE actual_pnl>0),0) AS avg_win,
                    COALESCE(AVG(actual_pnl) FILTER(WHERE actual_pnl<0),0) AS avg_loss,
                    COALESCE(MAX(actual_pnl),0)                   AS best_trade,
                    COALESCE(MIN(actual_pnl),0)                   AS worst_trade,
                    COALESCE(SUM(actual_pnl) FILTER(WHERE actual_pnl>0),0) /
                        NULLIF(ABS(SUM(actual_pnl) FILTER(WHERE actual_pnl<0)),0)
                                                                  AS profit_factor,
                    COUNT(*) FILTER (WHERE status='TARGET1_HIT')  AS target1_hits,
                    COUNT(*) FILTER (WHERE status='TARGET2_HIT')  AS target2_hits,
                    COUNT(*) FILTER (WHERE status='SL_HIT')       AS sl_hits,
                    COUNT(*) FILTER (WHERE status='TRAILING_SL_HIT') AS trailing_sl_hits
                FROM {tbl}
                WHERE ($1::date IS NULL OR signal_time::date >= $1)
                  AND ($2::date IS NULL OR signal_time::date <= $2)
            """
            row = await self._fetchrow(sql, from_date, to_date)
            results[tbl] = dict(row) if row else {}
        return results

    # ── Helpers ───────────────────────────────────────────────────
    @staticmethod
    def _get_call_type(sig: Signal) -> str:
        if "SWING" in sig.tags:      return "SWING"
        if "POSITIONAL" in sig.tags: return "POSITIONAL"
        return "INTRADAY"

    @staticmethod
    def _moneyness_tag(tags: list[str]) -> Optional[str]:
        for t in tags:
            if t in ("ITM","ATM","OTM","DITM","DOTM"):
                return t
        return None
