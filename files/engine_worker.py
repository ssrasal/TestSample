"""
engine_worker.py
Master async engine. Orchestrates the full pipeline:
  DataFetcher → SentimentEngine → StrategyRouter → RiskManager → DB + WebSocket

Designed for single-process, multi-coroutine execution.
One queue per instrument type prevents cross-instrument blocking.
"""

from __future__ import annotations

import asyncio
import logging
import signal as sys_signal
from datetime import datetime, time as dtime
from typing import Optional

from data.data_fetcher import DataFetcher, OHLCVBar
from core.sentiment_engine import SentimentAdaptiveEngine
from core.signal_generator import Signal
from core.db_connector import SignalRepository, init_pool, close_pool
from core.api_server import broadcast_new_signal, broadcast_signal_update
from risk.risk_manager import RiskManager, RiskConfig, OpenPosition
from strategy.strategy_router import StrategyRouter
from utils.notifier import Notifier
from utils.logger import setup_logging

logger = logging.getLogger(__name__)

# ── Market hours (IST) ────────────────────────────────────────
MARKET_OPEN        = dtime(9, 15)
MARKET_CLOSE       = dtime(15, 30)
NO_NEW_TRADES_AFTER= dtime(14, 45)
SQUARE_OFF_TIME    = dtime(15, 10)

# ── Scan intervals ────────────────────────────────────────────
EQUITY_SCAN_INTERVAL_S    = 60   # scan equity watchlist every 60s
INDEX_SCAN_INTERVAL_S     = 30   # scan Nifty/Sensex every 30s
SENTIMENT_REFRESH_INTERVAL= 300  # refresh sentiment every 5 min
POSITION_MONITOR_INTERVAL = 15   # check open positions every 15s

EQUITY_WATCHLIST = [
    "NSE:RELIANCE","NSE:TCS","NSE:HDFCBANK","NSE:INFY","NSE:ICICIBANK",
    "NSE:SBIN","NSE:HINDUNILVR","NSE:ITC","NSE:BAJFINANCE","NSE:AXISBANK",
    "NSE:KOTAKBANK","NSE:LT","NSE:WIPRO","NSE:MARUTI","NSE:TITAN",
    "NSE:ADANIENT","NSE:SUNPHARMA","NSE:ULTRACEMCO","NSE:ONGC","NSE:NTPC",
]


class EngineWorker:
    """
    The central orchestrator. Runs as a single asyncio application.
    Each instrument class gets its own queue and worker coroutine.
    """

    def __init__(self, kite=None, config: RiskConfig = None,
                 news_api_key: str = "",
                 telegram_token: str = "",
                 telegram_chat_id: str = ""):
        self.kite          = kite
        self.fetcher       = DataFetcher(kite=kite)
        self.repo          = SignalRepository()
        self.risk          = RiskManager(config or RiskConfig())
        self.notifier      = Notifier(telegram_token, telegram_chat_id)
        self._running      = False
        self._tasks: list[asyncio.Task] = []

        # Queues (created at run time inside the event loop)
        self._equity_queue:   Optional[asyncio.Queue] = None
        self._index_queue:    Optional[asyncio.Queue] = None
        self._sentiment_ctx   = None

        # Will be created after fetcher is initialized
        self._sentiment_engine: Optional[SentimentAdaptiveEngine] = None
        self._router:          Optional[StrategyRouter] = None
        self._news_api_key = news_api_key

    # ── Startup ───────────────────────────────────────────────────
    async def start(self):
        setup_logging()
        await init_pool()

        self._equity_queue = asyncio.Queue(maxsize=500)
        self._index_queue  = asyncio.Queue(maxsize=100)

        async with self.fetcher as f:
            self._sentiment_engine = SentimentAdaptiveEngine(
                f, news_api_key=self._news_api_key
            )
            self._router = StrategyRouter(self.risk)

            # Initial sentiment + day start
            self._sentiment_ctx = await self._sentiment_engine.get_context(force_refresh=True)
            balance = await self._get_balance()
            await self.risk.start_day(balance)

            await self.repo.log_event("ENGINE_STARTED", f"Balance: ₹{balance:,.2f}")
            self.notifier.send(
                f"🚀 AutoSignal Pro STARTED\n"
                f"Balance: ₹{balance:,.2f}\n"
                f"Regime: {self._sentiment_ctx.regime.value}\n"
                f"Sentiment: {self._sentiment_ctx.sentiment.value} "
                f"({self._sentiment_ctx.composite_score:+.3f})\n"
                f"VIX: {self._sentiment_ctx.india_vix or 'N/A'}"
            )

            self._running = True
            logger.info("═" * 60)
            logger.info("  AutoSignal Pro Engine — RUNNING")
            logger.info("═" * 60)

            # Register graceful shutdown
            loop = asyncio.get_event_loop()
            for sig in (sys_signal.SIGTERM, sys_signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

            # Launch all coroutines
            self._tasks = [
                asyncio.create_task(self._sentiment_refresh_loop(f)),
                asyncio.create_task(self._equity_producer_loop(f)),
                asyncio.create_task(self._equity_consumer_loop(f)),
                asyncio.create_task(self._index_producer_loop(f)),
                asyncio.create_task(self._index_consumer_loop(f)),
                asyncio.create_task(self._position_monitor_loop(f)),
                asyncio.create_task(self._market_timer_loop()),
            ]

            try:
                await asyncio.gather(*self._tasks)
            except asyncio.CancelledError:
                pass

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        await close_pool()
        logger.info("Engine stopped gracefully.")
        await self.repo.log_event("ENGINE_STOPPED", "Clean shutdown")

    # ── PRODUCER: Equity data fetcher ────────────────────────────
    async def _equity_producer_loop(self, fetcher: DataFetcher):
        """Fetches OHLCV for every equity watchlist symbol and pushes to queue."""
        while self._running:
            if not self._is_market_open():
                await asyncio.sleep(30)
                continue
            if not self._can_open_new_trades():
                await asyncio.sleep(60)
                continue

            for symbol in EQUITY_WATCHLIST:
                if not self._running: break
                try:
                    bars = await fetcher.fetch_ohlcv(symbol, interval="5m", days_back=5)
                    if bars:
                        df = fetcher.bars_to_dataframe(bars)
                        df = fetcher.enrich_with_indicators(df)
                        await self._equity_queue.put({
                            "symbol": symbol,
                            "instrument_type": "STOCK",
                            "df": df,
                        })
                except Exception as e:
                    logger.error(f"Equity producer error {symbol}: {e}")
                await asyncio.sleep(0.5)  # rate-limit between symbols

            await asyncio.sleep(EQUITY_SCAN_INTERVAL_S)

    # ── CONSUMER: Equity signal generator ────────────────────────
    async def _equity_consumer_loop(self, fetcher: DataFetcher):
        """Consumes equity queue → generates → approves → persists signals."""
        while self._running:
            try:
                item = await asyncio.wait_for(
                    self._equity_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                ctx = self._sentiment_ctx
                if ctx is None: continue

                signal = await self._router.route(
                    instrument_type = item["instrument_type"],
                    symbol          = item["symbol"],
                    df              = item["df"],
                    context         = ctx,
                    extra_kwargs    = {"call_type": "INTRADAY"},
                )

                if signal:
                    await self._persist_and_broadcast(signal)
            except Exception as e:
                logger.error(f"Equity consumer error: {e}", exc_info=True)
            finally:
                self._equity_queue.task_done()

    # ── PRODUCER: Index futures/options ──────────────────────────
    async def _index_producer_loop(self, fetcher: DataFetcher):
        """Fetches Nifty and Sensex data + option chain, pushes to index queue."""
        while self._running:
            if not self._is_market_open():
                await asyncio.sleep(30)
                continue

            for index, sym in [("NIFTY", "NSE:NIFTY 50"), ("SENSEX", "BSE:SENSEX")]:
                try:
                    bars = await fetcher.fetch_ohlcv(sym, interval="5m", days_back=3)
                    oc   = await fetcher.fetch_option_chain(index)
                    vix  = await fetcher.fetch_india_vix()

                    if bars and oc:
                        df   = fetcher.bars_to_dataframe(bars)
                        df   = fetcher.enrich_with_indicators(df)
                        spot = oc.spot_price
                        self._sentiment_engine.update_nifty_price(spot)

                        await self._index_queue.put({
                            "index": index, "sym": sym,
                            "df": df, "oc": oc, "spot": spot,
                            "vix": vix.vix if vix else 18.0,
                        })
                except Exception as e:
                    logger.error(f"Index producer error {index}: {e}")

            await asyncio.sleep(INDEX_SCAN_INTERVAL_S)

    # ── CONSUMER: Index signal generator ─────────────────────────
    async def _index_consumer_loop(self, fetcher: DataFetcher):
        while self._running:
            try:
                item = await asyncio.wait_for(
                    self._index_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                ctx  = self._sentiment_ctx
                oc   = item["oc"]
                df   = item["df"]
                spot = item["spot"]
                index = item["index"]

                # ── Futures signal ────────────────────────────────
                fut_contract, fut_expiry = self._get_nearest_future(oc, index)
                if fut_contract:
                    lot_size = 75 if index == "NIFTY" else 10
                    inst_type = f"{index}_FUT"
                    signal = await self._router.route(
                        instrument_type = inst_type,
                        symbol          = index,
                        df              = df,
                        context         = ctx,
                        extra_kwargs    = {
                            "spot": spot, "contract": fut_contract,
                            "expiry": fut_expiry, "lot_size": lot_size,
                        },
                    )
                    if signal:
                        await self._persist_and_broadcast(signal)

                # ── Options signal (ATM CE + ATM PE) ─────────────
                if ctx.allow_options_buy:
                    step = 50 if index == "NIFTY" else 100
                    atm  = round(spot / step) * step
                    for ot in ("CE", "PE"):
                        if ot == "CE" and not ctx.allow_long: continue
                        if ot == "PE" and not ctx.allow_short: continue
                        strike_data = self._find_strike(oc, atm, ot)
                        if not strike_data: continue
                        inst_type = f"{index}_OPT"
                        signal = await self._router.route(
                            instrument_type = inst_type,
                            symbol          = f"{index}{atm}{ot}",
                            df              = df,
                            context         = ctx,
                            extra_kwargs    = {
                                "spot": spot, "option_type": ot,
                                "strike": atm,
                                "premium": strike_data.get("ltp", 100),
                                "expiry": oc.expiry_date,
                                "dte": (oc.expiry_date - __import__("datetime").date.today()).days,
                                "iv": strike_data.get("iv", 18.0),
                                "greeks": {
                                    "delta": strike_data.get("delta"),
                                    "theta": strike_data.get("theta"),
                                },
                                "lot_size": 75 if index == "NIFTY" else 10,
                            },
                        )
                        if signal:
                            await self._persist_and_broadcast(signal)

            except Exception as e:
                logger.error(f"Index consumer error: {e}", exc_info=True)
            finally:
                self._index_queue.task_done()

    # ── Position monitor ──────────────────────────────────────────
    async def _position_monitor_loop(self, fetcher: DataFetcher):
        """
        Monitors all open positions every 15s.
        Updates trailing SLs and flags positions that hit SL/target.
        """
        while self._running:
            await asyncio.sleep(POSITION_MONITOR_INTERVAL)
            positions = dict(self.risk.open_positions)
            if not positions: continue

            for signal_id, pos in positions.items():
                try:
                    bars = await fetcher.fetch_ohlcv(pos.symbol, interval="1m", days_back=1)
                    if not bars: continue
                    current_price = bars[-1].close
                    df = fetcher.bars_to_dataframe(bars)
                    df = fetcher.enrich_with_indicators(df)
                    atr = float(df.iloc[-1].get("atr_14", current_price * 0.01))

                    # Check SL
                    sl_hit = (
                        (pos.direction == "LONG"  and current_price <= pos.stop_loss) or
                        (pos.direction == "SHORT" and current_price >= pos.stop_loss)
                    )
                    if sl_hit:
                        pnl = await self.risk.close_position(signal_id, current_price, "SL_HIT")
                        await self.repo.close_call(
                            self._signal_id_to_table(signal_id), signal_id,
                            current_price, "SL_HIT", pnl or 0
                        )
                        await broadcast_signal_update(signal_id, "SL_HIT", pnl)
                        self.notifier.send(
                            f"🔴 SL HIT: {pos.symbol}\n"
                            f"Exit: ₹{current_price:.2f} | P&L: ₹{pnl or 0:+,.2f}"
                        )
                        continue

                    # Check Target 1
                    target_hit = (
                        (pos.direction == "LONG"  and current_price >= pos.target_1) or
                        (pos.direction == "SHORT" and current_price <= pos.target_1)
                    )
                    if target_hit:
                        pnl = await self.risk.close_position(signal_id, current_price, "TARGET1_HIT")
                        await self.repo.close_call(
                            self._signal_id_to_table(signal_id), signal_id,
                            current_price, "TARGET1_HIT", pnl or 0
                        )
                        await broadcast_signal_update(signal_id, "TARGET1_HIT", pnl)
                        self.notifier.send(
                            f"🟢 TARGET HIT: {pos.symbol}\n"
                            f"Exit: ₹{current_price:.2f} | P&L: ₹{pnl or 0:+,.2f}"
                        )
                        continue

                    # Update trailing SL
                    new_sl = await self.risk.compute_trailing_sl(
                        signal_id, current_price, atr, atr_multiplier=1.5
                    )
                    if new_sl:
                        await self.repo.update_sl(
                            self._signal_id_to_table(signal_id), signal_id, new_sl
                        )
                        logger.info(f"Trailing SL updated: {pos.symbol} → ₹{new_sl:.2f}")

                except Exception as e:
                    logger.error(f"Position monitor error {signal_id}: {e}")

    # ── Sentiment refresh ─────────────────────────────────────────
    async def _sentiment_refresh_loop(self, fetcher: DataFetcher):
        while self._running:
            await asyncio.sleep(SENTIMENT_REFRESH_INTERVAL)
            try:
                self._sentiment_ctx = await self._sentiment_engine.get_context(force_refresh=True)
                await self.repo.insert_sentiment(self._sentiment_ctx)
                logger.info(
                    f"Sentiment refreshed: {self._sentiment_ctx.sentiment.value} "
                    f"({self._sentiment_ctx.composite_score:+.3f})"
                )
            except Exception as e:
                logger.error(f"Sentiment refresh error: {e}")

    # ── Market timer ──────────────────────────────────────────────
    async def _market_timer_loop(self):
        """Handles end-of-day square-off and daily summary."""
        while self._running:
            await asyncio.sleep(60)
            now = datetime.now().time()
            if now >= SQUARE_OFF_TIME and now < MARKET_CLOSE:
                logger.warning("Square-off time reached — closing all open positions.")
                await self._square_off_all()
            if now >= MARKET_CLOSE:
                await self._end_of_day()
                self._running = False

    # ── Helpers ───────────────────────────────────────────────────
    def _is_market_open(self) -> bool:
        now = datetime.now().time()
        return MARKET_OPEN <= now <= MARKET_CLOSE

    def _can_open_new_trades(self) -> bool:
        return datetime.now().time() <= NO_NEW_TRADES_AFTER

    async def _get_balance(self) -> float:
        if self.kite:
            try:
                m = self.kite.margins(segment="equity")
                return float(m["available"]["live_balance"])
            except Exception: pass
        return self.risk.cfg.total_capital

    async def _persist_and_broadcast(self, signal: Signal):
        """Write signal to DB and push to WebSocket."""
        try:
            TABLE_METHOD = {
                "STOCK":      self.repo.insert_stock_call,
                "NIFTY_FUT":  lambda s: self.repo.insert_nifty_futures_call(
                    s, s.tags[0] if s.tags else "NIFTY_FUT",
                    s.expiry_date
                ),
                "NIFTY_OPT":  lambda s: self.repo.insert_nifty_options_call(
                    s, s.symbol, s.expiry_date
                ),
                "SENSEX_FUT": lambda s: self.repo.insert_nifty_futures_call(
                    s, s.symbol, s.expiry_date, 10
                ),
                "SENSEX_OPT": lambda s: self.repo.insert_nifty_options_call(
                    s, s.symbol, s.expiry_date, 10
                ),
            }
            method = TABLE_METHOD.get(signal.instrument_type, self.repo.insert_stock_call)
            signal_id = await method(signal)

            # Register in risk manager
            pos = OpenPosition(
                signal_id=signal_id,
                symbol=signal.symbol,
                instrument_type=signal.instrument_type,
                direction=signal.direction.value,
                entry_price=signal.entry_price_high,
                stop_loss=signal.stop_loss,
                target_1=signal.target_1,
                quantity=signal.suggested_qty or 1,
                lots=signal.suggested_lots or 1,
                entry_time=datetime.now(),
                peak_price=signal.entry_price_high,
            )
            await self.risk.register_position(signal_id, pos)

            # Broadcast to dashboard WebSocket
            await broadcast_new_signal({
                "id":              signal_id,
                "instrument_type": signal.instrument_type,
                "symbol":          signal.symbol,
                "direction":       signal.direction.value,
                "confidence":      signal.confidence.value,
                "signal_score":    signal.signal_score,
                "entry_low":       signal.entry_price_low,
                "entry_high":      signal.entry_price_high,
                "stop_loss":       signal.stop_loss,
                "target_1":        signal.target_1,
                "target_2":        signal.target_2,
                "rrr":             signal.risk_reward_1,
                "regime":          signal.regime.value if signal.regime else None,
                "sentiment":       signal.sentiment.value if signal.sentiment else None,
                "hedge_strategy":  signal.hedge.strategy.value,
                "hedge_desc":      signal.hedge.description,
                "signal_time":     signal.signal_time.isoformat(),
            })

            self.notifier.send(
                f"⚡ NEW SIGNAL\n"
                f"{signal.instrument_type}: {signal.symbol}\n"
                f"{signal.direction.value} | {signal.confidence.value} (score={signal.signal_score})\n"
                f"Entry: ₹{signal.entry_price_low}–{signal.entry_price_high}\n"
                f"SL: ₹{signal.stop_loss} | T1: ₹{signal.target_1}\n"
                f"Hedge: {signal.hedge.strategy.value}"
            )

        except Exception as e:
            logger.error(f"Persist/broadcast error: {e}", exc_info=True)

    async def _square_off_all(self):
        snapshot = self.risk.get_portfolio_snapshot()
        logger.warning(f"Square-off: {snapshot['open_positions']} positions closing")
        for sid, pos in list(self.risk.open_positions.items()):
            await self.risk.close_position(sid, pos.stop_loss, "SQUARE_OFF")

    async def _end_of_day(self):
        snap = self.risk.get_portfolio_snapshot()
        logger.info(f"End of day. Daily P&L: ₹{snap['daily_pnl']:+,.2f}")
        await self.repo.log_event("END_OF_DAY",
            f"Daily P&L: ₹{snap['daily_pnl']:+,.2f}", metadata=snap)
        self.notifier.send(
            f"📊 Day Complete\n"
            f"P&L: ₹{snap['daily_pnl']:+,.2f} ({snap['daily_pnl_pct']:+.2f}%)\n"
            f"Open Positions: {snap['open_positions']}"
        )

    def _signal_id_to_table(self, signal_id: str) -> str:
        pos = self.risk.open_positions.get(signal_id)
        if not pos: return "stock_calls"
        return {
            "STOCK":      "stock_calls",
            "STOCK_OPT":  "stock_options_calls",
            "STOCK_FUT":  "stock_futures_calls",
            "NIFTY_FUT":  "nifty_futures_calls",
            "NIFTY_OPT":  "nifty_options_calls",
            "SENSEX_FUT": "sensex_futures_calls",
            "SENSEX_OPT": "sensex_options_calls",
        }.get(pos.instrument_type, "stock_calls")

    def _get_nearest_future(self, oc, index: str):
        try:
            exp = oc.expiry_date
            contract = f"{index}{exp.strftime('%y%b').upper()}FUT"
            return contract, exp
        except Exception:
            return None, None

    def _find_strike(self, oc, strike: float, opt_type: str) -> Optional[dict]:
        for s in oc.strikes:
            if abs(s["strike_price"] - strike) < 1 and s["option_type"] == opt_type:
                return s
        return None
