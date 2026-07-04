"""
data_fetcher.py — STEP 3 (Part 1)
Resilient async data pipeline with circuit breakers, retry logic,
rate limiting, and fallback cache. Handles OHLCV, Order Book,
Option Chain, India VIX, PCR, and FII/DII data.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Callable, Optional
from functools import wraps

import aiohttp
import pandas as pd
import pandas_ta as ta

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  DATA MODELS
# ════════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class OHLCVBar:
    symbol:        str
    interval:      str
    timestamp:     datetime
    open:          float
    high:          float
    low:           float
    close:         float
    volume:        int
    oi:            Optional[int]   = None
    vwap:          Optional[float] = None


@dataclass
class OptionChainData:
    underlying:    str
    snapshot_time: datetime
    expiry_date:   date
    spot_price:    float
    atm_strike:    float
    total_call_oi: int
    total_put_oi:  int
    pcr_oi:        float
    pcr_volume:    float
    max_pain:      Optional[float]
    iv_rank:       Optional[float]
    strikes:       list[dict]   = field(default_factory=list)


@dataclass
class FIIDIIData:
    activity_date:  date
    fii_net_eq:     float   # crores
    fii_net_deriv:  float
    dii_net_eq:     float
    dii_net_deriv:  float


@dataclass
class VIXData:
    recorded_at:   datetime
    vix:           float
    vix_change:    Optional[float]
    vix_pct_chg:   Optional[float]


# ════════════════════════════════════════════════════════════════
#  CIRCUIT BREAKER
# ════════════════════════════════════════════════════════════════

class CircuitState(Enum):
    CLOSED   = "CLOSED"    # normal, requests go through
    OPEN     = "OPEN"      # tripped, requests fail fast
    HALF_OPEN = "HALF_OPEN" # testing recovery


class CircuitBreaker:
    """
    Prevents cascading failures when a data source is unavailable.
    Opens after `failure_threshold` consecutive failures.
    Half-opens after `recovery_timeout` seconds to test recovery.
    """

    def __init__(self, name: str,
                 failure_threshold: int = 3,
                 recovery_timeout: float = 60.0):
        self.name              = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout  = recovery_timeout
        self._state            = CircuitState.CLOSED
        self._failure_count    = 0
        self._last_failure_time: Optional[float] = None

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._last_failure_time > self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(f"Circuit '{self.name}' → HALF_OPEN")
        return self._state

    def record_success(self):
        self._failure_count = 0
        if self._state != CircuitState.CLOSED:
            logger.info(f"Circuit '{self.name}' → CLOSED")
        self._state = CircuitState.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            if self._state != CircuitState.OPEN:
                logger.warning(f"Circuit '{self.name}' → OPEN after {self._failure_count} failures")
            self._state = CircuitState.OPEN

    def allow_request(self) -> bool:
        return self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)


def with_circuit_breaker(breaker: CircuitBreaker):
    """Decorator factory: wraps async method with circuit breaker logic."""
    def decorator(fn: Callable):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            if not breaker.allow_request():
                raise RuntimeError(f"Circuit '{breaker.name}' is OPEN — request blocked")
            try:
                result = await fn(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
        return wrapper
    return decorator


# ════════════════════════════════════════════════════════════════
#  RATE LIMITER
# ════════════════════════════════════════════════════════════════

class RateLimiter:
    """Token-bucket rate limiter for API calls."""

    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self._last_call   = 0.0
        self._lock        = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self._last_call = time.monotonic()


# ════════════════════════════════════════════════════════════════
#  IN-MEMORY CACHE (fallback when source is down)
# ════════════════════════════════════════════════════════════════

class DataCache:
    def __init__(self, ttl_seconds: int = 300):
        self._store: dict[str, tuple[Any, float]] = {}
        self.ttl = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry and time.monotonic() - entry[1] < self.ttl:
            return entry[0]
        return None

    def set(self, key: str, value: Any):
        self._store[key] = (value, time.monotonic())

    def get_stale(self, key: str) -> Optional[Any]:
        """Return cached data even if expired — emergency fallback."""
        entry = self._store.get(key)
        return entry[0] if entry else None


# ════════════════════════════════════════════════════════════════
#  MAIN DATA FETCHER
# ════════════════════════════════════════════════════════════════

class DataFetcher:
    """
    Resilient async data fetcher for all market data sources.
    - NSE Option Chain via scraping (public endpoint)
    - OHLCV via KiteConnect or yfinance fallback
    - India VIX via NSE API
    - FII/DII from NSE CSV
    """

    NSE_BASE     = "https://www.nseindia.com"
    NSE_API      = "https://www.nseindia.com/api"
    NSE_HEADERS  = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://www.nseindia.com",
        "X-Requested-With":"XMLHttpRequest",
    }

    def __init__(self, kite=None):
        self.kite         = kite       # KiteConnect instance (optional)
        self._session:    Optional[aiohttp.ClientSession] = None
        self._cache       = DataCache(ttl_seconds=120)
        self._long_cache  = DataCache(ttl_seconds=3600)  # 1hr for FII/DII
        self._nse_breaker = CircuitBreaker("NSE_API", failure_threshold=3)
        self._kite_breaker= CircuitBreaker("KITE_API", failure_threshold=5)
        self._rate_limiter= RateLimiter(calls_per_second=2.0)  # NSE rate limit
        self._instrument_map: dict[str, int] = {}  # symbol → kite token

    async def __aenter__(self) -> "DataFetcher":
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        self._session = aiohttp.ClientSession(
            headers=self.NSE_HEADERS,
            timeout=timeout,
            connector=aiohttp.TCPConnector(limit=10, limit_per_host=5),
        )
        # Prime NSE session cookie
        await self._prime_nse_session()
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    async def _prime_nse_session(self):
        """NSE requires a cookie from the homepage before API calls work."""
        try:
            async with self._session.get(self.NSE_BASE, timeout=aiohttp.ClientTimeout(total=8)) as r:
                logger.info(f"NSE session primed — status {r.status}")
        except Exception as e:
            logger.warning(f"NSE session prime failed (non-fatal): {e}")

    async def _nse_get(self, endpoint: str, params: dict = None) -> dict:
        """Rate-limited, circuit-breaker-protected GET to NSE API."""
        await self._rate_limiter.acquire()
        if not self._nse_breaker.allow_request():
            cached = self._cache.get_stale(endpoint)
            if cached:
                logger.warning(f"NSE circuit open — returning stale cache for {endpoint}")
                return cached
            raise RuntimeError(f"NSE circuit OPEN, no cache for {endpoint}")
        try:
            url = f"{self.NSE_API}/{endpoint}"
            async with self._session.get(url, params=params) as resp:
                if resp.status == 401:
                    await self._prime_nse_session()
                    async with self._session.get(url, params=params) as r2:
                        data = await r2.json(content_type=None)
                else:
                    data = await resp.json(content_type=None)
            self._nse_breaker.record_success()
            self._cache.set(endpoint, data)
            return data
        except Exception as e:
            self._nse_breaker.record_failure()
            stale = self._cache.get_stale(endpoint)
            if stale:
                logger.warning(f"NSE error, using stale: {e}")
                return stale
            raise

    # ── OHLCV ───────────────────────────────────────────────────
    async def fetch_ohlcv(self, symbol: str, interval: str = "5m",
                          days_back: int = 10) -> list[OHLCVBar]:
        """
        Fetch OHLCV via KiteConnect if available, else yfinance.
        Returns list of OHLCVBar sorted ascending by timestamp.
        """
        if self.kite and self._kite_breaker.allow_request():
            return await self._fetch_ohlcv_kite(symbol, interval, days_back)
        return await self._fetch_ohlcv_yfinance(symbol, interval, days_back)

    async def _fetch_ohlcv_kite(self, symbol: str, interval: str,
                                  days_back: int) -> list[OHLCVBar]:
        INTERVAL_MAP = {
            "1m": "minute", "3m": "3minute", "5m": "5minute",
            "15m": "15minute", "30m": "30minute", "1h": "60minute",
            "1d": "day"
        }
        kite_interval = INTERVAL_MAP.get(interval, "5minute")
        token = self._instrument_map.get(symbol)
        if not token:
            raise ValueError(f"No instrument token for {symbol}")
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=days_back)
        try:
            loop = asyncio.get_event_loop()
            raw  = await loop.run_in_executor(
                None,
                lambda: self.kite.historical_data(token, from_dt, to_dt, kite_interval)
            )
            self._kite_breaker.record_success()
            return [
                OHLCVBar(
                    symbol=symbol, interval=interval,
                    timestamp=bar["date"], open=bar["open"],
                    high=bar["high"], low=bar["low"],
                    close=bar["close"], volume=bar.get("volume",0),
                    oi=bar.get("oi")
                ) for bar in raw
            ]
        except Exception as e:
            self._kite_breaker.record_failure()
            logger.error(f"Kite OHLCV failed for {symbol}: {e}")
            return await self._fetch_ohlcv_yfinance(symbol, interval, days_back)

    async def _fetch_ohlcv_yfinance(self, symbol: str, interval: str,
                                     days_back: int) -> list[OHLCVBar]:
        """yfinance fallback — runs in executor to avoid blocking."""
        import yfinance as yf
        yf_symbol = symbol.replace("NSE:", "") + ".NS"
        PERIOD_MAP = {
            "1m": "5d", "5m": "60d", "15m": "60d",
            "30m": "60d", "1h": "730d", "1d": "5y"
        }
        period = PERIOD_MAP.get(interval, "60d")
        loop   = asyncio.get_event_loop()
        try:
            ticker = yf.Ticker(yf_symbol)
            df = await loop.run_in_executor(
                None, lambda: ticker.history(period=period, interval=interval)
            )
            if df.empty:
                return []
            bars = []
            for ts, row in df.iterrows():
                bars.append(OHLCVBar(
                    symbol=symbol, interval=interval,
                    timestamp=ts.to_pydatetime() if hasattr(ts, 'to_pydatetime') else ts,
                    open=float(row["Open"]), high=float(row["High"]),
                    low=float(row["Low"]),  close=float(row["Close"]),
                    volume=int(row.get("Volume", 0))
                ))
            return sorted(bars, key=lambda b: b.timestamp)
        except Exception as e:
            logger.error(f"yfinance fallback failed for {symbol}: {e}")
            return []

    # ── OPTION CHAIN ────────────────────────────────────────────
    async def fetch_option_chain(self, underlying: str = "NIFTY") -> Optional[OptionChainData]:
        """
        Fetch full option chain from NSE public API.
        Supports: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY,
                  SENSEX, BANKEX, and any stock symbol.
        """
        cache_key = f"oc_{underlying}"
        cached = self._cache.get(cache_key)
        if cached:
            return cached

        INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
        endpoint = (f"option-chain-indices?symbol={underlying}"
                    if underlying in INDEX_SYMBOLS
                    else f"option-chain-equities?symbol={underlying}")

        try:
            data = await self._nse_get(endpoint)
            result = self._parse_option_chain(data, underlying)
            if result:
                self._cache.set(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Option chain fetch failed for {underlying}: {e}")
            return self._cache.get_stale(cache_key)

    def _parse_option_chain(self, raw: dict, underlying: str) -> Optional[OptionChainData]:
        records = raw.get("records", {})
        data    = records.get("data", [])
        spot    = records.get("underlyingValue", 0)
        if not data or not spot:
            return None
        step = 50 if "NIFTY" in underlying else (100 if "SENSEX" in underlying else 5)
        atm  = round(spot / step) * step

        strikes   = []
        total_coi = 0
        total_poi = 0
        total_cvol= 0
        total_pvol= 0

        for rec in data:
            exp  = rec.get("expiryDate","")
            strk = rec.get("strikePrice", 0)
            for ot in ("CE", "PE"):
                leg = rec.get(ot, {})
                if not leg:
                    continue
                s = {
                    "strike_price": strk, "option_type": ot,
                    "expiry":       exp,
                    "ltp":          leg.get("lastPrice", 0),
                    "bid":          leg.get("bidprice", 0),
                    "ask":          leg.get("askPrice", 0),
                    "iv":           leg.get("impliedVolatility", 0),
                    "delta":        leg.get("delta"),
                    "theta":        leg.get("theta"),
                    "vega":         leg.get("vega"),
                    "gamma":        leg.get("gamma"),
                    "oi":           leg.get("openInterest", 0),
                    "oi_change":    leg.get("changeinOpenInterest", 0),
                    "volume":       leg.get("totalTradedVolume", 0),
                    "bid_qty":      leg.get("bidQty", 0),
                    "ask_qty":      leg.get("askQty", 0),
                }
                strikes.append(s)
                if ot == "CE":
                    total_coi  += s["oi"]
                    total_cvol += s["volume"]
                else:
                    total_poi  += s["oi"]
                    total_pvol += s["volume"]

        pcr_oi  = total_poi / total_coi  if total_coi  else 0
        pcr_vol = total_pvol / total_cvol if total_cvol else 0
        max_pain = self._compute_max_pain(strikes)
        nearest_expiry = self._get_nearest_expiry(strikes)

        return OptionChainData(
            underlying=underlying, snapshot_time=datetime.now(),
            expiry_date=nearest_expiry, spot_price=spot, atm_strike=atm,
            total_call_oi=total_coi, total_put_oi=total_poi,
            pcr_oi=round(pcr_oi,4), pcr_volume=round(pcr_vol,4),
            max_pain=max_pain, iv_rank=None, strikes=strikes,
        )

    def _compute_max_pain(self, strikes: list[dict]) -> Optional[float]:
        """Max pain = strike where total option premium loss is maximum."""
        if not strikes:
            return None
        unique_strikes = sorted(set(s["strike_price"] for s in strikes))
        min_pain = float("inf")
        max_pain_strike = unique_strikes[0]
        for test_strike in unique_strikes:
            total_loss = 0
            for s in strikes:
                if s["option_type"] == "CE":
                    total_loss += max(0, s["strike_price"] - test_strike) * s["oi"]
                else:
                    total_loss += max(0, test_strike - s["strike_price"]) * s["oi"]
            if total_loss < min_pain:
                min_pain = total_loss
                max_pain_strike = test_strike
        return max_pain_strike

    def _get_nearest_expiry(self, strikes: list[dict]) -> date:
        expiries = []
        for s in strikes:
            try:
                expiries.append(datetime.strptime(s["expiry"], "%d-%b-%Y").date())
            except (ValueError, KeyError):
                pass
        future = [e for e in expiries if e >= date.today()]
        return min(future) if future else date.today()

    # ── INDIA VIX ────────────────────────────────────────────────
    async def fetch_india_vix(self) -> Optional[VIXData]:
        cached = self._cache.get("india_vix")
        if cached:
            return cached
        try:
            data = await self._nse_get("allIndices")
            for idx in data.get("data", []):
                if idx.get("indexSymbol") == "India VIX":
                    vix_data = VIXData(
                        recorded_at=datetime.now(),
                        vix=float(idx["last"]),
                        vix_change=float(idx.get("change", 0)),
                        vix_pct_chg=float(idx.get("percentChange", 0)),
                    )
                    self._cache.set("india_vix", vix_data)
                    return vix_data
        except Exception as e:
            logger.error(f"VIX fetch error: {e}")
            return self._cache.get_stale("india_vix")
        return None

    # ── FII / DII ────────────────────────────────────────────────
    async def fetch_fii_dii(self) -> Optional[FIIDIIData]:
        """Fetch FII/DII from NSE CSV report."""
        cached = self._long_cache.get("fii_dii")
        if cached:
            return cached
        try:
            url = f"{self.NSE_BASE}/products/dynaContent/equities/equityDerivativesStatistics.htm"
            async with self._session.get(url) as resp:
                text = await resp.text()
            # NSE also provides structured JSON for FII/DII
            data = await self._nse_get("fiidiiTradeReact")
            rows = data if isinstance(data, list) else []
            fii_eq = dii_eq = fii_d = dii_d = 0.0
            for row in rows:
                cat  = row.get("category","").upper()
                bval = float(row.get("buyValue", 0) or 0)
                sval = float(row.get("sellValue",0) or 0)
                net  = bval - sval
                if "FII" in cat and "EQUITY" in cat:   fii_eq = net
                elif "DII" in cat and "EQUITY" in cat: dii_eq = net
                elif "FII" in cat:                     fii_d  = net
                elif "DII" in cat:                     dii_d  = net
            result = FIIDIIData(
                activity_date=date.today(),
                fii_net_eq=fii_eq, fii_net_deriv=fii_d,
                dii_net_eq=dii_eq, dii_net_deriv=dii_d,
            )
            self._long_cache.set("fii_dii", result)
            return result
        except Exception as e:
            logger.error(f"FII/DII fetch error: {e}")
            return self._long_cache.get_stale("fii_dii")

    # ── ORDER BOOK ───────────────────────────────────────────────
    async def fetch_order_book(self, symbol: str) -> Optional[dict]:
        """Fetch 5-level depth via KiteConnect."""
        if not self.kite:
            return None
        token = self._instrument_map.get(symbol)
        if not token:
            return None
        try:
            loop = asyncio.get_event_loop()
            quote = await loop.run_in_executor(None, lambda: self.kite.quote([symbol]))
            q     = quote.get(symbol, {})
            depth = q.get("depth", {})
            best_bid = depth.get("buy",  [{}])[0].get("price", 0)
            best_ask = depth.get("sell", [{}])[0].get("price", 0)
            total_bid_qty = sum(d.get("quantity",0) for d in depth.get("buy",  []))
            total_ask_qty = sum(d.get("quantity",0) for d in depth.get("sell", []))
            imbalance = (total_bid_qty / (total_bid_qty + total_ask_qty)
                         if (total_bid_qty + total_ask_qty) > 0 else 0.5)
            return {
                "symbol": symbol, "best_bid": best_bid, "best_ask": best_ask,
                "total_bid_qty": total_bid_qty, "total_ask_qty": total_ask_qty,
                "imbalance_ratio": round(imbalance, 4), "depth_json": json.dumps(depth),
            }
        except Exception as e:
            logger.error(f"Order book fetch failed for {symbol}: {e}")
            return None

    # ── CONVENIENCE: BUILD INDICATOR DATAFRAME ───────────────────
    def bars_to_dataframe(self, bars: list[OHLCVBar]) -> pd.DataFrame:
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame([
            {"date": b.timestamp, "open": b.open, "high": b.high,
             "low": b.low, "close": b.close, "volume": b.volume or 0}
            for b in bars
        ])
        df.set_index("date", inplace=True)
        return df

    def enrich_with_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all technical indicators to OHLCV dataframe."""
        if len(df) < 30:
            return df
        df = df.copy()
        df["ema_9"]       = ta.ema(df["close"], length=9)
        df["ema_21"]      = ta.ema(df["close"], length=21)
        df["ema_50"]      = ta.ema(df["close"], length=50)
        df["ema_200"]     = ta.ema(df["close"], length=200)
        df["rsi_14"]      = ta.rsi(df["close"], length=14)
        df["rsi_9"]       = ta.rsi(df["close"], length=9)
        df["atr_14"]      = ta.atr(df["high"], df["low"], df["close"], length=14)
        df["adx_14"]      = ta.adx(df["high"], df["low"], df["close"], length=14).iloc[:,0]
        df["obv"]         = ta.obv(df["close"], df["volume"])
        df["vol_sma_20"]  = df["volume"].rolling(20).mean()
        df["volume_ratio"]= df["volume"] / df["vol_sma_20"].replace(0, 1)

        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None and not macd.empty:
            df["macd"]        = macd.iloc[:, 0]
            df["macd_signal"] = macd.iloc[:, 2]
            df["macd_hist"]   = macd.iloc[:, 1]

        bb = ta.bbands(df["close"], length=20, std=2)
        if bb is not None and not bb.empty:
            df["bb_upper"] = bb.iloc[:, 2]
            df["bb_mid"]   = bb.iloc[:, 1]
            df["bb_lower"] = bb.iloc[:, 0]
            df["bb_pct"]   = (df["close"] - bb.iloc[:,0]) / (bb.iloc[:,2] - bb.iloc[:,0]).replace(0,1)

        df["vwap"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])

        st = ta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3.0)
        if st is not None and not st.empty:
            df["supertrend"]     = st.iloc[:, 0]
            df["supertrend_dir"] = st.iloc[:, 1].astype(int)

        df["pct_change"]    = df["close"].pct_change() * 100
        df["hl_range_pct"]  = (df["high"] - df["low"]) / df["close"] * 100
        return df
