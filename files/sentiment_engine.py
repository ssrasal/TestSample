"""
sentiment_engine.py — STEP 3 (Part 2)
SentimentAdaptiveEngine: Classifies market regime and computes a
composite sentiment score from VIX, PCR, FII/DII, news NLP, and
technical breadth. The score gates which strategies are permitted.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from data.data_fetcher import DataFetcher, VIXData, FIIDIIData, OptionChainData

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _vader = SentimentIntensityAnalyzer()
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

import aiohttp

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  ENUMS & DATA CLASSES
# ════════════════════════════════════════════════════════════════

class MarketRegime(str, Enum):
    STRONG_BULL = "STRONG_BULL"
    BULL        = "BULL"
    SIDEWAYS    = "SIDEWAYS"
    BEAR        = "BEAR"
    STRONG_BEAR = "STRONG_BEAR"
    HIGH_VOL    = "HIGH_VOL"     # extreme volatility — special rules apply


class SentimentLabel(str, Enum):
    VERY_BULLISH = "VERY_BULLISH"
    BULLISH      = "BULLISH"
    NEUTRAL      = "NEUTRAL"
    BEARISH      = "BEARISH"
    VERY_BEARISH = "VERY_BEARISH"


@dataclass
class SentimentContext:
    """Complete market context snapshot used by strategies."""
    computed_at:        datetime
    regime:             MarketRegime
    sentiment:          SentimentLabel
    composite_score:    float           # -1.0 (very bearish) → +1.0 (very bullish)

    # Sub-scores (each -1 to +1)
    vix_score:          float
    pcr_score:          float
    fii_score:          float
    news_score:         float
    breadth_score:      float
    momentum_score:     float

    # Raw inputs
    india_vix:          Optional[float]
    nifty_pcr:          Optional[float]
    fii_net_crores:     Optional[float]
    advance_decline:    Optional[float]

    # Strategy gate flags
    allow_long:         bool
    allow_short:        bool
    allow_options_buy:  bool
    allow_options_sell: bool   # writing options — needs low VIX

    # Human-readable explanation
    narrative:          str


# ════════════════════════════════════════════════════════════════
#  WEIGHTS FOR COMPOSITE SCORE
# ════════════════════════════════════════════════════════════════

SCORE_WEIGHTS = {
    "vix":       0.30,   # highest weight — VIX is the fear gauge
    "pcr":       0.20,   # put-call ratio: contrarian indicator
    "fii":       0.20,   # institutional flow
    "news":      0.10,   # NLP sentiment from news
    "breadth":   0.10,   # advance/decline
    "momentum":  0.10,   # Nifty 20-day momentum
}


# ════════════════════════════════════════════════════════════════
#  SENTIMENT ADAPTIVE ENGINE
# ════════════════════════════════════════════════════════════════

class SentimentAdaptiveEngine:
    """
    Evaluates all available market context signals and produces a
    SentimentContext object that the StrategyRouter uses to filter
    and adapt strategies.

    Refreshed every REFRESH_INTERVAL_SECONDS (configurable, default 300s).
    Thread-safe via asyncio.Lock.
    """

    REFRESH_INTERVAL_SECONDS = 300   # 5 minutes
    NEWS_API_URL = "https://newsapi.org/v2/everything"

    def __init__(self, data_fetcher: DataFetcher,
                 news_api_key: str = "",
                 refresh_interval: int = 300):
        self.fetcher          = data_fetcher
        self.news_api_key     = news_api_key
        self.refresh_interval = refresh_interval
        self._lock            = asyncio.Lock()
        self._context:        Optional[SentimentContext] = None
        self._last_refresh:   float = 0.0
        self._nifty_prices:   list[float] = []  # rolling price buffer for momentum

    # ── Public API ───────────────────────────────────────────────
    async def get_context(self, force_refresh: bool = False) -> SentimentContext:
        """
        Returns current SentimentContext.
        Refreshes automatically when stale or on force_refresh.
        """
        import time
        async with self._lock:
            if (force_refresh or self._context is None or
                    time.monotonic() - self._last_refresh > self.refresh_interval):
                self._context = await self._compute()
                self._last_refresh = time.monotonic()
            return self._context

    # ── Core computation ─────────────────────────────────────────
    async def _compute(self) -> SentimentContext:
        logger.info("Computing market sentiment…")

        # Fetch all inputs concurrently
        vix_data, oc_nifty, fii_data, news_headlines = await asyncio.gather(
            self.fetcher.fetch_india_vix(),
            self.fetcher.fetch_option_chain("NIFTY"),
            self.fetcher.fetch_fii_dii(),
            self._fetch_news_headlines(),
            return_exceptions=True,
        )

        # Guard: if fetch returned an exception, treat as None
        vix_data    = vix_data    if not isinstance(vix_data,    Exception) else None
        oc_nifty    = oc_nifty    if not isinstance(oc_nifty,    Exception) else None
        fii_data    = fii_data    if not isinstance(fii_data,    Exception) else None
        news_headlines = news_headlines if not isinstance(news_headlines, Exception) else []

        # Compute sub-scores
        vix_score      = self._score_vix(vix_data)
        pcr_score      = self._score_pcr(oc_nifty)
        fii_score      = self._score_fii(fii_data)
        news_score     = self._score_news(news_headlines)
        breadth_score  = await self._score_breadth()
        momentum_score = self._score_momentum()

        # Weighted composite
        composite = (
            SCORE_WEIGHTS["vix"]      * vix_score      +
            SCORE_WEIGHTS["pcr"]      * pcr_score      +
            SCORE_WEIGHTS["fii"]      * fii_score      +
            SCORE_WEIGHTS["news"]     * news_score      +
            SCORE_WEIGHTS["breadth"]  * breadth_score   +
            SCORE_WEIGHTS["momentum"] * momentum_score
        )
        composite = max(-1.0, min(1.0, round(composite, 4)))

        regime    = self._classify_regime(composite, vix_data)
        sentiment = self._label_sentiment(composite)
        gates     = self._compute_strategy_gates(composite, vix_data, regime)
        narrative = self._build_narrative(
            composite, vix_data, oc_nifty, fii_data,
            vix_score, pcr_score, fii_score, news_score
        )

        ctx = SentimentContext(
            computed_at      = datetime.now(),
            regime           = regime,
            sentiment        = sentiment,
            composite_score  = composite,
            vix_score        = round(vix_score,  4),
            pcr_score        = round(pcr_score,  4),
            fii_score        = round(fii_score,  4),
            news_score       = round(news_score,  4),
            breadth_score    = round(breadth_score, 4),
            momentum_score   = round(momentum_score, 4),
            india_vix        = vix_data.vix        if vix_data  else None,
            nifty_pcr        = oc_nifty.pcr_oi     if oc_nifty  else None,
            fii_net_crores   = fii_data.fii_net_eq if fii_data  else None,
            advance_decline  = None,
            allow_long       = gates["allow_long"],
            allow_short      = gates["allow_short"],
            allow_options_buy  = gates["allow_options_buy"],
            allow_options_sell = gates["allow_options_sell"],
            narrative        = narrative,
        )
        logger.info(f"Sentiment: {sentiment.value} ({composite:+.3f}) | Regime: {regime.value}")
        return ctx

    # ── Sub-scorers ──────────────────────────────────────────────
    def _score_vix(self, vix_data: Optional[VIXData]) -> float:
        """
        India VIX → sentiment score.
        VIX < 12:  very complacent → bullish (+1.0)
        VIX 12-15: low vol         → mildly bullish (+0.5)
        VIX 15-20: normal          → neutral (0.0)
        VIX 20-25: elevated        → bearish (-0.5)
        VIX 25-30: high fear       → bearish (-0.8)
        VIX > 30:  panic           → very bearish (-1.0)
        Note: VIX is a CONTRARIAN indicator at extremes.
        """
        if not vix_data:
            return 0.0
        vix = vix_data.vix
        if vix < 12:   return +0.9
        if vix < 15:   return +0.5
        if vix < 18:   return +0.2
        if vix < 20:   return  0.0
        if vix < 23:   return -0.4
        if vix < 27:   return -0.7
        if vix < 32:   return -0.9
        return -1.0

    def _score_pcr(self, oc: Optional[OptionChainData]) -> float:
        """
        Put-Call Ratio by OI → sentiment (CONTRARIAN).
        PCR > 1.5: extreme put buying → contrarian bullish (+0.8)
        PCR > 1.2: elevated puts      → mildly bullish (+0.4)
        PCR 0.8-1.2: neutral          → neutral (0.0)
        PCR 0.6-0.8: call heavy       → mildly bearish (-0.4)
        PCR < 0.6:  extreme call buy  → contrarian bearish (-0.8)
        """
        if not oc or not oc.pcr_oi:
            return 0.0
        pcr = oc.pcr_oi
        if pcr > 1.5:   return +0.8
        if pcr > 1.2:   return +0.4
        if pcr > 1.0:   return +0.1
        if pcr > 0.8:   return  0.0
        if pcr > 0.6:   return -0.4
        return -0.8

    def _score_fii(self, fii: Optional[FIIDIIData]) -> float:
        """
        FII net equity buying in crores → normalized score.
        Scale: ±2000 crores as the ±1.0 extremes.
        """
        if not fii:
            return 0.0
        net = fii.fii_net_eq
        scale = 2000.0
        return max(-1.0, min(1.0, net / scale))

    def _score_news(self, headlines: list[str]) -> float:
        """VADER NLP on financial news headlines."""
        if not headlines or not VADER_AVAILABLE:
            return 0.0
        scores = [_vader.polarity_scores(h)["compound"] for h in headlines]
        return sum(scores) / len(scores)

    async def _score_breadth(self) -> float:
        """
        Advance/Decline ratio from NSE.
        Returns score: -1 to +1.
        """
        try:
            data = await self.fetcher._nse_get("allIndices")
            total = 0; advancing = 0
            for idx in data.get("data", []):
                if "BROAD" in idx.get("indexType","").upper():
                    a = idx.get("advances", 0)
                    d = idx.get("declines", 0)
                    advancing += a
                    total += a + d
            if not total:
                return 0.0
            ad_ratio = advancing / total
            return (ad_ratio - 0.5) * 2  # scale 0-1 → -1 to +1
        except Exception:
            return 0.0

    def _score_momentum(self) -> float:
        """
        20-period price momentum of Nifty.
        Uses rolling price buffer maintained by engine.
        """
        if len(self._nifty_prices) < 5:
            return 0.0
        lookback = min(20, len(self._nifty_prices))
        old  = self._nifty_prices[-lookback]
        curr = self._nifty_prices[-1]
        if old == 0:
            return 0.0
        pct_chg = (curr - old) / old * 100
        if pct_chg >  5.0: return +1.0
        if pct_chg >  2.0: return +0.6
        if pct_chg >  0.5: return +0.2
        if pct_chg > -0.5: return  0.0
        if pct_chg > -2.0: return -0.2
        if pct_chg > -5.0: return -0.6
        return -1.0

    def update_nifty_price(self, price: float):
        """Called by DataFetcher on each new Nifty tick."""
        self._nifty_prices.append(price)
        if len(self._nifty_prices) > 50:
            self._nifty_prices.pop(0)

    # ── Regime classifier ────────────────────────────────────────
    def _classify_regime(self, score: float,
                          vix: Optional[VIXData]) -> MarketRegime:
        # Special case: extreme VIX
        if vix and vix.vix > 30:
            return MarketRegime.HIGH_VOL
        if score >=  0.60: return MarketRegime.STRONG_BULL
        if score >=  0.25: return MarketRegime.BULL
        if score >= -0.25: return MarketRegime.SIDEWAYS
        if score >= -0.60: return MarketRegime.BEAR
        return MarketRegime.STRONG_BEAR

    def _label_sentiment(self, score: float) -> SentimentLabel:
        if score >=  0.50: return SentimentLabel.VERY_BULLISH
        if score >=  0.20: return SentimentLabel.BULLISH
        if score >= -0.20: return SentimentLabel.NEUTRAL
        if score >= -0.50: return SentimentLabel.BEARISH
        return SentimentLabel.VERY_BEARISH

    # ── Strategy gates ───────────────────────────────────────────
    def _compute_strategy_gates(self, score: float,
                                 vix: Optional[VIXData],
                                 regime: MarketRegime) -> dict:
        vix_val = vix.vix if vix else 18.0
        return {
            "allow_long":          score > -0.40,
            "allow_short":         score < +0.40,
            "allow_options_buy":   vix_val < 35,            # don't buy options in extreme vol
            "allow_options_sell":  vix_val < 20,            # only sell/write in calm markets
        }

    # ── News headlines ───────────────────────────────────────────
    async def _fetch_news_headlines(self) -> list[str]:
        if not self.news_api_key:
            return []
        queries = [
            "Nifty Sensex NSE BSE India stock market",
            "RBI India economy inflation rupee",
            "FII FPI India stocks investment",
        ]
        headlines = []
        async with aiohttp.ClientSession() as session:
            for q in queries:
                try:
                    async with session.get(self.NEWS_API_URL, params={
                        "q": q, "language": "en", "sortBy": "publishedAt",
                        "pageSize": 15, "apiKey": self.news_api_key,
                    }, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        data = await resp.json()
                        for a in data.get("articles", []):
                            headlines.append(
                                (a.get("title") or "") + ". " + (a.get("description") or "")
                            )
                except Exception as e:
                    logger.debug(f"News fetch error: {e}")
        return headlines

    # ── Narrative builder ─────────────────────────────────────────
    def _build_narrative(self, score, vix_data, oc, fii, v_sc, p_sc, f_sc, n_sc) -> str:
        parts = []
        if vix_data:
            level = ("extremely elevated" if vix_data.vix > 25 else
                     "elevated" if vix_data.vix > 20 else
                     "moderate" if vix_data.vix > 15 else "low")
            parts.append(f"India VIX at {vix_data.vix:.2f} ({level} fear)")
        if oc:
            direction = ("bullish (put-heavy)" if oc.pcr_oi > 1.2 else
                         "bearish (call-heavy)" if oc.pcr_oi < 0.8 else "neutral")
            parts.append(f"PCR OI at {oc.pcr_oi:.2f} — {direction}")
        if fii:
            flow = "net buying" if fii.fii_net_eq > 0 else "net selling"
            parts.append(f"FII {flow} ₹{abs(fii.fii_net_eq):.0f} Cr in equities")
        if not parts:
            parts.append("Insufficient data for detailed narrative")
        return ". ".join(parts) + f". Composite score: {score:+.3f}"
