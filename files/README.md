# AutoSignal Pro
### Production-Ready Algorithmic Signal Generation System for NSE/BSE

```
AutoSignal Pro — System Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 REGIME:     BULL  │  VIX: 15.3  │  PCR: 1.18
 SENTIMENT:  +0.42 (BULLISH)
 SIGNALS:    12 active  │  8 positions open
 DAILY P&L:  +₹18,432 (+1.84%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## What Was Built (Complete Deliverables)

| Step | Component | Files |
|------|-----------|-------|
| 1 | Architecture Blueprint | `architecture/ARCHITECTURE.md` |
| 2 | PostgreSQL 18 DDL | `database/schema.sql` |
| 3A | DataFetcher | `backend/data/data_fetcher.py` |
| 3B | SentimentAdaptiveEngine | `backend/core/sentiment_engine.py` |
| 3C | SignalGenerator + Hedge Logic | `backend/core/signal_generator.py` |
| 3D | DB Connector (asyncpg pool) | `backend/core/db_connector.py` |
| 3E | FastAPI + WebSocket server | `backend/core/api_server.py` |
| 3F | Risk Manager (Kelly + Circuit Breaker) | `backend/risk/risk_manager.py` |
| 3G | Strategy Router | `backend/strategy/strategy_router.py` |
| 3H | Engine Worker (Master orchestrator) | `backend/workers/engine_worker.py` |
| 4 | Dashboard UI (HTML/JS) | `frontend/dashboard.html` |
| 5 | QA Test Suite | `tests/test_suite.py` |

---

## Quick Start

### 1. Prerequisites
```bash
# Python 3.11+
# PostgreSQL 18
# (Optional) Zerodha KiteConnect API credentials
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Database
```bash
cp .env.example .env       # fill in your credentials
chmod +x scripts/setup_db.sh
./scripts/setup_db.sh
```

### 4. Run the System

**Paper trading mode (no live broker):**
```bash
python main.py --mode paper
```

**API only (dashboard without engine):**
```bash
python main.py --mode api-only
```

**Live trading with KiteConnect:**
```bash
python main.py --mode live
```

### 5. Access Dashboard
```
http://localhost:8000/
```

### 6. Run Tests
```bash
# Unit tests only (no DB required)
pytest tests/ -v -m "not integration"

# All tests (requires PostgreSQL)
pytest tests/ -v

# With coverage report
pytest tests/ --cov=backend --cov-report=html
```

---

## Database Tables

| Table | Description |
|-------|-------------|
| `stock_calls` | Cash equity signals (intraday/swing) |
| `stock_options_calls` | Individual stock options CE/PE |
| `stock_futures_calls` | Single-stock futures |
| `nifty_futures_calls` | Nifty 50 index futures |
| `nifty_options_calls` | Nifty options (CE/PE) with spreads |
| `sensex_futures_calls` | BSE Sensex futures |
| `sensex_options_calls` | Sensex options (CE/PE) |
| `instruments` | Master instrument reference |
| `ohlcv_candles` | Partitioned OHLCV history |
| `option_chain_snapshots` | OC snapshots + PCR |
| `option_chain_strikes` | Strike-level Greeks |
| `india_vix` | VIX time series |
| `fii_dii_activity` | Institutional flow data |
| `market_sentiment_log` | Computed sentiment snapshots |
| `hedge_executions` | Hedge leg audit trail |
| `daily_performance` | EOD summary + metrics |
| `system_events` | Engine event log |

---

## Signal Structure (Every Signal Includes)

```python
Signal:
  ├── Entry Price Range (Low / High)
  ├── Target 1, Target 2 (with % gain)
  ├── Stop Loss (ATR-based + swing-high/low anchor)
  ├── Trailing SL Rule (ATR method, with trigger)
  ├── Risk/Reward Ratio (minimum 1.5x enforced)
  ├── Signal Score (-10 to +10, 10-factor model)
  ├── Confidence Grade (A+ / A / B+ / B / C)
  └── Hedge Specification (MANDATORY for all F&O):
      ├── Hedge Strategy (Bull Put Spread, Bear Call Spread,
      │                   Iron Condor, Protective Put, Collar…)
      ├── Hedge Leg Details (strike, type, lots, premium est.)
      ├── Max Loss (defined-risk trades)
      ├── Breakeven Points
      └── Human-readable rationale
```

---

## Architecture Summary

```
NSE/BSE/KiteConnect → DataFetcher (circuit breaker + cache)
                    ↓
              asyncio.Queue (per instrument type)
                    ↓
        SentimentAdaptiveEngine (VIX+PCR+FII+NLP+Breadth)
                    ↓
           StrategyRouter (regime-based dispatch)
                    ↓
          SignalGenerator (10-factor scoring + hedge)
                    ↓
           RiskManager (Kelly sizing + all guards)
                    ↓
    ┌──────────────────────────────────┐
    │    asyncpg Pool → PostgreSQL 18  │
    │    WebSocket → Dashboard         │
    │    Telegram Notifier             │
    └──────────────────────────────────┘
```

---

## Risk Controls

| Guard | Threshold | Action |
|-------|-----------|--------|
| Daily loss | -3% of capital | Halt all new signals |
| VIX spike | > 30 | No new trades |
| Consecutive losses | 4 in a row | Pause + alert |
| SL too wide | > 8% from entry | Reject signal |
| SL too tight | < 0.3% from entry | Reject (noise) |
| RRR too low | < 1.5x | Reject signal |
| Max positions | 8 open | Queue new signals |
| F&O exposure | > 40% of capital | Block F&O entries |

---

## Adding a New Instrument

1. Add instrument to `instruments` table
2. Add a new call table in `database/schema.sql`
3. Add insert method to `SignalRepository` in `db_connector.py`
4. Add generator method in `SignalGenerator`
5. Add route in `StrategyRouter._generate()`
6. Add symbol to the relevant producer loop in `engine_worker.py`
7. Add corresponding unit test in `tests/test_suite.py`

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | System health check |
| GET | `/api/signals/active` | All active signals (unified view) |
| GET | `/api/signals/{type}` | Signals by instrument type |
| GET | `/api/performance/instruments` | Win rate + P&L per instrument |
| GET | `/api/performance/validate` | Full validation report |
| GET | `/api/performance/daily` | Daily P&L history |
| GET | `/api/sentiment/latest` | Latest sentiment snapshot |
| GET | `/api/events` | System event log |
| POST | `/api/signals/close` | Manually close a signal |
| WS | `/ws/signals` | Real-time signal stream |

---

## Disclaimer

This system is for **educational and research purposes only**.
Algorithmic trading involves significant financial risk.
Past signal performance does not guarantee future results.
Always paper-trade first and consult a SEBI-registered advisor
before deploying real capital.
