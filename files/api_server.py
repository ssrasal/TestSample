"""
api_server.py — STEP 4 Backend
FastAPI server with REST API + WebSocket for real-time signal broadcast.
Designed to serve the React dashboard.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.db_connector import SignalRepository, init_pool, close_pool

logger = logging.getLogger(__name__)
repo   = SignalRepository()

# ── WebSocket connection manager ──────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.add(ws)
        logger.info(f"WS connected. Total: {len(self.active)}")

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self.active.discard(ws)

    async def broadcast(self, message: dict):
        """Broadcast to all connected dashboard clients."""
        if not self.active:
            return
        payload = json.dumps(message, default=str)
        dead = set()
        for ws in list(self.active):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        async with self._lock:
            self.active -= dead

ws_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    logger.info("AutoSignal Pro API ready.")
    yield
    await close_pool()


app = FastAPI(
    title="AutoSignal Pro — Market Analysis API",
    version="1.0.0",
    description="Real-time market signal generation for NSE/BSE instruments",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ════════════════════════════════════════════════════════════════
#  REST ENDPOINTS
# ════════════════════════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat(), "version": "1.0.0"}


@app.get("/api/signals/active")
async def active_signals():
    """All currently active signals across all instrument types."""
    data = await repo.get_active_signals()
    return {"signals": data, "count": len(data), "timestamp": datetime.now().isoformat()}


@app.get("/api/signals/{instrument_type}")
async def signals_by_type(
    instrument_type: str,
    status:    Optional[str] = Query(None),
    limit:     int            = Query(100, ge=1, le=500),
):
    """
    Fetch signals for a specific instrument type.
    instrument_type: stock | stock_opt | stock_fut | nifty_fut | nifty_opt | sensex_fut | sensex_opt
    """
    TABLE_MAP = {
        "stock":      "stock_calls",
        "stock_opt":  "stock_options_calls",
        "stock_fut":  "stock_futures_calls",
        "nifty_fut":  "nifty_futures_calls",
        "nifty_opt":  "nifty_options_calls",
        "sensex_fut": "sensex_futures_calls",
        "sensex_opt": "sensex_options_calls",
    }
    table = TABLE_MAP.get(instrument_type.lower())
    if not table:
        raise HTTPException(400, f"Unknown instrument type: {instrument_type}")
    data = await repo.get_calls_by_table(table, limit=limit, status=status)
    return {"signals": data, "instrument": instrument_type, "count": len(data)}


@app.get("/api/performance/instruments")
async def instrument_performance():
    """Win rate, profit factor per instrument type."""
    data = await repo.get_instrument_performance()
    return {"performance": data}


@app.get("/api/performance/daily")
async def daily_performance(days: int = Query(30, ge=1, le=365)):
    data = await repo.get_daily_performance(days)
    return {"daily": data}


@app.get("/api/performance/validate")
async def validate_performance(
    instrument: Optional[str] = Query(None),
    date_from:  Optional[str] = Query(None),
    date_to:    Optional[str] = Query(None),
):
    """
    Full validation report: win rate, profit factor, avg win/loss,
    best/worst trade, SL hit rate, target hit rate — per table.
    """
    data = await repo.validate_performance(
        table=instrument, from_date=date_from, to_date=date_to
    )
    return {"validation": data, "generated_at": datetime.now().isoformat()}


@app.get("/api/sentiment/latest")
async def latest_sentiment():
    """Most recent market sentiment snapshot."""
    from core.db_connector import acquire
    async with acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM market_sentiment_log ORDER BY computed_at DESC LIMIT 1"
        )
    return dict(row) if row else {}


@app.get("/api/signals/detail/{table}/{signal_id}")
async def signal_detail(table: str, signal_id: str):
    """Full detail for a single signal including hedge spec."""
    from core.db_connector import acquire
    valid_tables = {
        "stock_calls","stock_options_calls","stock_futures_calls",
        "nifty_futures_calls","nifty_options_calls",
        "sensex_futures_calls","sensex_options_calls"
    }
    if table not in valid_tables:
        raise HTTPException(400, "Invalid table name")
    async with acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {table} WHERE id=$1::uuid", signal_id
        )
    if not row:
        raise HTTPException(404, "Signal not found")
    return dict(row)


class ManualCloseRequest(BaseModel):
    table:       str
    signal_id:   str
    exit_price:  float
    actual_pnl:  float
    notes:       Optional[str] = None


@app.post("/api/signals/close")
async def manual_close(req: ManualCloseRequest):
    """Manually close an active signal."""
    valid_tables = {
        "stock_calls","stock_options_calls","stock_futures_calls",
        "nifty_futures_calls","nifty_options_calls",
        "sensex_futures_calls","sensex_options_calls"
    }
    if req.table not in valid_tables:
        raise HTTPException(400, "Invalid table")
    await repo.close_call(req.table, req.signal_id,
                          req.exit_price, "MANUALLY_CLOSED", req.actual_pnl)
    await ws_manager.broadcast({
        "event": "SIGNAL_CLOSED",
        "signal_id": req.signal_id,
        "table": req.table,
        "exit_price": req.exit_price,
        "pnl": req.actual_pnl,
    })
    return {"message": "Signal closed successfully"}


@app.get("/api/events")
async def system_events(limit: int = Query(50, ge=1, le=200)):
    from core.db_connector import acquire
    async with acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM system_events ORDER BY created_at DESC LIMIT $1", limit
        )
    return {"events": [dict(r) for r in rows]}


# ════════════════════════════════════════════════════════════════
#  WEBSOCKET — Real-time signal broadcast
# ════════════════════════════════════════════════════════════════

@app.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        # Send current active signals on connect
        signals = await repo.get_active_signals()
        await websocket.send_text(json.dumps({
            "event": "INITIAL_STATE",
            "signals": signals,
        }, default=str))
        # Keep connection alive
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"event": "PING"}))
    except WebSocketDisconnect:
        pass
    finally:
        await ws_manager.disconnect(websocket)


# ── Called by engine when new signal is generated ─────────────
async def broadcast_new_signal(signal_dict: dict):
    await ws_manager.broadcast({"event": "NEW_SIGNAL", "signal": signal_dict})


async def broadcast_signal_update(signal_id: str, status: str, pnl: float = None):
    await ws_manager.broadcast({
        "event": "SIGNAL_UPDATE",
        "signal_id": signal_id,
        "status": status,
        "pnl": pnl,
    })
