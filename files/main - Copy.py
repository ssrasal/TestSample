"""
main.py — AutoSignal Pro entry point.
Run:  python main.py [--paper | --live]
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from utils.logger import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="AutoSignal Pro — Algorithmic Signal Engine")
    p.add_argument("--mode",  choices=["paper","live","api-only"], default="paper")
    p.add_argument("--host",  default="0.0.0.0")
    p.add_argument("--port",  type=int, default=8000)
    p.add_argument("--log",   default="INFO")
    return p.parse_args()


async def run_api_server(host: str, port: int):
    import uvicorn
    from core.api_server import app
    config = uvicorn.Config(
        app, host=host, port=port,
        log_level="warning",
        ws_ping_interval=20,
        ws_ping_timeout=10,
    )
    server = uvicorn.Server(config)
    logger.info(f"Dashboard: http://{host}:{port}/")
    logger.info(f"API Docs:  http://{host}:{port}/docs")
    await server.serve()


async def run_full_system(mode: str, host: str, port: int):
    from workers.engine_worker import EngineWorker
    from risk.risk_manager import RiskConfig

    kite = None
    if mode == "live":
        try:
            from kiteconnect import KiteConnect
            from auth.kite_auth import get_kite_session
            kite = get_kite_session()
            logger.info("Kite session established — LIVE MODE")
        except Exception as e:
            logger.error(f"Kite auth failed: {e}. Falling back to paper.")
            mode = "paper"

    config = RiskConfig(
        total_capital          = float(os.getenv("CAPITAL",          "500000")),
        max_risk_per_trade_pct = float(os.getenv("RISK_PCT",         "1.5")),
        max_daily_loss_pct     = float(os.getenv("MAX_DAILY_LOSS",   "3.0")),
        max_open_positions     = int(os.getenv("MAX_POSITIONS",       "8")),
        vix_halt_threshold     = float(os.getenv("VIX_HALT",         "30.0")),
    )

    engine = EngineWorker(
        kite             = kite,
        config           = config,
        news_api_key     = os.getenv("NEWS_API_KEY",      ""),
        telegram_token   = os.getenv("TELEGRAM_TOKEN",   ""),
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", ""),
    )

    logger.info(f"Mode: {mode.upper()}")
    await asyncio.gather(
        run_api_server(host, port),
        engine.start(),
    )


def main():
    from dotenv import load_dotenv
    load_dotenv()
    args = parse_args()
    setup_logging(level=args.log)
    logger.info("=" * 60)
    logger.info("  AutoSignal Pro v1.0 — Starting")
    logger.info("=" * 60)
    try:
        if args.mode == "api-only":
            asyncio.run(run_api_server(args.host, args.port))
        else:
            asyncio.run(run_full_system(args.mode, args.host, args.port))
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")


if __name__ == "__main__":
    main()
