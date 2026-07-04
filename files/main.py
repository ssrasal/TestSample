"""
main.py — AutoSignal Pro entry point.
Run:  python main.py --mode paper
      python main.py --mode live
      python main.py --mode api-only
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

# ── Fix Python path FIRST — before any project imports ─────────────────────────
# Adds  <project_root>/backend  to sys.path so that
# "from utils.logger import ..."  "from core.api_server import ..." etc. all work
# regardless of which directory you run the script from.
ROOT_DIR    = Path(__file__).resolve().parent          # …/algo_trading_system/
BACKEND_DIR = ROOT_DIR / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ── Now safe to import project modules ─────────────────────────────────────────
from utils.logger import setup_logging          # backend/utils/logger.py
import logging

setup_logging()
logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="AutoSignal Pro — Algorithmic Signal Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["paper", "live", "api-only"],
        default="paper",
        help="paper = no real orders | live = live broker | api-only = dashboard only",
    )
    p.add_argument("--host",  default="0.0.0.0",  help="API server bind host")
    p.add_argument("--port",  type=int, default=8000, help="API server port")
    p.add_argument("--log",   default="INFO",      help="Log level: DEBUG/INFO/WARNING")
    return p.parse_args()


# ───────────────────────────────────────────────────────────────────────────────
async def run_api_server(host: str, port: int) -> None:
    """Start the FastAPI + WebSocket server."""
    try:
        import uvicorn
    except ImportError:
        logger.error("uvicorn not installed. Run:  pip install uvicorn[standard]")
        sys.exit(1)

    from core.api_server import app   # backend/core/api_server.py

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        ws_ping_interval=20,
        ws_ping_timeout=10,
    )
    server = uvicorn.Server(config)
    logger.info(f"Dashboard → http://{host if host != '0.0.0.0' else 'localhost'}:{port}/")
    logger.info(f"API Docs  → http://{host if host != '0.0.0.0' else 'localhost'}:{port}/docs")
    await server.serve()


# ───────────────────────────────────────────────────────────────────────────────
async def run_full_system(mode: str, host: str, port: int) -> None:
    """Start the signal engine + API server together."""
    from workers.engine_worker import EngineWorker   # backend/workers/engine_worker.py
    from risk.risk_manager import RiskConfig          # backend/risk/risk_manager.py

    # ── Optional: connect to live broker ───────────────────────────────────────
    kite = None
    if mode == "live":
        try:
            from kiteconnect import KiteConnect
            # Simple token-file auth for paper trading on Kite sandbox
            api_key    = os.getenv("ZERODHA_API_KEY", "")
            api_secret = os.getenv("ZERODHA_API_SECRET", "")
            token_file = ROOT_DIR / ".kite_token"

            if not api_key or not api_secret:
                raise ValueError("ZERODHA_API_KEY / ZERODHA_API_SECRET not set in .env")

            kite = KiteConnect(api_key=api_key)

            if token_file.exists():
                kite.set_access_token(token_file.read_text().strip())
                logger.info("Kite access token loaded from .kite_token")
            else:
                login_url = kite.login_url()
                logger.warning(f"No .kite_token found. Open this URL to log in:\n{login_url}")
                request_token = input("Paste request_token from redirect URL: ").strip()
                session = kite.generate_session(request_token, api_secret=api_secret)
                kite.set_access_token(session["access_token"])
                token_file.write_text(session["access_token"])
                logger.info("Kite session created and token saved.")

        except Exception as exc:
            logger.error(f"Kite auth failed: {exc} — falling back to PAPER mode.")
            kite = None
            mode = "paper"

    # ── Risk config from .env ──────────────────────────────────────────────────
    config = RiskConfig(
        total_capital           = float(os.getenv("CAPITAL",         "500000")),
        max_risk_per_trade_pct  = float(os.getenv("RISK_PCT",        "1.5")),
        max_daily_loss_pct      = float(os.getenv("MAX_DAILY_LOSS",  "3.0")),
        max_open_positions      = int(  os.getenv("MAX_POSITIONS",   "8")),
        vix_halt_threshold      = float(os.getenv("VIX_HALT",        "30.0")),
    )

    engine = EngineWorker(
        kite             = kite,
        config           = config,
        news_api_key     = os.getenv("NEWS_API_KEY",      ""),
        telegram_token   = os.getenv("TELEGRAM_TOKEN",    ""),
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID",  ""),
    )

    logger.info(f"Mode: {mode.upper()}")
    # Run API server and engine concurrently
    await asyncio.gather(
        run_api_server(host, port),
        engine.start(),
    )


# ───────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # Load .env file (must exist; copy from .env.example if missing)
    env_file = ROOT_DIR / ".env"
    if not env_file.exists():
        logger.warning(
            ".env not found — copy .env.example to .env and fill in your credentials.\n"
            f"  Expected location: {env_file}"
        )

    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        logger.warning("python-dotenv not installed. Reading env vars from shell only.")

    args = parse_args()
    setup_logging(level=args.log)

    logger.info("=" * 60)
    logger.info("  AutoSignal Pro v1.0 — Starting")
    logger.info(f"  Mode     : {args.mode.upper()}")
    logger.info(f"  Backend  : {BACKEND_DIR}")
    logger.info(f"  Log level: {args.log}")
    logger.info("=" * 60)

    try:
        if args.mode == "api-only":
            asyncio.run(run_api_server(args.host, args.port))
        else:
            asyncio.run(run_full_system(args.mode, args.host, args.port))
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user (Ctrl+C).")
    except Exception as exc:
        logger.exception(f"Fatal error: {exc}")
        sys.exit(1)


# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
