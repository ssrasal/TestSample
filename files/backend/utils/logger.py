"""
logger.py — Centralized logging setup
Used by main.py and all backend modules.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logging(level: str = "INFO"):
    """
    Configure structured logging to stdout + file.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Root logger
    root = logging.getLogger()
    root.setLevel(log_level)
    
    # Remove existing handlers
    root.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)
    
    # File handler (rotate every 10MB, keep 5 backups)
    try:
        file_handler = RotatingFileHandler(
            "autosignal.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except Exception:
        pass  # Fail silently if log file can't be created
