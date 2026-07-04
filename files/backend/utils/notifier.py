"""
notifier.py — Telegram alert system
Sends real-time notifications to trader.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class Notifier:
    """Sends alerts via Telegram."""
    
    def __init__(self, token: str = "", chat_id: str = ""):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
    
    def send(self, message: str) -> None:
        """Send a message to Telegram chat."""
        if not self.enabled:
            logger.debug(f"[Telegram disabled] {message}")
            return
        
        try:
            import asyncio
            from telegram import Bot
            
            async def _send():
                bot = Bot(token=self.token)
                await bot.send_message(chat_id=self.chat_id, text=message, parse_mode="HTML")
            
            asyncio.create_task(_send())
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
