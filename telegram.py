import logging
import os
import requests
from typing import Dict
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.user_id = os.getenv("TELEGRAM_USER_ID")
        
        if not self.bot_token or not self.user_id:
            raise ValueError("Missing Telegram credentials in environment variables")
    
    def send_message(self, message: str) -> bool:
        """Send a message via Telegram Bot API."""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {
                "chat_id": self.user_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            logger.info(f"Telegram message (not sent): {message}")
            return False
    
    def send_scraping_report(self, stats: Dict) -> bool:
        """Send a formatted scraping report."""
        try:
            # Format duration
            duration_str = f"{stats['duration_minutes']}min {stats['duration_seconds']}s"
            
            # Build message
            message = f"""🏆 <b>AUTOSCOUT SCRAPING COMPLETED</b>
─────────────────────
📄 Pages processed: {stats['pages_processed']}
🚗 Cars found: {stats['cars_found']}
✅ New cars: {stats['cars_new']}
🔄 Duplicates: {stats['cars_duplicate']}
⏱️ Duration: {duration_str}
🗓️ Finished: {stats['finished_at']}"""
            
            return self.send_message(message)
        except Exception as e:
            logger.error(f"Error formatting scraping report: {e}")
            return False