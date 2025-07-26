"""
AutoScout24 Car Scraper Orchestrator

This script orchestrates the scraping of car listings from AutoScout24
and stores them in a Supabase database. It follows a page-based workflow:

1. Start from page 1, process up to 20 pages
2. Extract car URLs from each page
3. Check for existing URLs in database
4. Fetch detail pages for new cars
5. Store listings and link to users
6. Send completion report via Telegram

Environment variables required:
- SUPABASE_URL
- SUPABASE_SERVICE_KEY  
- TELEGRAM_BOT_TOKEN
- TELEGRAM_USER_ID
"""

import logging
import traceback
from datetime import datetime
from typing import Dict, Any

from db import DatabaseManager
from src.autoscout import AutoscoutScraper
from src.autoscout.data_processor import DataProcessor
from telegram import TelegramNotifier

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('autoscout_scraper.log')
    ]
)
logger = logging.getLogger(__name__)


class AutoscoutOrchestrator:
    """Orchestrates the AutoScout24 scraping workflow."""
    
    def __init__(self):
        """Initialize the orchestrator with all required components."""
        self.db = DatabaseManager()
        self.scraper = AutoscoutScraper(self.db)
        self.data_processor = DataProcessor(self.db)
        self.telegram = TelegramNotifier()
        
        # Statistics counters
        self.stats: Dict[str, Any] = {
            'pages_processed': 0,
            'cars_found': 0,
            'cars_new': 0,
            'cars_duplicate': 0,
            'detail_pages_fetched': 0,
            'errors': 0,
            'start_time': None,
            'last_page_processed': 0,
            'processed_listings': 0,
            'location_mapped': 0
        }
    
    def run(self) -> None:
        """Main orchestration method following the data flow."""
        logger.info("Starting AutoScout24 scraping orchestration...")
        self.stats['start_time'] = datetime.now()
        
        try:
            # Step 1: Scrape all listings (page-based)
            self.scraper.scrape_all_listings()
            
            # Step 2: Process raw listings through pre-upload steps
            if hasattr(self.scraper, 'scraped_listings') and self.scraper.scraped_listings:
                logger.info("Phase 3: Data Processing")
                logger.info(f"Raw listings to process: {len(self.scraper.scraped_listings)}")
                
                # Process listings through data processor
                processed_listings = self.data_processor.process_listings_batch(self.scraper.scraped_listings)
                
                # Step 3: Insert processed listings into database
                if processed_listings:
                    logger.info("Phase 4: Database Insertion")
                    stored_count = self.db.insert_listings_batch(processed_listings)
                    self.stats['cars_new'] = stored_count
                    self.stats['processed_listings'] = len(processed_listings)
                else:
                    logger.warning("No listings to insert after processing")
                    self.stats['cars_new'] = 0
            else:
                logger.warning("No listings found to process")
                self.stats['cars_new'] = 0
            
            # Extract additional statistics from scraper's internal state
            if hasattr(self.scraper, 'scraped_listings'):
                self.stats['cars_found'] = len(self.scraper.scraped_listings)
                self.stats['detail_pages_fetched'] = len(self.scraper.scraped_listings)
            
            # Estimate pages processed based on cars found (20 per page)
            if self.stats['cars_new'] > 0:
                self.stats['pages_processed'] = (self.stats['cars_new'] + 19) // 20  # Ceiling division
            
            # Step 4: Send final report
            self._send_final_report()
            
        except Exception as e:
            logger.error(f"Critical error in orchestration: {e}")
            logger.error(traceback.format_exc())
            self._send_error_report(str(e))
    
    def _send_final_report(self) -> None:
        """Send the final scraping report via Telegram."""
        if not self.stats['start_time']:
            return
        
        # Calculate duration
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        duration_minutes = int(duration.total_seconds() // 60)
        duration_seconds = int(duration.total_seconds() % 60)
        
        # Prepare report data
        report_data = {
            'pages_processed': self.stats['pages_processed'],
            'cars_found': self.stats['cars_found'],
            'cars_new': self.stats['cars_new'],
            'cars_duplicate': self.stats['cars_duplicate'],
            'detail_pages_fetched': self.stats['detail_pages_fetched'],
            'errors': self.stats['errors'],
            'duration_minutes': duration_minutes,
            'duration_seconds': duration_seconds,
            'finished_at': end_time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send report
        success = self.telegram.send_scraping_report(report_data)
        
        if success:
            logger.info("Final report sent successfully")
        else:
            logger.error("Failed to send final report")
        
        # Log summary
        logger.info("Scraping completed successfully")
        logger.info(f"Pages processed: {report_data['pages_processed']}")
        logger.info(f"Cars found: {report_data['cars_found']}")
        logger.info(f"New cars: {report_data['cars_new']}")
        logger.info(f"Duplicates: {report_data['cars_duplicate']}")
        logger.info(f"Processed listings: {self.stats['processed_listings']}")
        logger.info(f"Duration: {duration_minutes}min {duration_seconds}s")
        logger.info(f"Finished: {report_data['finished_at']}")
    
    def _send_error_report(self, error_message: str) -> None:
        """Send error report via Telegram."""
        message = f"❌ <b>AUTOSCOUT SCRAPING FAILED</b>\n\nError: {error_message}"
        self.telegram.send_message(message)


def main() -> None:
    """Main entry point."""
    try:
        orchestrator = AutoscoutOrchestrator()
        orchestrator.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        # Try to send error notification
        try:
            telegram = TelegramNotifier()
            telegram.send_message(f"❌ <b>FATAL ERROR</b>\n\n{str(e)}")
        except Exception as telegram_error:
            logger.error(f"Failed to send Telegram error notification: {telegram_error}")


if __name__ == "__main__":
    main()