import logging
import os
from typing import List, Dict, Optional
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        
        if not url or not key:
            raise ValueError("Missing Supabase credentials in environment variables")
        
        self.supabase: Client = create_client(url, key)
    
    def get_recent_urls(self, days: int = 7) -> List[str]:
        """
        Get URLs from recent listings for deduplication purposes
        
        Args:
            days: Number of days to look back (default: 7)
            
        Returns:
            List of URLs from recent listings
        """
        try:
            threshold_date = datetime.now() - timedelta(days=days)
            result = self.supabase.table('listings').select('url').gte('created_at', threshold_date.isoformat()).execute()
            
            urls = [row['url'] for row in result.data if row.get('url')]
            logger.info(f"Retrieved {len(urls)} URLs from last {days} days")
            return urls
            
        except Exception as e:
            logger.error(f"Error retrieving recent URLs: {e}")
            return []
    
    def get_zipcode_id_by_zipcode(self, zipcode: str) -> Optional[int]:
        """
        Get zipcode ID by zipcode number
        
        Args:
            zipcode: Zipcode number (e.g., "4000")
            
        Returns:
            Zipcode ID if found, None otherwise
        """
        try:
            result = self.supabase.table('zipcodes').select('id').eq('zipcode', zipcode).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting zipcode ID for zipcode {zipcode}: {e}")
            return None
    
    def insert_listings_batch(self, listings_data: List[Dict]) -> int:
        """
        Insert multiple listings with robust error handling.
        
        Strategy:
        1. Try batch insertion first (most efficient)
        2. If batch fails, fall back to individual insertions
        3. Track all failures and provide detailed reporting
        4. Ensure no data is lost due to batch failures
        
        Args:
            listings_data: List of processed listing dictionaries
            
        Returns:
            Number of successfully inserted listings
        """
        if not listings_data:
            return 0
        
        total_listings = len(listings_data)
        stored_count = 0
        failed_listings = []
        batch_success_count = 0
        individual_success_count = 0
        
        logger.info(f"Processing {total_listings} listings for database insertion...")
        
        # Strategy 1: Try batch insertion first
        try:
            logger.info(f"Attempting batch insertion of {total_listings} listings...")
            result = self.supabase.table('listings').insert(listings_data).execute()
            stored_count = len(result.data)
            batch_success_count = stored_count
            logger.info(f"Successfully inserted {stored_count}/{total_listings} listings in batch")
            
        except Exception as e:
            logger.error(f"Batch insertion failed: {e}")
            logger.info("Falling back to individual insertions...")
            
            # Strategy 2: Fall back to individual insertions
            for i, listing in enumerate(listings_data):
                try:
                    result = self.supabase.table('listings').insert(listing).execute()
                    stored_count += 1
                    individual_success_count += 1
                    
                    # Progress indicator for large datasets
                    if (i + 1) % 10 == 0:
                        logger.info(f"Processed {i + 1}/{total_listings} listings individually")
                        
                except Exception as e2:
                    error_msg = str(e2)
                    logger.error(f"Failed to insert listing {listing.get('id', 'unknown')}: {error_msg}")
                    failed_listings.append({
                        'listing': listing,
                        'error': error_msg,
                        'id': listing.get('id', 'unknown')
                    })
        
        # Link to users if any listings were successfully inserted
        if stored_count > 0:
            logger.info(f"Linking {stored_count} listings to users...")
            # Only link successfully inserted listings
            successful_listings = [listing for listing in listings_data 
                                 if not any(f['id'] == listing.get('id') for f in failed_listings)]
            self._link_listings_to_users(successful_listings)
        
        # Detailed reporting
        logger.info("Insertion Summary:")
        logger.info(f"Total listings: {total_listings}")
        logger.info(f"Successfully inserted: {stored_count}")
        logger.info(f"Batch insertions: {batch_success_count}")
        logger.info(f"Individual insertions: {individual_success_count}")
        logger.info(f"Failed insertions: {len(failed_listings)}")
        
        if failed_listings:
            logger.warning("Failed listings details:")
            for failure in failed_listings[:5]:  # Show first 5 failures
                logger.warning(f"{failure['id']}: {failure['error']}")
            if len(failed_listings) > 5:
                logger.warning(f"... and {len(failed_listings) - 5} more failures")
        
        return stored_count
    
    def _link_listings_to_users(self, listings: List[Dict]):
        """
        Link new listings to interested users based on preferences
        
        Args:
            listings: List of listing dictionaries
        """
        try:
            # Get all users with preferences
            users_result = self.supabase.table('users').select('id, price_min, price_max, mileage_min, mileage_max, year_min, year_max').execute()
            
            if not users_result.data:
                logger.info("No users found for linking")
                return
            
            # Get user zipcode preferences
            user_zipcodes_result = self.supabase.table('user_zipcodes').select('user_id, zipcode_id').execute()
            user_zipcodes = {}
            for row in user_zipcodes_result.data:
                user_id = row['user_id']
                if user_id not in user_zipcodes:
                    user_zipcodes[user_id] = []
                user_zipcodes[user_id].append(row['zipcode_id'])
            
            linked_count = 0
            # Check each listing against each user's preferences
            for listing in listings:
                for user in users_result.data:
                    if self._matches_user_preferences(listing, user, user_zipcodes.get(user['id'], [])):
                        if self._create_user_listing_link(user['id'], listing['id']):
                            linked_count += 1
            
            logger.info(f"Linked {linked_count} listings to users")
            
        except Exception as e:
            logger.error(f"Error linking listings to users: {e}")
    
    def _matches_user_preferences(self, listing: Dict, user: Dict, user_zipcodes: List[int]) -> bool:
        """
        Check if listing matches user preferences
        
        Args:
            listing: Listing dictionary
            user: User dictionary with preferences
            user_zipcodes: List of user's preferred zipcode IDs
            
        Returns:
            True if listing matches user preferences
        """
        try:
            # Price range check
            listing_price = listing.get('price', 0)
            if listing_price < user.get('price_min', 0) or listing_price > user.get('price_max', 1000000):
                return False
            
            # Mileage range check
            listing_mileage = listing.get('mileage', 0)
            if listing_mileage < user.get('mileage_min', 0) or listing_mileage > user.get('mileage_max', 200000):
                return False
            
            # Year range check
            listing_year = listing.get('year')
            if listing_year:
                try:
                    # Handle both date strings and year integers
                    if isinstance(listing_year, str):
                        year_int = int(listing_year[:4])  # Extract year from date string
                    else:
                        year_int = int(listing_year)
                    
                    year_min = user.get('year_min')
                    year_max = user.get('year_max')
                    
                    if year_min and year_int < int(year_min):
                        return False
                    if year_max and year_int > int(year_max):
                        return False
                except (ValueError, TypeError):
                    pass
            
            # Location check (if user has zipcode preferences)
            if user_zipcodes and listing.get('source_zipcode_id'):
                if listing['source_zipcode_id'] not in user_zipcodes:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking user preferences: {e}")
            return False
    
    def _create_user_listing_link(self, user_id: str, listing_id: str) -> bool:
        """
        Create a link between user and listing
        
        Args:
            user_id: User ID
            listing_id: Listing ID
            
        Returns:
            True if link was created, False if it already exists
        """
        try:
            # Check if link already exists
            existing = self.supabase.table('user_listings').select('id').eq('user_id', user_id).eq('listing_id', listing_id).execute()
            
            if not existing.data:
                # Create new link
                self.supabase.table('user_listings').insert({
                    'user_id': user_id,
                    'listing_id': listing_id,
                    'status': 'nouveau'
                }).execute()
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating user-listing link: {e}")
            return False