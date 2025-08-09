"""
Listing Checker for AutoScout24

This module handles existence and price checking for listings with different intervals:
- Linked listings (in user_listings): checked every 6 hours
- Unlinked listings: checked every week

It leverages the existing scraper infrastructure for consistency.
"""

import logging
import time
import json
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ListingChecker:
    def __init__(self, db_manager):
        self.db = db_manager
        self.linked_check_interval = timedelta(hours=6)
        self.unlinked_check_interval = timedelta(days=7)
        self.linked_batch_size = 10
        self.unlinked_batch_size = 5
        self.request_delay = 2  # Same as existing scraper
        self.existence_timeout = 5
        self.price_timeout = 10
        
    def check_linked_listings(self) -> Dict:
        """Check listings that are linked to users (every 6 hours)"""
        listings = self._get_listings_to_check(linked_only=True, interval=self.linked_check_interval)
        return self._check_listings_batch(listings, check_type="linked")
    
    def check_unlinked_listings(self) -> Dict:
        """Check listings that are not linked to users (every week)"""
        listings = self._get_listings_to_check(linked_only=False, interval=self.unlinked_check_interval)
        return self._check_listings_batch(listings, check_type="unlinked")
    
    def _get_listings_to_check(self, linked_only: bool, interval: timedelta) -> List[Dict]:
        """Get listings that need checking based on updated_at timestamp."""
        cutoff_time = datetime.now() - interval
        
        query = self.db.supabase.table('listings').select(
            'id, url, price, exists, updated_at'
        ).eq('exists', True).lt('updated_at', cutoff_time.isoformat()).order('updated_at', desc=False)
        
        if linked_only:
            # Get only listings that are linked to users
            linked_ids = self._get_linked_listing_ids()
            if linked_ids:
                query = query.in_('id', linked_ids)
            else:
                return []  # No linked listings
        
        result = query.execute()
        return result.data
    
    def _get_linked_listing_ids(self) -> List[str]:
        """Get IDs of listings that are linked to users."""
        try:
            result = self.db.supabase.table('user_listings').select('listing_id').execute()
            return [row['listing_id'] for row in result.data]
        except Exception as e:
            logger.error(f"Failed to get linked listing IDs: {e}")
            return []
    
    def _check_listings_batch(self, listings: List[Dict], check_type: str) -> Dict:
        """Check a batch of listings for existence and price changes."""
        if not listings:
            return {'checked': 0, 'existence_changes': 0, 'price_changes': 0, 'errors': 0, 'type': check_type}
        
        logger.info(f"Checking {len(listings)} {check_type} listings...")
        
        # Use existing scraper's session setup for consistency
        session = self._setup_session()
        
        existence_changes = 0
        price_changes = 0
        errors = 0
        
        # Process in batches
        batch_size = self.linked_batch_size if check_type == "linked" else self.unlinked_batch_size
        
        for i in range(0, len(listings), batch_size):
            batch = listings[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(listings) + batch_size - 1)//batch_size}")
            
            for listing in batch:
                try:
                    # Check existence first
                    exists = self._check_existence(listing['url'], session)
                    
                    if not exists and listing['exists']:
                        # Listing was deleted
                        self._handle_listing_deleted(listing)
                        existence_changes += 1
                        continue
                    
                    if exists:
                        # Check price if listing still exists
                        current_price = self._check_price(listing['url'], session)
                        
                        if current_price and current_price != listing['price']:
                            self._handle_price_change(listing, current_price)
                            price_changes += 1
                    
                    # Update updated_at timestamp
                    self._update_last_checked(listing['id'])
                    
                except Exception as e:
                    logger.error(f"Error checking listing {listing['id']}: {e}")
                    errors += 1
                
                # Rate limiting - same as existing scraper
                time.sleep(self.request_delay)
        
        logger.info(f"Completed {check_type} check: {len(listings)} checked, {existence_changes} deleted, {price_changes} price changes, {errors} errors")
        
        return {
            'checked': len(listings),
            'existence_changes': existence_changes,
            'price_changes': price_changes,
            'errors': errors,
            'type': check_type
        }
    
    def _setup_session(self) -> requests.Session:
        """Reuse existing scraper's session setup."""
        session = requests.Session()
        
        # Use the same headers as the existing scraper
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Referer': 'https://www.autoscout24.be/fr/lst/mercedes-benz?atype=C&custtype=P&cy=B&damaged_listing=exclude&desc=0&powertype=kw&search_id=1yjcruz1o5x&sort=standard&source=homepage_search-mask&ustate=N%2CU'
        })
        
        # Configure session for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def _check_existence(self, url: str, session: requests.Session) -> bool:
        """Check if listing still exists using HEAD request."""
        try:
            response = session.head(url, timeout=self.existence_timeout, allow_redirects=False)
            return 200 <= response.status_code < 400
        except Exception as e:
            logger.warning(f"Existence check failed for {url}: {e}")
            return False  # Assume doesn't exist on error
    
    def _check_price(self, url: str, session: requests.Session) -> Optional[int]:
        """Extract current price from listing page."""
        try:
            response = session.get(url, timeout=self.price_timeout)
            if response.status_code == 200:
                return self._extract_price_from_html(response.text)
        except Exception as e:
            logger.warning(f"Price check failed for {url}: {e}")
        return None
    
    def _extract_price_from_html(self, html: str) -> Optional[int]:
        """Extract price using existing scraper logic."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Method 1: Look for price in JSON-LD data (same as existing scraper)
            script_tags = soup.find_all('script', type='application/ld+json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get('@type') == 'Car':
                        price = data.get('offers', {}).get('price')
                        if price:
                            return int(price)
                except (json.JSONDecodeError, ValueError):
                    continue
            
            # Method 2: Look for embedded JSON data (same as existing scraper)
            script_tags = soup.find_all('script')
            for script in script_tags:
                if script.string and 'window.__INITIAL_STATE__' in script.string:
                    try:
                        # Extract JSON from the script
                        json_start = script.string.find('{')
                        json_end = script.string.rfind('}') + 1
                        if json_start > -1 and json_end > json_start:
                            json_str = script.string[json_start:json_end]
                            data = json.loads(json_str)
                            
                            # Navigate through the JSON structure to find price
                            # This would need to be adapted based on the actual structure
                            if 'listing' in data and 'price' in data['listing']:
                                return int(data['listing']['price'])
                    except (json.JSONDecodeError, ValueError, KeyError):
                        continue
            
            # Method 3: Look for price in HTML (fallback)
            price_pattern = r'(\d{1,3}(?:[.,]\d{3})*)\s*€'
            matches = re.findall(price_pattern, html)
            if matches:
                # Clean and convert to integer
                price_str = matches[0].replace('.', '').replace(',', '')
                return int(price_str)
                
        except Exception as e:
            logger.warning(f"Price extraction failed: {e}")
        
        return None
    
    def _handle_listing_deleted(self, listing: Dict) -> None:
        """Handle when a listing is deleted."""
        try:
            logger.info(f"Listing {listing['id']} was deleted, updating status...")
            
            # Update listing status
            self.db.supabase.table('listings').update({
                'exists': False,
                'updated_at': datetime.now().isoformat()
            }).eq('id', listing['id']).execute()
            
            # Move user listings to trash
            result = self.db.supabase.table('user_listings').update({
                'status': 'corbeille',
                'updated_at': datetime.now().isoformat()
            }).eq('listing_id', listing['id']).execute()
            
            # Create notifications for affected users
            self._create_deletion_notifications(listing['id'])
            
            logger.info(f"Successfully handled deletion of listing {listing['id']}")
            
        except Exception as e:
            logger.error(f"Failed to handle listing deletion for {listing['id']}: {e}")
    
    def _handle_price_change(self, listing: Dict, new_price: int) -> None:
        """Handle when a listing's price changes."""
        try:
            logger.info(f"Price changed for listing {listing['id']}: {listing['price']} -> {new_price}")
            
            # Get current price history
            result = self.db.supabase.table('listings').select('price_history').eq('id', listing['id']).execute()
            current_history = result.data[0].get('price_history', []) if result.data else []
            
            # Add new entry to history
            history_entry = {
                'timestamp': datetime.now().isoformat(),
                'old_price': listing['price'],
                'new_price': new_price,
                'source': 'autoscout24',
                'run_id': datetime.now().strftime('%Y%m%d_%H%M%S')
            }
            
            current_history.append(history_entry)
            
            # Update listing
            self.db.supabase.table('listings').update({
                'price': new_price,
                'price_history': current_history,
                'updated_at': datetime.now().isoformat()
            }).eq('id', listing['id']).execute()
            
            # Create price change notifications
            self._create_price_change_notifications(listing['id'], listing['price'], new_price)
            
            logger.info(f"Successfully handled price change for listing {listing['id']}")
            
        except Exception as e:
            logger.error(f"Failed to handle price change for {listing['id']}: {e}")
    
    def _update_last_checked(self, listing_id: str) -> None:
        """Update the updated_at timestamp for a listing."""
        try:
            self.db.supabase.table('listings').update({
                'updated_at': datetime.now().isoformat()
            }).eq('id', listing_id).execute()
        except Exception as e:
            logger.error(f"Failed to update updated_at for {listing_id}: {e}")
    
    def _create_deletion_notifications(self, listing_id: str) -> None:
        """Create notifications for users when a listing is deleted."""
        try:
            # Get users affected by this listing
            result = self.db.supabase.table('user_listings').select('user_id').eq('listing_id', listing_id).execute()
            
            notifications = []
            for row in result.data:
                notifications.append({
                    'user_id': row['user_id'],
                    'type': 'listing_deleted',
                    'title': 'Annonce supprimée',
                    'message': 'Une annonce que vous suivez a été supprimée du site source.',
                    'data': {'listing_id': listing_id},
                    'created_at': datetime.now().isoformat()
                })
            
            if notifications:
                self.db.supabase.table('notifications').insert(notifications).execute()
                logger.info(f"Created {len(notifications)} deletion notifications for listing {listing_id}")
                
        except Exception as e:
            logger.error(f"Failed to create deletion notifications for {listing_id}: {e}")
    
    def _create_price_change_notifications(self, listing_id: str, old_price: int, new_price: int) -> None:
        """Create notifications for users when a listing's price changes."""
        try:
            # Get users affected by this listing
            result = self.db.supabase.table('user_listings').select('user_id').eq('listing_id', listing_id).execute()
            
            # Format prices for display
            old_price_euros = old_price // 100 if old_price else 0
            new_price_euros = new_price // 100 if new_price else 0
            
            notifications = []
            for row in result.data:
                notifications.append({
                    'user_id': row['user_id'],
                    'type': 'price_changed',
                    'title': 'Prix modifié',
                    'message': f'Le prix d\'une annonce que vous suivez a changé: {old_price_euros:,}€ → {new_price_euros:,}€',
                    'data': {
                        'listing_id': listing_id,
                        'old_price': old_price,
                        'new_price': new_price
                    },
                    'created_at': datetime.now().isoformat()
                })
            
            if notifications:
                self.db.supabase.table('notifications').insert(notifications).execute()
                logger.info(f"Created {len(notifications)} price change notifications for listing {listing_id}")
                
        except Exception as e:
            logger.error(f"Failed to create price change notifications for {listing_id}: {e}")
