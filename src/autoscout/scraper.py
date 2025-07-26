import logging
import time
import requests
import json
import re
from typing import List, Dict, Optional, Set
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode

from .models import FUEL_TYPES, TRANSMISSION_TYPES, CAR_BRANDS, SCRAPING_CONFIG, Listing

logger = logging.getLogger(__name__)

class AutoscoutScraper:
    def __init__(self, db_manager=None):
        self.base_url = "https://www.autoscout24.be/fr/lst"
        self.base_params = {
            'atype': 'C',
            'custtype': 'P',  # Private only
            'cy': 'B',        # Belgium
            'damaged_listing': 'exclude',
            'desc': '1',
            'powertype': 'kw',
            'sort': 'age',
            'source': 'homepage_search-mask',
            'ustate': 'N,U'
        }
        self.session = self._setup_session()
        self.max_pages = SCRAPING_CONFIG['max_pages']
        self.delay_between_requests = SCRAPING_CONFIG['delay_between_requests']
        self.db = db_manager
        self.existing_urls: Set[str] = set()
        self.scraped_listings: List[Dict] = []
    
    def _setup_session(self) -> requests.Session:
        """Setup requests session with proper headers - OPTIMIZED"""
        session = requests.Session()
        
        # Optimize session for performance
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
    
    def scrape_all_listings(self) -> Dict:
        """
        Main scraping workflow - EFFICIENT APPROACH
        Phase 1: Collect all URLs from pages until we hit existing ones
        Phase 2: Check all URLs against database
        Phase 3: Scrape details for all new URLs at once
        Returns: Dict with scraping statistics and results
        """
        logger.info("Starting AutoScout24 scraping process...")
        
        # PHASE 1: PLANNING PHASE
        logger.info("Phase 1: Planning")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Max pages: {self.max_pages}")
        logger.info(f"Delay between requests: {self.delay_between_requests}s")
        logger.info(f"Batch size: {SCRAPING_CONFIG['batch_size']}")
        
        # Load existing URLs for deduplication
        if self.db:
            self._load_existing_urls()
        
        # PHASE 2: URL COLLECTION PHASE
        logger.info("Phase 2: URL Collection")
        all_urls = []
        current_page = 1
        stop_reason = None
        consecutive_existing_pages = 0
        
        while current_page <= self.max_pages:
            try:
                logger.info(f"Processing page {current_page}/{self.max_pages}")
                
                # Extract URLs from current page
                car_urls = self._extract_car_urls_from_page(current_page)
                
                if not car_urls:
                    logger.warning(f"No cars found on page {current_page}")
                    stop_reason = "No more cars found"
                    break
                
                logger.info(f"Found {len(car_urls)} car URLs on page {current_page}")
                
                # Check if any URLs already exist (since listings are sorted by date)
                existing_urls_on_page = [url for url in car_urls if url in self.existing_urls]
                existing_count = len(existing_urls_on_page)
                
                logger.info(f"Found {existing_count} existing URLs on page {current_page}")
                
                # Check if this page has mostly existing URLs (>70%)
                if existing_count > len(car_urls) * 0.7:
                    consecutive_existing_pages += 1
                    logger.info(f"Page {current_page} has mostly existing URLs ({consecutive_existing_pages} consecutive)")
                    
                    # Stop if we have 2 consecutive pages with mostly existing URLs
                    if consecutive_existing_pages >= 2:
                        stop_reason = "Hit existing listings on consecutive pages"
                        break
                else:
                    consecutive_existing_pages = 0  # Reset counter
                
                # Add new URLs from this page
                new_urls = [url for url in car_urls if url not in self.existing_urls]
                all_urls.extend(new_urls)
                logger.info(f"Added {len(new_urls)} new URLs (total: {len(all_urls)})")
                
                # Add delay between pages
                if current_page < self.max_pages:
                    time.sleep(1.5)
                
                current_page += 1
                
            except Exception as e:
                logger.error(f"Error on page {current_page}: {e}")
                stop_reason = f"Error: {str(e)}"
                break
        
        # PHASE 3: DETAIL SCRAPING PHASE
        logger.info(f"Phase 3: Detail Scraping ({len(all_urls)} URLs)")
        
        total_listings_scraped = 0
        for i, car_url in enumerate(all_urls, 1):
            logger.info(f"Scraping detail {i}/{len(all_urls)}: {car_url}")
            
            car_data = self._scrape_car_detail(car_url)
            if car_data:
                self.scraped_listings.append(car_data)
                total_listings_scraped += 1
            
            # Rate limiting
            if i < len(all_urls):
                time.sleep(self.delay_between_requests)
        
        # Return comprehensive results
        results = {
            'total_pages_processed': current_page - 1,
            'total_urls_found': len(all_urls),
            'total_new_urls': len(all_urls),
            'total_listings_scraped': total_listings_scraped,
            'scraped_listings': self.scraped_listings,
            'stop_reason': stop_reason,
            'existing_urls_count': len(self.existing_urls)
        }
        
        logger.info("Scraping completed")
        logger.info(f"Pages processed: {results['total_pages_processed']}")
        logger.info(f"URLs found: {results['total_urls_found']}")
        logger.info(f"New URLs: {results['total_new_urls']}")
        logger.info(f"Listings scraped: {results['total_listings_scraped']}")
        logger.info(f"Existing URLs in DB: {results['existing_urls_count']}")
        logger.info(f"Stop reason: {results['stop_reason']}")
        
        # Return the number of new cars scraped for orchestrator compatibility
        return results['total_listings_scraped']
    
    def _load_existing_urls(self):
        """Load existing URLs from database for deduplication"""
        try:
            if self.db:
                logger.info("Loading existing URLs from database...")
                # Get recent URLs (last 30 days) for better deduplication
                recent_urls = self.db.get_recent_urls(days=30)
                self.existing_urls = set(recent_urls)
                logger.info(f"Loaded {len(self.existing_urls)} existing URLs")
        except Exception as e:
            logger.warning(f"Error loading existing URLs: {e}")
            self.existing_urls = set()
    
    def _filter_new_urls(self, urls: List[str]) -> List[str]:
        """Filter out URLs that already exist"""
        new_urls = [url for url in urls if url not in self.existing_urls]
        return new_urls
    
    def _should_stop_scraping(self) -> bool:
        """Check if we should stop scraping based on various conditions"""
        # Stop if we have too many recent listings (indicating we're hitting old content)
        if len(self.scraped_listings) > 100:
            return True
        
        # Stop if we've processed too many pages without finding new content
        # This would be implemented based on your specific business logic
        
        return False
    
    def _extract_car_urls_from_page(self, page_number: int) -> List[str]:
        """Extract car URLs from a listings page"""
        try:
            # Build page URL
            params = self.base_params.copy()
            params['page'] = page_number
            page_url = f"{self.base_url}?{urlencode(params)}"
            
            logger.info(f"Fetching: {page_url}")
            
            # Fetch page with retry logic
            for attempt in range(3):
                try:
                    response = self.session.get(page_url, timeout=30)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {type(e).__name__}: {e}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
                    else:
                        raise
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find listing elements using the correct selector
            listing_elements = soup.find_all('article', class_='cldt-summary-full-item')
            
            if not listing_elements:
                logger.warning("No listing elements found")
                return []
            
            logger.info(f"Found {len(listing_elements)} listing elements")
            
            # Extract URLs from title links
            urls = []
            for listing_elem in listing_elements:
                title_link = listing_elem.select_one('a.ListItem_title__ndA4s')
                if title_link and title_link.get('href'):
                    href = title_link.get('href')
                    # Convert to absolute URL
                    if href.startswith('/'):
                        full_url = f"https://www.autoscout24.be{href}"
                    else:
                        full_url = href
                    urls.append(full_url)
            
            return list(set(urls))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"Error extracting URLs from page {page_number}: {e}")
            return []
    
    def _scrape_car_detail(self, car_url: str) -> Optional[Dict]:
        """Scrape detailed car information from a detail page"""
        try:
            logger.info(f"Scraping detail page: {car_url}")
            
            # Fetch detail page with retry logic
            response = None
            for attempt in range(3):
                try:
                    response = self.session.get(car_url, timeout=30)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Attempt {attempt + 1} failed for {car_url}: {type(e).__name__}: {e}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
                    else:
                        logger.error(f"All attempts failed for {car_url}")
                        return None
            
            if not response:
                logger.error(f"No response received for {car_url}")
                return None
            
            # Extract JSON-LD data
            car_data = self._extract_json_ld_data(response.text, car_url)
            
            if car_data:
                # Validate essential data
                if self._validate_car_data(car_data):
                    # Format data for consistency
                    formatted_data = self._format_listing(car_data)
                    logger.info(f"Successfully extracted data for {car_data.get('id', 'unknown')}")
                    return formatted_data
                else:
                    logger.warning(f"Insufficient data extracted for {car_url}")
                    return None
            else:
                logger.error(f"No data extracted from {car_url}")
                return None
            
        except Exception as e:
            logger.error(f"Error scraping car detail {car_url}: {e}")
            return None
    
    def _validate_car_data(self, car_data: Dict) -> bool:
        """Validate that essential car data is present for Supabase schema"""
        # Check for required fields (based on Supabase schema)
        required_fields = ['id', 'url', 'source_site']
        for field in required_fields:
            if not car_data.get(field):
                return False
        
        # Check for at least some meaningful data
        meaningful_fields = ['brand', 'model', 'price', 'description']
        meaningful_data_count = sum(1 for field in meaningful_fields if car_data.get(field))
        
        # Require at least 2 meaningful fields
        return meaningful_data_count >= 2
    
    def _extract_json_ld_data(self, html_content: str, car_url: str) -> Optional[Dict]:
        """Extract structured data from JSON-LD in HTML with enhanced parsing"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find JSON-LD script tags
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            car_data = {
                'url': car_url,
                'source_site': 'autoscout24'
            }
            
            # Process all JSON-LD scripts for comprehensive data extraction
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    
                    # Extract vehicle information from Car type
                    if data.get('@type') == 'Car':
                        car_data.update(self._extract_vehicle_info(data))
                    
                    # Extract vehicle information from Product type (AutoScout24 uses Product for cars)
                    if data.get('@type') == 'Product':
                        # Check if this is a car product
                        if 'brand' in data or 'model' in data or 'vehicleEngine' in data:
                            car_data.update(self._extract_vehicle_info(data))
                    
                    # Extract price information from any object with price
                    if 'price' in data:
                        car_data.update(self._extract_price_info(data))
                    
                    # Extract price from offers field (common in Product JSON-LD)
                    if 'offers' in data:
                        car_data.update(self._extract_offers_info(data))
                    
                    # Extract contact information
                    if 'phones' in data or 'contactPoint' in data:
                        car_data.update(self._extract_contact_info(data))
                    
                    # Extract images from any object with image
                    if 'image' in data:
                        car_data.update(self._extract_image_info(data))
                    
                    # Extract title for reference (not stored in DB)
                    if 'name' in data and not car_data.get('title'):
                        car_data['title'] = data.get('name')
                        
                except json.JSONDecodeError:
                    continue
            
            # Extract car ID from URL
            car_id_match = re.search(r'/([a-f0-9-]+)(?:\?|$)', car_url)
            if car_id_match:
                car_data['id'] = car_id_match.group(1)
            else:
                # Try alternative pattern for AutoScout24 URLs
                alt_match = re.search(r'offres/[^/]+/([a-f0-9-]+)', car_url)
                if alt_match:
                    car_data['id'] = alt_match.group(1)
                else:
                    # Try another pattern for the specific URL format
                    alt2_match = re.search(r'([a-f0-9-]{8}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{12})', car_url)
                    if alt2_match:
                        car_data['id'] = alt2_match.group(1)
            
            # Extract additional data from embedded JSON in the page
            car_data.update(self._extract_embedded_json_data(html_content))
            
            return car_data if car_data.get('id') else None
            
        except Exception as e:
            logger.error(f"Error extracting JSON-LD data: {e}")
            return None
    
    def _extract_embedded_json_data(self, html_content: str) -> Dict:
        """Extract data from embedded JSON objects in the HTML page - OPTIMIZED"""
        extracted_data = {}
        
        try:
            # Look for embedded JSON data in script tags - use non-greedy matching
            script_pattern = r'<script[^>]*>.*?window\.__INITIAL_STATE__\s*=\s*({.*?});.*?</script>'
            matches = re.findall(script_pattern, html_content, re.DOTALL)
            
            for match in matches:
                try:
                    data = json.loads(match)
                    extracted_data.update(self._extract_from_initial_state(data))
                except json.JSONDecodeError:
                    continue
            
            # Look for the specific listing data structure we found
            # This targets the listingDetails object in the embedded JSON
            listing_pattern = r'"listingDetails":\s*({[^}]+})'
            listing_matches = re.findall(listing_pattern, html_content, re.DOTALL)
            
            for match in listing_matches:
                try:
                    # Try to parse the listing details
                    listing_data = json.loads(match)
                    extracted_data.update(self._extract_from_listing_details(listing_data))
                except json.JSONDecodeError:
                    continue
            
            # Extract specific fields using targeted patterns
            if not extracted_data.get('year'):
                # Look for firstRegistrationDateRaw
                year_match = re.search(r'"firstRegistrationDateRaw":\s*"(\d{4})-\d{2}-\d{2}"', html_content)
                if year_match:
                    extracted_data['year'] = year_match.group(1)
                else:
                    # Look for firstRegistrationDate
                    year_match = re.search(r'"firstRegistrationDate":\s*"(\d{2})/(\d{4})"', html_content)
                    if year_match:
                        extracted_data['year'] = year_match.group(2)
            
            if not extracted_data.get('mileage'):
                # Look for mileageInKmRaw
                mileage_match = re.search(r'"mileageInKmRaw":\s*(\d+)', html_content)
                if mileage_match:
                    extracted_data['mileage'] = int(mileage_match.group(1))
                else:
                    # Look for mileageInKm
                    mileage_match = re.search(r'"mileageInKm":\s*"([^"]+)"', html_content)
                    if mileage_match:
                        mileage_str = mileage_match.group(1)
                        # Extract numbers from "239 833 km"
                        numbers = re.findall(r'\d+', mileage_str)
                        if numbers:
                            extracted_data['mileage'] = int(''.join(numbers))
            
            if not extracted_data.get('location'):
                # Look for location object
                location_match = re.search(r'"location":\s*{([^}]+)}', html_content)
                if location_match:
                    location_str = location_match.group(1)
                    # Extract both zipcode and city
                    zip_match = re.search(r'"zip":\s*"([^"]+)"', location_str)
                    city_match = re.search(r'"city":\s*"([^"]+)"', location_str)
                    
                    zipcode = zip_match.group(1) if zip_match else ''
                    city = city_match.group(1) if city_match else ''
                    
                    if zipcode and city:
                        # Format: "1000 Brussels" (Belgian format)
                        extracted_data['location'] = f"{zipcode} {city}"
                    elif zipcode:
                        extracted_data['location'] = zipcode
                    elif city:
                        extracted_data['location'] = city
                
                # Also look for location patterns in the HTML
                if not extracted_data.get('location'):
                    # Look for location patterns that include both zipcode and city
                    location_patterns = [
                        r'"location":\s*"([^"]+)"',
                        r'"address":\s*"([^"]+)"',
                        r'Localisation[^"]*["\']([^"\']+)["\']',
                        r'BE-(\d{4})\s+([^,\s]+)',
                        r'(\d{4})\s+([^,\s]+)',
                    ]
                    
                    for pattern in location_patterns:
                        location_match = re.search(pattern, html_content, re.IGNORECASE)
                        if location_match:
                            if pattern == r'BE-(\d{4})\s+([^,\s]+)':
                                # Format: BE-1000 Brussels
                                zipcode = location_match.group(1)
                                city = location_match.group(2)
                                extracted_data['location'] = f"{zipcode} {city}"
                            elif pattern == r'(\d{4})\s+([^,\s]+)':
                                # Format: 1000 Brussels
                                zipcode = location_match.group(1)
                                city = location_match.group(2)
                                extracted_data['location'] = f"{zipcode} {city}"
                            else:
                                # Other patterns
                                extracted_data['location'] = location_match.group(1)
                            break
                    
                    # If still no location, try to find zipcode and city separately
                    if not extracted_data.get('location'):
                        # Look for Belgian zipcode pattern (4 digits)
                        zipcode_match = re.search(r'\b(\d{4})\b', html_content)
                        if zipcode_match:
                            zipcode = zipcode_match.group(1)
                            # Try to find city name near the zipcode
                            city_patterns = [
                                r'(\d{4})\s+([A-Za-zÀ-ÿ\s]+)',
                                r'([A-Za-zÀ-ÿ\s]+)\s+(\d{4})',
                            ]
                            
                            for city_pattern in city_patterns:
                                city_match = re.search(city_pattern, html_content, re.IGNORECASE)
                                if city_match:
                                    if city_match.group(1) == zipcode:
                                        city = city_match.group(2).strip()
                                    else:
                                        city = city_match.group(1).strip()
                                    
                                    if city and len(city) > 2:  # Ensure city name is substantial
                                        extracted_data['location'] = f"{zipcode} {city}"
                                        break
                            
                            # If no city found, just use zipcode
                            if not extracted_data.get('location'):
                                extracted_data['location'] = zipcode
            
            # Extract fuel type if not already found
            if not extracted_data.get('fuel_type'):
                extracted_data['fuel_type'] = self._extract_fuel_type_from_html(html_content)
            
            # Extract transmission if not already found
            if not extracted_data.get('transmission'):
                extracted_data['transmission'] = self._extract_transmission_from_html(html_content)
            
            # Extract description if not already found
            if not extracted_data.get('description'):
                extracted_data['description'] = self._extract_description_from_html(html_content)
            
            # Use simpler, more efficient patterns to avoid catastrophic backtracking
            # Look for specific JSON patterns with limited scope
            if not extracted_data.get('description'):
                # Simple description pattern - avoid complex regex
                desc_match = re.search(r'"description":\s*"([^"]*)"', html_content)
                if desc_match:
                    description = desc_match.group(1).replace('\\u003cbr /\\u003e', '\n')
                    extracted_data['description'] = description
            
            if not extracted_data.get('seller_phone'):
                # Simple phone pattern
                phone_match = re.search(r'"phones":\s*\[([^\]]*)\]', html_content)
                if phone_match:
                    try:
                        phone_data = json.loads(f"[{phone_match.group(1)}]")
                        if phone_data and len(phone_data) > 0:
                            phone = phone_data[0]
                            extracted_data['seller_phone'] = phone.get('formattedNumber', phone.get('callTo', ''))
                    except json.JSONDecodeError:
                        pass
            
            if not extracted_data.get('estimated_price'):
                # Simple evaluation ranges pattern
                eval_match = re.search(r'"evaluationRanges":\s*\[([^\]]*)\]', html_content)
                if eval_match:
                    try:
                        eval_data = json.loads(f"[{eval_match.group(1)}]")
                        for range_data in eval_data:
                            if range_data.get('category') == 1:
                                extracted_data['estimated_price'] = range_data.get('maximum', 0)
                                break
                    except json.JSONDecodeError:
                        pass
            
            if not extracted_data.get('image_url'):
                # Simple images pattern
                img_match = re.search(r'"images":\s*\[([^\]]*)\]', html_content)
                if img_match:
                    try:
                        img_data = json.loads(f"[{img_match.group(1)}]")
                        if img_data:
                            extracted_data['image_url'] = img_data
                    except json.JSONDecodeError:
                        pass
            
            if not extracted_data.get('model'):
                # Simple model pattern
                model_match = re.search(r'"model":\s*"([^"]*)"', html_content)
                if model_match:
                    extracted_data['model'] = model_match.group(1)
                else:
                    # Simple name pattern
                    name_match = re.search(r'"name":\s*"([^"]*)"', html_content)
                    if name_match:
                        name = name_match.group(1)
                        if any(char.isdigit() for char in name) or any(word in name.lower() for word in ['c', 'e', 's', 'a', 'b', 'classe', 'class']):
                            extracted_data['model'] = name
            
        except Exception as e:
            logger.error(f"Error extracting embedded JSON data: {e}")
        
        return extracted_data
    
    def _extract_from_listing_details(self, data: Dict) -> Dict:
        """Extract data from the listingDetails object"""
        extracted = {}
        
        try:
            # Extract vehicle information
            if 'vehicle' in data:
                vehicle = data['vehicle']
                
                # Extract year from firstRegistrationDateRaw
                if 'firstRegistrationDateRaw' in vehicle:
                    date_str = vehicle['firstRegistrationDateRaw']
                    year_match = re.search(r'(\d{4})', date_str)
                    if year_match:
                        extracted['year'] = year_match.group(1)
                
                # Extract mileage from mileageInKmRaw
                if 'mileageInKmRaw' in vehicle:
                    extracted['mileage'] = vehicle['mileageInKmRaw']
                
                # Extract model
                if 'model' in vehicle:
                    extracted['model'] = vehicle['model']
                
                # Extract brand
                if 'make' in vehicle:
                    extracted['brand'] = vehicle['make']
            
            # Extract location
            if 'location' in data:
                location = data['location']
                # Try to combine zipcode and city for better location data
                zipcode = location.get('zip', '')
                city = location.get('city', '')
                
                if zipcode and city:
                    # Format: "1000 Brussels" (Belgian format)
                    extracted['location'] = f"{zipcode} {city}"
                elif zipcode:
                    extracted['location'] = zipcode
                elif city:
                    extracted['location'] = city
            
            # Extract description
            if 'description' in data:
                description = data['description']
                # Clean up HTML entities
                description = description.replace('\\u003cbr /\\u003e', '\n').replace('\\u003cbr /\\u003e\\u003cbr /\\u003e', '\n\n')
                extracted['description'] = description
            
            # Extract images
            if 'images' in data:
                images = data['images']
                if images:
                    extracted['image_url'] = images
            
            # Extract seller information
            if 'seller' in data:
                seller = data['seller']
                if 'phones' in seller and seller['phones']:
                    phone = seller['phones'][0]
                    extracted['seller_phone'] = phone.get('formattedNumber', phone.get('callTo', ''))
                
                if 'contactName' in seller:
                    extracted['seller_name'] = seller['contactName']
            
            # Extract price information
            if 'prices' in data:
                prices = data['prices']
                if 'public' in prices and 'priceRaw' in prices['public']:
                    extracted['price'] = prices['public']['priceRaw']
                
                if 'public' in prices and 'evaluationRanges' in prices['public']:
                    eval_ranges = prices['public']['evaluationRanges']
                    for range_data in eval_ranges:
                        if range_data.get('category') == 1:
                            extracted['estimated_price'] = range_data.get('maximum', 0)
                            break
                        
        except Exception as e:
            logger.error(f"Error extracting from listing details: {e}")
        
        return extracted
    
    def _extract_from_initial_state(self, data: Dict) -> Dict:
        """Extract relevant data from the initial state JSON"""
        extracted = {}
        
        try:
            # Navigate through the nested structure to find listing data
            if 'listing' in data:
                listing = data['listing']
                if 'listing' in listing:
                    listing_data = listing['listing']
                    
                    # Extract description
                    if 'description' in listing_data:
                        description = listing_data['description']
                        # Clean up HTML entities
                        description = description.replace('\\u003cbr /\\u003e', '\n').replace('\\u003cbr /\\u003e\\u003cbr /\\u003e', '\n\n')
                        extracted['description'] = description
                    
                    # Extract model information
                    if 'model' in listing_data:
                        extracted['model'] = listing_data['model']
                    elif 'name' in listing_data:
                        name = listing_data['name']
                        if any(char.isdigit() for char in name) or any(word in name.lower() for word in ['c', 'e', 's', 'a', 'b', 'classe', 'class']):
                            extracted['model'] = name
                    
                    # Extract images
                    if 'images' in listing_data:
                        images = listing_data['images']
                        if images:
                            extracted['image_url'] = images
                    
                    # Extract phone information
                    if 'phones' in listing_data:
                        phones = listing_data['phones']
                        if phones and len(phones) > 0:
                            phone = phones[0]
                            extracted['seller_phone'] = phone.get('formattedNumber', phone.get('callTo', ''))
                    
                    # Extract evaluation ranges for estimated price
                    if 'prices' in listing_data:
                        prices = listing_data['prices']
                        if 'public' in prices and 'evaluationRanges' in prices['public']:
                            eval_ranges = prices['public']['evaluationRanges']
                            for range_data in eval_ranges:
                                if range_data.get('category') == 1:
                                    extracted['estimated_price'] = range_data.get('maximum', 0)
                                    break
                        
        except Exception as e:
            logger.error(f"Error extracting from initial state: {e}")
        
        return extracted
    
    def _extract_vehicle_info(self, data: Dict) -> Dict:
        """Extract comprehensive vehicle specifications"""
        # Handle brand extraction - could be string or dict
        brand = data.get('manufacturer') or data.get('brand')
        if isinstance(brand, dict) and 'name' in brand:
            brand = brand['name']
        
        vehicle_info = {
            'brand': brand,
            'model': data.get('model'),
            'year': self._parse_year(data.get('productionDate') or data.get('dateVehicleFirstRegistered')),
            'mileage': self._extract_mileage(data),
            'fuel_type': self._extract_fuel_type(data.get('vehicleEngine', [])),
            'transmission': self._extract_transmission(data),
            'description': self._clean_description(data.get('description', '')),
        }
        
        return vehicle_info
    

    
    def _extract_price_info(self, data: Dict) -> Dict:
        """Extract price information"""
        price = data.get('price')
        if price:
            try:
                price = int(float(price))
            except (ValueError, TypeError):
                price = None
        
        return {'price': price} if price is not None else {}
    
    def _extract_offers_info(self, data: Dict) -> Dict:
        """Extract price information from offers field"""
        offers = data.get('offers', {})
        if isinstance(offers, dict):
            price = offers.get('price')
            if price:
                try:
                    price = int(float(price))
                    return {'price': price}
                except (ValueError, TypeError):
                    pass
        elif isinstance(offers, list) and offers:
            # Handle list of offers
            for offer in offers:
                if isinstance(offer, dict):
                    price = offer.get('price')
                    if price:
                        try:
                            price = int(float(price))
                            return {'price': price}
                        except (ValueError, TypeError):
                            continue
        
        return {}
    
    def _extract_contact_info(self, data: Dict) -> Dict:
        """Extract comprehensive contact information"""
        contact_info = {}
        
        # Extract phone information
        phones = data.get('phones', [])
        if phones:
            phone = phones[0]  # Take first phone
            contact_info['seller_phone'] = phone.get('formattedNumber') or phone.get('number')
            contact_info['seller_name'] = phone.get('phoneType', 'Private')
        
        # Extract contact point information
        contact_point = data.get('contactPoint', {})
        if contact_point:
            if not contact_info.get('seller_phone'):
                contact_info['seller_phone'] = contact_point.get('telephone')
            if not contact_info.get('seller_name'):
                contact_info['seller_name'] = contact_point.get('name', 'Private')
            contact_info['seller_email'] = contact_point.get('email')
        
        # Extract seller information from other fields
        if 'seller' in data:
            seller = data['seller']
            if isinstance(seller, dict):
                contact_info['seller_name'] = seller.get('name', contact_info.get('seller_name', 'Private'))
                contact_info['seller_email'] = seller.get('email', contact_info.get('seller_email'))
        
        return contact_info
    
    def _extract_image_info(self, data: Dict) -> Dict:
        """Extract image URLs from data"""
        images = data.get('image', [])
        if isinstance(images, str):
            images = [images]
        
        # Filter valid image URLs
        filtered_images = []
        for img in images:
            if isinstance(img, str) and any(ext in img for ext in ['.webp', '.jpg', '.jpeg', '.png']):
                filtered_images.append(img)
        
        # Remove duplicates while preserving order
        unique_images = []
        seen = set()
        for img in filtered_images:
            if img not in seen:
                unique_images.append(img)
                seen.add(img)
        
        return {'image_url': unique_images} if unique_images else {}
    

    
    def _parse_year(self, production_date: str) -> Optional[str]:
        """Parse year from production date"""
        if not production_date:
            return None
        
        try:
            # Extract year from date string
            year_match = re.search(r'(\d{4})', production_date)
            if year_match:
                return year_match.group(1)
        except:
            pass
        
        return None
    
    def _extract_mileage(self, data: Dict) -> Optional[int]:
        """Extract mileage from vehicle data"""
        # Look for mileage in various possible fields
        mileage_fields = ['mileage', 'odometer', 'kilometers']
        
        for field in mileage_fields:
            value = data.get(field)
            if value:
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    continue
        
        return None
    
    def _extract_fuel_type(self, engine_data: List) -> str:
        """Extract fuel type from engine data"""
        if not engine_data:
            return 'Unknown'
        
        for engine in engine_data:
            fuel_type = engine.get('fuelType')
            if fuel_type:
                return fuel_type
        
        return 'Unknown'
    
    def _extract_fuel_type_from_html(self, html_content: str) -> str:
        """Extract fuel type from HTML content using multiple methods"""
        # Method 1: Look for fuelType in JSON-LD
        fuel_patterns = [
            r'"fuelType":\s*"([^"]+)"',
            r'"fuel_type":\s*"([^"]+)"',
        ]
        
        for pattern in fuel_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches and matches[0].strip():
                return matches[0].strip()
        
        # Method 2: Look for fuel type in the URL or title
        if 'diesel' in html_content.lower():
            return 'Diesel'
        elif 'essence' in html_content.lower() or 'gasoline' in html_content.lower():
            return 'Gasoline'
        elif 'electric' in html_content.lower() or 'électrique' in html_content.lower():
            return 'Electric'
        elif 'hybrid' in html_content.lower() or 'hybride' in html_content.lower():
            return 'Hybrid'
        
        return 'Unknown'
    
    def _clean_description(self, description: str) -> str:
        """Clean and decode HTML entities in description"""
        if not description:
            return ''
        
        # Decode HTML entities
        import html
        description = html.unescape(description)
        
        # Replace common HTML tags with newlines
        description = description.replace('\\u003cbr /\\u003e', '\n')
        description = description.replace('\\u003cbr /\\u003e\\u003cbr /\\u003e', '\n\n')
        description = description.replace('<br />', '\n')
        description = description.replace('<br>', '\n')
        description = description.replace('<br/>', '\n')
        
        # Remove other HTML tags
        import re
        description = re.sub(r'<[^>]+>', '', description)
        
        # Clean up extra whitespace
        description = re.sub(r'\n\s*\n', '\n\n', description)
        description = description.strip()
        
        return description
    
    def _extract_description_from_html(self, html_content: str) -> str:
        """Extract description from HTML content using multiple methods"""
        try:
            # Method 1: Look for specific description patterns in HTML
            description_patterns = [
                r'<br /><ul><li>(.*?)</ul><br />Focus Active Business(.*?)Home Safe',
                r'Les bonne nouvelles :(.*?)Home Safe',
                r'Controle technique en ordre(.*?)Home Safe',
                r'<br /><ul><li>(.*?)</ul>',
                r'<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</div>',
                r'<p[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</p>',
            ]
            
            for pattern in description_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)
                if matches:
                    # Handle both single and multiple groups
                    if isinstance(matches[0], tuple):
                        description = ''.join(matches[0])
                    else:
                        description = matches[0]
                    
                    # Clean up the HTML
                    description = re.sub(r'<[^>]+>', '', description)
                    # Decode HTML entities
                    import html
                    description = html.unescape(description)
                    # Replace list items with proper formatting
                    description = re.sub(r'</li><li>', '\n• ', description)
                    description = re.sub(r'<li>', '• ', description)
                    description = re.sub(r'</li>', '', description)
                    
                    # Add line breaks for better formatting
                    description = re.sub(r'(Controle technique)', r'\n\1', description)
                    description = re.sub(r'(Entretien des)', r'\n• \1', description)
                    description = re.sub(r'(Remplacement des)', r'\n• \1', description)
                    description = re.sub(r'(Parfait état)', r'\n• \1', description)
                    description = re.sub(r'(Note :)', r'\n• \1', description)
                    description = re.sub(r'(Focus Active Business)', r'\n\n\1', description)
                    description = re.sub(r'(Teinte:)', r'\n\1', description)
                    description = re.sub(r'(Revêtement:)', r'\n\1', description)
                    description = re.sub(r'(BLIS Blind Spot)', r'\n\1', description)
                    description = re.sub(r'(Vitres arrière)', r'\n\1', description)
                    description = re.sub(r'(Jantes en alliage)', r'\n\1', description)
                    description = re.sub(r'(Attache-remorque)', r'\n\1', description)
                    description = re.sub(r'(Frais de mise)', r'\n\1', description)
                    description = re.sub(r'(Comfort Pack)', r'\n\1', description)
                    description = re.sub(r'(Climatisation automatique)', r'\n\1', description)
                    description = re.sub(r'(Système Keyfree)', r'\n\1', description)
                    description = re.sub(r'(Essuie-glaces automatiques)', r'\n\1', description)
                    
                    # Clean up extra whitespace but preserve line breaks
                    description = re.sub(r' +', ' ', description)
                    description = re.sub(r'\n +', '\n', description)
                    description = description.strip()
                    if description and len(description) > 50:
                        return description
            
            # Method 2: Look for description in JSON patterns
            json_patterns = [
                r'"description":\s*"([^"]+)"',
                r'"text":\s*"([^"]+)"',
                r'"comment":\s*"([^"]+)"',
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    description = matches[0]
                    # Decode HTML entities
                    import html
                    description = html.unescape(description)
                    if description and len(description) > 50:
                        return description
                        
        except Exception as e:
            print(f"HTML description extraction error: {e}")
        
        return ''
    
    def _extract_transmission(self, data: Dict) -> str:
        """Extract transmission type"""
        # Look for transmission in various possible fields
        transmission_fields = ['transmission', 'gearBox', 'vehicleTransmission']
        
        for field in transmission_fields:
            value = data.get(field)
            if value:
                return value
        
        return 'Unknown'
    
    def _extract_transmission_from_html(self, html_content: str) -> str:
        """Extract transmission from HTML content using multiple methods"""
        # Method 1: Look for transmission patterns in HTML
        transmission_patterns = [
            r'boîte\s+manuelle',
            r'boite\s+manuelle', 
            r'boîte\s+automatique',
            r'boite\s+automatique',
            r'manuelle',
            r'automatique',
            r'manual',
            r'automatic',
            r'semi-automatique',
            r'semi-automatic',
        ]
        
        for pattern in transmission_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches and matches[0].strip():
                return matches[0].strip()
        
        # Method 2: Look for transmission in JSON patterns
        json_patterns = [
            r'"transmission":\s*"([^"]+)"',
            r'"vehicleTransmission":\s*"([^"]+)"',
            r'"gearBox":\s*"([^"]+)"',
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches and matches[0].strip():
                return matches[0].strip()
        
        return 'Unknown'
    
    def _format_listing(self, data: Dict) -> Dict:
        """Format listing data to match database standards"""
        formatted = data.copy()
        
        # Format enum fields
        if 'fuel_type' in formatted:
            formatted['fuel_type'] = self._format_fuel_type(formatted['fuel_type'])
        
        if 'transmission' in formatted:
            formatted['transmission'] = self._format_transmission(formatted['transmission'])
            
        if 'brand' in formatted:
            formatted['brand'] = self._format_brand(formatted['brand'])
            
        # Ensure numeric fields are proper types for Supabase schema
        if 'price' in formatted and formatted['price']:
            try:
                formatted['price'] = int(float(formatted['price']))
            except (ValueError, TypeError):
                formatted['price'] = None
                
        if 'estimated_price' in formatted and formatted['estimated_price']:
            try:
                formatted['estimated_price'] = int(float(formatted['estimated_price']))
            except (ValueError, TypeError):
                formatted['estimated_price'] = None
                
        if 'year' in formatted and formatted['year']:
            try:
                # Convert year to date format for Supabase schema
                year_int = int(formatted['year'])
                formatted['year'] = datetime(year_int, 1, 1).date()
            except (ValueError, TypeError):
                formatted['year'] = None
                
        if 'mileage' in formatted and formatted['mileage']:
            try:
                formatted['mileage'] = int(formatted['mileage'])
            except (ValueError, TypeError):
                formatted['mileage'] = None
                
        # Extract and format identifier
        if 'id' in formatted:
            formatted['id'] = str(formatted['id'])
        
        return formatted
    
    def _format_fuel_type(self, value: Optional[str]) -> str:
        """Format a fuel type value to match the standard enum values."""
        if not value:
            return "Unknown"
        
        # Normalize the input
        value = str(value).strip().lower()
        
        # Use mapping for consistent transformation
        fuel_type_map = {
            'essence': 'Gasoline', 'gasoline': 'Gasoline', 'petrol': 'Gasoline',
            'benzine': 'Gasoline', 'b': 'Gasoline', 'diesel': 'Diesel',
            'd': 'Diesel', 'electric': 'Electric', 'électrique': 'Electric',
            'electrique': 'Electric', 'elektro': 'Electric', 'e': 'Electric',
            'hybrid': 'Hybrid', 'hybride': 'Hybrid', 'h': 'Hybrid',
            'lpg': 'Other', 'gpl': 'Other', 'l': 'Other', 'cng': 'Other',
            'gnc': 'Other', 'c': 'Other', 'gas': 'Other'
        }
        
        for k, v in fuel_type_map.items():
            if k in value:
                return v
        
        # Check if value matches any of our standard types (case insensitive)
        for fuel_type in FUEL_TYPES:
            if value == fuel_type.lower():
                return fuel_type
        
        return "Unknown"
    
    def _format_transmission(self, value: Optional[str]) -> str:
        """Format a transmission value to match the standard enum values."""
        if not value:
            return "Unknown"
        
        # Normalize the input
        value = str(value).strip().lower()
        
        # Use mapping for consistent transformation
        transmission_map = {
            'manual': 'Manual', 'manuelle': 'Manual', 'automatic': 'Automatic',
            'automatique': 'Automatic', 'semi-automatic': 'Semi-automatic',
            'semi-automatique': 'Semi-automatic', 'semi': 'Semi-automatic',
            'boîte manuelle': 'Manual', 'boite manuelle': 'Manual',
            'boîte automatique': 'Automatic', 'boite automatique': 'Automatic'
        }
        
        for k, v in transmission_map.items():
            if k in value:
                return v
        
        # Direct matches with enum values (case insensitive)
        for transmission in TRANSMISSION_TYPES:
            if value == transmission.lower():
                return transmission
        
        return "Unknown"
    
    def _format_brand(self, value: Optional[str]) -> str:
        """Format a brand/make value to a standardized format."""
        if not value:
            return ""
        
        # Clean and normalize
        brand = str(value).strip().title()
        
        # Handle common variations and mappings
        brand_mappings = {
            'Vw': 'Volkswagen', 'Bmw': 'BMW', 'Mercedes': 'Mercedes-Benz',
            'Merc': 'Mercedes-Benz', 'Alfa': 'Alfa Romeo', 'Range': 'Land Rover',
            'Mini': 'Mini'
        }
        
        # Apply mappings
        for k, v in brand_mappings.items():
            if brand.lower() == k.lower():
                return v
        
        # Check if it's a known brand
        if brand.upper() in [b.upper() for b in CAR_BRANDS]:
            return brand
        
        return brand if brand else "" 