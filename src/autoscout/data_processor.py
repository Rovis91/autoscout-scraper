"""
Data Processor for AutoScout24 Listings

Handles the two pre-upload processing steps:
1. City mapping using zipcodes table to get city id of each listing
2. Data formatting based on SQL schema with proper types and default values
"""

import logging
import re
from typing import List, Dict, Optional, Any
from datetime import datetime, date
from dataclasses import asdict

from .models.listing import Listing
from .models.enums import FUEL_TYPES, TRANSMISSION_TYPES, CAR_BRANDS

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Processes raw listing data before database insertion.
    
    Handles:
    - Location mapping to zipcodes table
    - Data type conversion and validation
    - Default value assignment
    - Enum value formatting
    """
    
    def __init__(self, db_manager):
        self.db = db_manager
        self._zipcode_cache = {}  # Cache for zipcode lookups
    
    def process_listings_batch(self, raw_listings: List[Dict]) -> List[Dict]:
        """
        Process a batch of raw listings through both pre-upload steps.
        
        Args:
            raw_listings: List of raw listing dictionaries from scraper
            
        Returns:
            List of processed listings ready for database insertion
        """
        processed_listings = []
        
        logger.info(f"Processing {len(raw_listings)} listings through pre-upload steps...")
        
        for i, raw_listing in enumerate(raw_listings):
            try:
                # Step 1: Create Listing model instance
                listing = self._create_listing_model(raw_listing)
                
                # Step 2: Map location to zipcode
                listing = self._map_location_to_zipcode(listing)
                
                # Step 3: Format and validate data
                listing = self._format_listing_data(listing)
                
                # Step 4: Convert to database-ready dictionary
                db_listing = self._prepare_for_database(listing)
                
                processed_listings.append(db_listing)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(raw_listings)} listings")
                    
            except Exception as e:
                logger.error(f"Error processing listing {raw_listing.get('id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(processed_listings)}/{len(raw_listings)} listings")
        return processed_listings
    
    def _create_listing_model(self, raw_data: Dict) -> Listing:
        """Create a Listing model instance from raw data."""
        try:
            listing_data = {
                'id': str(raw_data.get('id', '')),
                'url': raw_data.get('url', ''),
                'source_site': 'autoscout',
                
                # Basic information
                'title': raw_data.get('title'),
                'brand': raw_data.get('brand'),
                'model': raw_data.get('model'),
                'year': self._parse_year(raw_data.get('year')),
                'mileage': self._parse_numeric(raw_data.get('mileage')),
                'price': self._parse_numeric(raw_data.get('price')),
                'estimated_price': self._parse_numeric(raw_data.get('estimated_price')),
                
                # Technical specifications
                'fuel_type': raw_data.get('fuel_type'),
                'transmission': raw_data.get('transmission'),
                
                # Description and details
                'description': raw_data.get('description'),
                
                # Seller information
                'seller_name': raw_data.get('seller_name'),
                'seller_phone': raw_data.get('seller_phone'),
                'seller_email': raw_data.get('seller_email'),
                
                # Media and location
                'image_url': self._parse_image_urls(raw_data.get('image_url')),
                'location': raw_data.get('location'),
            }
            
            return Listing(**listing_data)
            
        except Exception as e:
            raise ValueError(f"Error creating listing model: {e}")
    
    def _map_location_to_zipcode(self, listing: Listing) -> Listing:
        """
        Map location information to zipcode_id using the zipcodes table.
        """
        try:
            if not listing.location:
                return listing
            
            # Extract zipcode from location
            zipcode = self._extract_zipcode_from_location(listing.location)
            if not zipcode:
                return listing
            
            # Look up zipcode in database
            zipcode_id = self._get_zipcode_id(zipcode)
            if zipcode_id:
                listing.source_zipcode_id = zipcode_id
                logger.info(f"Mapped location '{listing.location}' to zipcode_id {zipcode_id}")
            
            return listing
            
        except Exception as e:
            logger.warning(f"Error mapping location for listing {listing.id}: {e}")
            return listing
    
    def _extract_zipcode_from_location(self, location: str) -> Optional[str]:
        """Extract zipcode from location string."""
        if not location:
            return None
        
        # Belgian zipcode pattern: 4 digits
        zipcode_pattern = r'\b(\d{4})\b'
        match = re.search(zipcode_pattern, location)
        
        if match:
            return match.group(1)
        
        return None
    
    def _get_zipcode_id(self, zipcode: str) -> Optional[int]:
        """Get zipcode_id from database by zipcode number."""
        if not zipcode:
            return None
        
        # Check cache first
        if zipcode in self._zipcode_cache:
            return self._zipcode_cache[zipcode]
        
        try:
            result = self.db.supabase.table('zipcodes').select('id').eq('zipcode', zipcode).execute()
            
            if result.data and len(result.data) > 0:
                zipcode_id = result.data[0]['id']
                # Cache the result
                self._zipcode_cache[zipcode] = zipcode_id
                return zipcode_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error querying zipcode {zipcode}: {e}")
            return None
    
    def _format_listing_data(self, listing: Listing) -> Listing:
        """
        Format listing data with proper types and enum values.
        """
        try:
            # Format enum fields
            if listing.fuel_type:
                listing.fuel_type = self._format_fuel_type(listing.fuel_type)
            
            if listing.transmission:
                listing.transmission = self._format_transmission(listing.transmission)
            
            if listing.brand:
                listing.brand = self._format_brand(listing.brand)
            
            # Ensure numeric fields are proper types
            if listing.price is not None:
                listing.price = int(listing.price) if listing.price > 0 else None
            
            if listing.estimated_price is not None:
                listing.estimated_price = int(listing.estimated_price) if listing.estimated_price > 0 else None
            
            if listing.mileage is not None:
                listing.mileage = int(listing.mileage) if listing.mileage > 0 else None
            
            # Set estimated_price to price if not set
            if listing.estimated_price is None and listing.price is not None:
                listing.estimated_price = listing.price
            
            return listing
            
        except Exception as e:
            raise ValueError(f"Error formatting listing data: {e}")
    
    def _prepare_for_database(self, listing: Listing) -> Dict[str, Any]:
        """
        Convert Listing model to database-ready dictionary.
        """
        try:
            # Convert to dict
            db_data = asdict(listing)
            
            # Handle date serialization for Supabase
            if db_data.get('year'):
                if isinstance(db_data['year'], date):
                    db_data['year'] = db_data['year'].isoformat()
                elif isinstance(db_data['year'], str):
                    try:
                        datetime.strptime(db_data['year'], '%Y-%m-%d')
                    except ValueError:
                        try:
                            year_int = int(db_data['year'])
                            db_data['year'] = date(year_int, 1, 1).isoformat()
                        except (ValueError, TypeError):
                            db_data['year'] = None
            
            # Handle other date fields
            for date_field in ['last_check_date', 'date_added', 'created_at', 'updated_at']:
                if db_data.get(date_field):
                    if isinstance(db_data[date_field], (date, datetime)):
                        db_data[date_field] = db_data[date_field].isoformat()
            
            # Ensure numeric fields are integers
            for numeric_field in ['price', 'mileage', 'estimated_price', 'source_zipcode_id']:
                if db_data.get(numeric_field) is not None:
                    db_data[numeric_field] = int(db_data[numeric_field])
            
            # Ensure boolean field is boolean
            if db_data.get('exists') is not None:
                db_data['exists'] = bool(db_data['exists'])
            
            # Ensure array field is list
            if db_data.get('image_url') is None:
                db_data['image_url'] = []
            
            # Ensure JSON field is list
            if db_data.get('price_history') is None:
                db_data['price_history'] = []
            
            # Remove empty strings for text fields
            text_fields = ['brand', 'model', 'description', 'seller_name', 
                          'seller_phone', 'seller_email', 'location']
            
            for field in text_fields:
                if db_data.get(field) == '':
                    db_data[field] = None
            
            # Remove fields that don't exist in the database schema
            if 'title' in db_data:
                del db_data['title']
            
            # Ensure required fields are present
            required_fields = ['id', 'url', 'source_site', 'exists']
            for field in required_fields:
                if field not in db_data:
                    raise ValueError(f"Missing required field: {field}")
            
            return db_data
            
        except Exception as e:
            raise ValueError(f"Error preparing for database: {e}")
    
    def _parse_year(self, year_value: Any) -> Optional[date]:
        """Parse year value to date object."""
        if not year_value:
            return None
        
        try:
            if isinstance(year_value, date):
                return year_value
            elif isinstance(year_value, str):
                year_str = str(year_value).strip()
                
                # If it's just a year number
                if year_str.isdigit() and len(year_str) == 4:
                    return date(int(year_str), 1, 1)
                
                # Try ISO format
                try:
                    return datetime.strptime(year_str, '%Y-%m-%d').date()
                except ValueError:
                    pass
            
            elif isinstance(year_value, int):
                if 1900 <= year_value <= 2100:
                    return date(year_value, 1, 1)
            
            return None
            
        except Exception:
            return None
    
    def _parse_numeric(self, value: Any) -> Optional[int]:
        """Parse numeric value (price, mileage) to integer."""
        if not value:
            return None
        
        try:
            if isinstance(value, int):
                return value if value > 0 else None
            elif isinstance(value, str):
                # Extract numbers from string like "25 000 €" or "239 833 km"
                numbers = re.findall(r'\d+', value)
                if numbers:
                    return int(''.join(numbers))
            elif isinstance(value, (float, int)):
                value_int = int(float(value))
                return value_int if value_int > 0 else None
            
            return None
            
        except Exception:
            return None
    
    def _parse_image_urls(self, image_data: Any) -> List[str]:
        """Parse image URLs from various formats."""
        if not image_data:
            return []
        
        try:
            if isinstance(image_data, list):
                urls = []
                for item in image_data:
                    if isinstance(item, str):
                        urls.append(item)
                    elif isinstance(item, dict) and 'url' in item:
                        urls.append(item['url'])
                return urls
            elif isinstance(image_data, str):
                return [image_data]
            else:
                return []
                
        except Exception:
            return []
    
    def _format_fuel_type(self, value: str) -> str:
        """Format fuel type to match enum values."""
        if not value:
            return "Unknown"
        
        value = str(value).strip().lower()
        
        # Mapping for common variations
        fuel_mapping = {
            'essence': 'Gasoline',
            'gasoline': 'Gasoline',
            'petrol': 'Gasoline',
            'benzine': 'Gasoline',
            'b': 'Gasoline',
            'diesel': 'Diesel',
            'd': 'Diesel',
            'electric': 'Electric',
            'électrique': 'Electric',
            'electrique': 'Electric',
            'elektro': 'Electric',
            'e': 'Electric',
            'hybrid': 'Hybrid',
            'hybride': 'Hybrid',
            'h': 'Hybrid',
            'lpg': 'Other',
            'gpl': 'Other',
            'l': 'Other',
            'cng': 'Other',
            'gnc': 'Other',
            'c': 'Other',
            'gas': 'Other'
        }
        
        # Check exact matches first
        if value in fuel_mapping:
            return fuel_mapping[value]
        
        # Check direct matches with enum values
        for fuel_type in FUEL_TYPES:
            if value == fuel_type.lower():
                return fuel_type
        
        return "Unknown"
    
    def _format_transmission(self, value: str) -> str:
        """Format transmission to match enum values."""
        if not value:
            return "Unknown"
        
        value = str(value).strip().lower()
        
        # Mapping for common variations
        transmission_mapping = {
            'manual': 'Manual',
            'manuelle': 'Manual',
            'automatic': 'Automatic',
            'automatique': 'Automatic',
            'semi-automatic': 'Semi-automatic',
            'semi-automatique': 'Semi-automatic',
            'semi': 'Semi-automatic'
        }
        
        # Check exact matches first
        if value in transmission_mapping:
            return transmission_mapping[value]
        
        # Check direct matches with enum values
        for transmission in TRANSMISSION_TYPES:
            if value == transmission.lower():
                return transmission
        
        return "Unknown"
    
    def _format_brand(self, value: str) -> str:
        """Format brand to match enum values."""
        if not value:
            return ""
        
        brand = str(value).strip().title()
        
        # Handle common variations and mappings
        brand_mappings = {
            'Vw': 'Volkswagen',
            'Bmw': 'BMW',
            'Mercedes': 'Mercedes-Benz',
            'Merc': 'Mercedes-Benz',
            'Alfa': 'Alfa Romeo',
            'Range': 'Land Rover',
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