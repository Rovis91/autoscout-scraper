"""
User Preferences Model

Represents user preferences for car search, mapped to the database schema.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from uuid import UUID

@dataclass
class UserPreferences:
    """
    Represents user preferences for car search.
    
    Maps to the database schema:
    - id: User UUID
    - email: User email
    - price_min/max: Price range preferences
    - mileage_min/max: Mileage range preferences
    - year_min/max: Year range preferences
    """
    
    # Required fields
    id: UUID
    
    # Contact information
    email: Optional[str] = None
    
    # Price preferences
    price_min: Optional[int] = 0
    price_max: Optional[int] = 1000000
    
    # Mileage preferences
    mileage_min: Optional[int] = 0
    mileage_max: Optional[int] = 200000
    
    # Year preferences
    year_min: Optional[date] = None
    year_max: Optional[date] = None
    
    # Additional preferences (extensible)
    preferred_brands: List[str] = field(default_factory=list)
    preferred_fuel_types: List[str] = field(default_factory=list)
    preferred_transmissions: List[str] = field(default_factory=list)
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Set default values after initialization"""
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion"""
        return {
            'id': str(self.id),
            'email': self.email,
            'price_min': self.price_min,
            'price_max': self.price_max,
            'mileage_min': self.mileage_min,
            'mileage_max': self.mileage_max,
            'year_min': self.year_min,
            'year_max': self.year_max,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserPreferences":
        """Create UserPreferences from dictionary"""
        # Convert string ID to UUID if needed
        if isinstance(data.get('id'), str):
            data['id'] = UUID(data['id'])
        return cls(**data)
    
    def matches_listing(self, listing_data: Dict[str, Any]) -> bool:
        """
        Check if a listing matches user preferences.
        
        Args:
            listing_data: Dictionary containing listing information
            
        Returns:
            bool: True if listing matches preferences
        """
        # Price check
        listing_price = listing_data.get('price', 0)
        if listing_price < self.price_min or listing_price > self.price_max:
            return False
        
        # Mileage check
        listing_mileage = listing_data.get('mileage', 0)
        if listing_mileage < self.mileage_min or listing_mileage > self.mileage_max:
            return False
        
        # Year check
        listing_year = listing_data.get('year')
        if listing_year:
            try:
                year_int = int(listing_year)
                if self.year_min and year_int < self.year_min.year:
                    return False
                if self.year_max and year_int > self.year_max.year:
                    return False
            except (ValueError, TypeError):
                pass
        
        # Brand check (if preferences specified)
        if self.preferred_brands:
            listing_brand = listing_data.get('brand', '').upper()
            if not any(brand.upper() in listing_brand for brand in self.preferred_brands):
                return False
        
        # Fuel type check (if preferences specified)
        if self.preferred_fuel_types:
            listing_fuel = listing_data.get('fuel_type', '').upper()
            if listing_fuel not in [fuel.upper() for fuel in self.preferred_fuel_types]:
                return False
        
        # Transmission check (if preferences specified)
        if self.preferred_transmissions:
            listing_transmission = listing_data.get('transmission', '').upper()
            if listing_transmission not in [trans.upper() for trans in self.preferred_transmissions]:
                return False
        
        return True
    
    def get_search_criteria(self) -> Dict[str, Any]:
        """Get search criteria for database queries"""
        criteria = {
            'price_min': self.price_min,
            'price_max': self.price_max,
            'mileage_min': self.mileage_min,
            'mileage_max': self.mileage_max
        }
        
        if self.year_min:
            criteria['year_min'] = self.year_min
        if self.year_max:
            criteria['year_max'] = self.year_max
        if self.preferred_brands:
            criteria['brands'] = self.preferred_brands
        if self.preferred_fuel_types:
            criteria['fuel_types'] = self.preferred_fuel_types
        if self.preferred_transmissions:
            criteria['transmissions'] = self.preferred_transmissions
        
        return criteria 