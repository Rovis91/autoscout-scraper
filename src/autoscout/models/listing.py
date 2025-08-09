"""
Listing Model for AutoScout24 Cars

Represents a car listing from AutoScout24, mapped to the database schema.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from .enums import FUEL_TYPES, TRANSMISSION_TYPES, CAR_BRANDS, SOURCE_SITES

@dataclass
class Listing:
    """
    Represents a car listing from AutoScout24.
    
    Maps to the database schema:
    - id: AutoScout24 car ID (UUID)
    - url: Full detail page URL
    - title: Car title/description
    - price: Price in EUR
    - estimated_price: Same as price for AutoScout24
    - brand: BMW, Audi, etc.
    - model: 318, A4, etc.
    - year: Production year
    - mileage: Kilometers
    - fuel_type: Diesel, Petrol, Electric
    - transmission: Manual, Automatic
    - description: Vehicle description
    - seller_phone: Formatted phone number
    - seller_name: Seller type (Private/Dealer)
    - seller_email: Email (if available)
    - image_url: Array of image URLs
    - location: Location text
    - source_site: 'autoscout24'
    - exists: True
    - last_check_date: Current date
    - created_at: Current timestamp
    - updated_at: Current timestamp
    """
    
    # Required fields
    id: str
    url: str
    source_site: str = "autoscout24"
    
    # Basic information
    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[date] = None  # Changed to date for Supabase schema
    mileage: Optional[int] = None
    price: Optional[int] = None
    estimated_price: Optional[int] = None
    
    # Technical specifications
    fuel_type: Optional[str] = None
    transmission: Optional[str] = None
    
    # Description and details
    description: Optional[str] = None
    
    # Seller information
    seller_name: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_email: Optional[str] = None
    
    # Media and location
    image_url: List[str] = field(default_factory=list)
    location: Optional[str] = None
    
    # Metadata
    exists: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Optional database fields
    source_zipcode_id: Optional[int] = None
    price_history: List[Dict[str, Any]] = field(default_factory=list)
    date_added: Optional[date] = None
    
    def __post_init__(self):
        """Set default values after initialization"""
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
        if self.date_added is None:
            self.date_added = date.today()
        if self.estimated_price is None and self.price is not None:
            self.estimated_price = self.price
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion"""
        return {
            'id': self.id,
            'url': self.url,
            'source_site': self.source_site,
            'title': self.title,
            'brand': self.brand,
            'model': self.model,
            'year': self.year,
            'mileage': self.mileage,
            'price': self.price,
            'estimated_price': self.estimated_price,
            'fuel_type': self.fuel_type,
            'transmission': self.transmission,
            'description': self.description,
            'seller_name': self.seller_name,
            'seller_phone': self.seller_phone,
            'seller_email': self.seller_email,
            'image_url': self.image_url,
            'location': self.location,
            'exists': self.exists,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'source_zipcode_id': self.source_zipcode_id,
            'price_history': self.price_history,
            'date_added': self.date_added
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Listing":
        """Create Listing from dictionary"""
        return cls(**data)
    
    def validate(self) -> bool:
        """Validate that required fields are present"""
        return bool(self.id and self.url and self.source_site)
    
    def get_preview_data(self) -> Dict[str, Any]:
        """Get data for preview/listing display"""
        return {
            'id': self.id,
            'title': self.title or f"{self.brand or 'Unknown'} {self.model or ''}",
            'brand': self.brand,
            'model': self.model,
            'year': self.year,
            'mileage': self.mileage,
            'price': self.price,
            'fuel_type': self.fuel_type,
            'transmission': self.transmission,
            'image_url': self.image_url[0] if self.image_url else None,
            'location': self.location,
            'url': self.url
        }
    
    def get_detail_data(self) -> Dict[str, Any]:
        """Get complete data for detail view"""
        return self.to_dict() 