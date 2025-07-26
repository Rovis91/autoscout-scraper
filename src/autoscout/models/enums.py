"""
Enum Definitions for AutoScout24 Scraper

Contains all enum values and constants used throughout the scraper.
"""

# Fuel type enum values
FUEL_TYPES = [
    'Gasoline',
    'Diesel', 
    'Electric',
    'Hybrid',
    'Other',
    'Unknown'
]

# Transmission type enum values
TRANSMISSION_TYPES = [
    'Manual',
    'Automatic',
    'Semi-automatic',
    'Unknown'
]

# Car brand enum values
CAR_BRANDS = [
    'Audi',
    'BMW',
    'Citroen',
    'Cupra',
    'Dacia',
    'Fiat',
    'Ford',
    'Honda',
    'Hyundai',
    'Jaguar',
    'Jeep',
    'Kia',
    'Land Rover',
    'Lexus',
    'Mazda',
    'Mercedes-Benz',
    'MG',
    'Mini',
    'Mitsubishi',
    'Nissan',
    'Oldtimer',
    'Opel',
    'Peugeot',
    'Piaggio',
    'Polestar',
    'Porsche',
    'Renault',
    'Rover',
    'Seat',
    'Skoda',
    'Smart',
    'Subaru',
    'Suzuki',
    'Tesla',
    'Toyota',
    'Volkswagen',
    'Volvo',
    'Other'
]

# Source site enum values
SOURCE_SITES = [
    'autoscout',
    'leboncoin'
]

# Scraping configuration - OPTIMIZED
SCRAPING_CONFIG = {
    'max_pages': 20,
    'delay_between_requests': 1,  # Reduced from 2 to 1 second for better performance
    'batch_size': 20,
    'timeout': 30
} 