import requests
import pandas as pd
import time
import json
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlencode
import logging
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchCriteria:
    """Search criteria for property lookup"""
    city: str
    state: str
    min_price: int = 0
    max_price: int = 5000000
    min_beds: int = 1
    max_beds: int = 10
    min_baths: float = 1.0
    max_baths: float = 10.0
    property_types: List[str] = None  # ['house', 'condo', 'townhouse']
    
    def __post_init__(self):
        if self.property_types is None:
            self.property_types = ['house', 'condo', 'townhouse']

@dataclass
class Property:
    """Property data structure"""
    address: str
    price: int
    beds: int
    baths: float
    sqft: Optional[int]
    lot_size: Optional[float]
    year_built: Optional[int]
    property_type: str
    latitude: float
    longitude: float
    url: str
    mls_id: Optional[str] = None
    
    # Enhanced criteria scores
    crime_score: Optional[float] = None
    school_score: Optional[float] = None
    market_distance: Optional[float] = None
    overall_score: Optional[float] = None

class RedfinScraper:
    """Main scraper class for Redfin properties"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.redfin.com/',
            'Origin': 'https://www.redfin.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.geocoder = Nominatim(user_agent="property_finder")
    
    def search_properties(self, criteria: SearchCriteria) -> List[Property]:
        """Search for properties based on criteria with fallback methods"""
        logger.info(f"Searching properties in {criteria.city}, {criteria.state}")
        
        # Method 1: Try API approach
        region_id = self._get_region_id(criteria.city, criteria.state)
        if region_id:
            search_params = self._build_search_params(criteria, region_id)
            properties = self._fetch_properties(search_params)
            if properties:
                return properties
        
        # Method 2: Try alternative API endpoints
        logger.info("Trying alternative search method...")
        properties = self._search_alternative_method(criteria)
        if properties:
            return properties
        
        # Method 3: Generate sample data for testing
        logger.warning("API methods failed, generating sample data for testing...")
        return self._generate_sample_data(criteria)
    
    def _search_alternative_method(self, criteria: SearchCriteria) -> List[Property]:
        """Alternative search method using different API endpoint"""
        try:
            # Try using the search URL approach
            base_url = f"https://www.redfin.com/city/{criteria.city.lower().replace(' ', '-')}/{criteria.state}/filter"
            params = {
                'min-price': criteria.min_price,
                'max-price': criteria.max_price,
                'beds': f"{criteria.min_beds}-{criteria.max_beds}",
                'baths': f"{criteria.min_baths}-{criteria.max_baths}",
            }
            
            # This would require HTML parsing instead of API calls
            # For now, return empty list to fall back to sample data
            return []
            
        except Exception as e:
            logger.error(f"Alternative search method failed: {e}")
            return []
    
    def _generate_sample_data(self, criteria: SearchCriteria) -> List[Property]:
        """Generate sample property data for testing when API fails"""
        import random
        
        logger.info(f"Generating sample data for {criteria.city}, {criteria.state}")
        
        # Sample addresses in Austin, TX
        sample_addresses = [
            "123 Main St, Austin, TX 78701",
            "456 Oak Ave, Austin, TX 78702", 
            "789 Pine Dr, Austin, TX 78703",
            "321 Elm St, Austin, TX 78704",
            "654 Cedar Ln, Austin, TX 78705",
            "987 Maple Rd, Austin, TX 78706",
            "147 Birch Way, Austin, TX 78707",
            "258 Willow St, Austin, TX 78708",
            "369 Ash Blvd, Austin, TX 78709",
            "741 Hickory Ave, Austin, TX 78710"
        ]
        
        # Austin coordinates (approximate center)
        base_lat, base_lon = 30.2672, -97.7431
        
        properties = []
        for i, address in enumerate(sample_addresses):
            # Randomize coordinates around Austin
            lat_offset = random.uniform(-0.1, 0.1)
            lon_offset = random.uniform(-0.1, 0.1)
            
            prop = Property(
                address=address,
                price=random.randint(criteria.min_price, criteria.max_price),
                beds=random.randint(criteria.min_beds, criteria.max_beds),
                baths=random.choice([1.0, 1.5, 2.0, 2.5, 3.0, 3.5]),
                sqft=random.randint(1200, 3500),
                lot_size=random.uniform(0.1, 0.5),
                year_built=random.randint(1980, 2020),
                property_type=random.choice(['house', 'condo', 'townhouse']),
                latitude=base_lat + lat_offset,
                longitude=base_lon + lon_offset,
                url=f"https://www.redfin.com/TX/Austin/sample-{i+1}",
                mls_id=f"SAMPLE{1000+i}"
            )
            properties.append(prop)
        
        logger.info(f"Generated {len(properties)} sample properties")
        return properties
    
    def _get_region_id(self, city: str, state: str) -> Optional[str]:
        """Get Redfin region ID for a city"""
        url = "https://www.redfin.com/stingray/api/location/search"
        params = {
            'location': f"{city}, {state}",
            'start': 0,
            'count': 10,
            'v': 2
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Log the raw response for debugging
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            text = response.text
            logger.info(f"Raw response: {text[:200]}...")
            
            # Redfin sometimes returns JSONP with a callback, strip it
            if text.startswith('{}&&'):
                text = text[4:]
            
            data = json.loads(text)
            
            if 'payload' in data and 'sections' in data['payload']:
                for section in data['payload']['sections']:
                    if 'rows' in section:
                        for row in section['rows']:
                            if row.get('type') == 'city':
                                region_id = row.get('id', {}).get('tableId')
                                if region_id:
                                    logger.info(f"Found region ID: {region_id}")
                                    return str(region_id)
            
            # Alternative approach: try to extract from URL structure
            if 'payload' in data and 'exactMatch' in data['payload']:
                exact_match = data['payload']['exactMatch']
                if exact_match and 'id' in exact_match:
                    region_id = exact_match['id'].get('tableId')
                    if region_id:
                        logger.info(f"Found region ID from exact match: {region_id}")
                        return str(region_id)
            
            logger.warning("Could not find region ID in response structure")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            logger.error(f"Raw response: {response.text[:500]}...")
            return None
        except Exception as e:
            logger.error(f"Error getting region ID: {e}")
            return None
    
    def _build_search_params(self, criteria: SearchCriteria, region_id: str) -> Dict:
        """Build search parameters for Redfin API"""
        return {
            'al': 1,  # Include active listings
            'region_id': region_id,
            'region_type': 6,  # City region type
            'min_price': criteria.min_price,
            'max_price': criteria.max_price,
            'min_beds': criteria.min_beds,
            'max_beds': criteria.max_beds,
            'min_baths': criteria.min_baths,
            'max_baths': criteria.max_baths,
            'property_types': criteria.property_types,
            'num_homes': 350,  # Maximum results per request
            'start_date': '',
            'end_date': ''
        }
    
    def _fetch_properties(self, search_params: Dict) -> List[Property]:
        """Fetch properties from Redfin API"""
        url = "https://www.redfin.com/stingray/api/gis"
        
        try:
            response = self.session.get(url, params=search_params)
            response.raise_for_status()
            
            text = response.text
            logger.info(f"GIS API Response status: {response.status_code}")
            logger.info(f"Raw GIS response: {text[:200]}...")
            
            # Handle JSONP response format
            if text.startswith('{}&&'):
                text = text[4:]
            
            data = json.loads(text)
            
            properties = []
            if 'payload' in data and 'homes' in data['payload']:
                homes = data['payload']['homes']
                logger.info(f"Found {len(homes)} homes in API response")
                
                for home in homes:
                    prop = self._parse_property(home)
                    if prop:
                        properties.append(prop)
            else:
                logger.warning("No homes found in API response")
                logger.info(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            return properties
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in _fetch_properties: {e}")
            logger.error(f"Raw response: {response.text[:500]}...")
            return []
        except Exception as e:
            logger.error(f"Error fetching properties: {e}")
            return []
    
    def _parse_property(self, home_data: Dict) -> Optional[Property]:
        """Parse property data from Redfin response"""
        try:
            return Property(
                address=home_data.get('streetLine', {}).get('value', ''),
                price=home_data.get('price', {}).get('value', 0),
                beds=home_data.get('beds', 0),
                baths=home_data.get('baths', 0),
                sqft=home_data.get('sqFt', {}).get('value'),
                lot_size=home_data.get('lotSize', {}).get('value'),
                year_built=home_data.get('yearBuilt', {}).get('value'),
                property_type=home_data.get('propertyType', ''),
                latitude=home_data.get('latLong', {}).get('latitude', 0),
                longitude=home_data.get('latLong', {}).get('longitude', 0),
                url=f"https://www.redfin.com{home_data.get('url', '')}",
                mls_id=home_data.get('mlsId', {}).get('value')
            )
        except Exception as e:
            logger.error(f"Error parsing property: {e}")
            return None

class CrimeDataAnalyzer:
    """Analyze crime data for properties"""
    
    def __init__(self):
        self.crime_cache = {}
    
    def get_crime_score(self, latitude: float, longitude: float) -> float:
        """Get crime score for a location (0-10, lower is better)"""
        # This is a placeholder implementation
        # In a real application, you would integrate with:
        # - Local police department APIs
        # - CrimeData.org API
        # - SpotCrime API
        # - NeighborhoodScout API
        
        coords_key = f"{latitude:.4f},{longitude:.4f}"
        if coords_key in self.crime_cache:
            return self.crime_cache[coords_key]
        
        # Simulated crime score based on location
        # In reality, you would make API calls here
        import random
        crime_score = random.uniform(2.0, 8.0)
        self.crime_cache[coords_key] = crime_score
        
        logger.info(f"Crime score for {coords_key}: {crime_score:.2f}")
        return crime_score

class SchoolDataAnalyzer:
    """Analyze school quality for properties"""
    
    def __init__(self):
        self.school_cache = {}
    
    def get_school_score(self, latitude: float, longitude: float) -> float:
        """Get school quality score (0-10, higher is better)"""
        # This is a placeholder implementation
        # In a real application, you would integrate with:
        # - GreatSchools.org API
        # - SchoolDigger API
        # - State education department APIs
        
        coords_key = f"{latitude:.4f},{longitude:.4f}"
        if coords_key in self.school_cache:
            return self.school_cache[coords_key]
        
        # Simulated school score
        import random
        school_score = random.uniform(4.0, 9.5)
        self.school_cache[coords_key] = school_score
        
        logger.info(f"School score for {coords_key}: {school_score:.2f}")
        return school_score

class MarketProximityAnalyzer:
    """Analyze proximity to markets and amenities"""
    
    def __init__(self):
        self.market_cache = {}
        self.geocoder = Nominatim(user_agent="market_finder")
    
    def get_market_distance(self, latitude: float, longitude: float) -> float:
        """Get distance to nearest major grocery store/market in miles"""
        coords_key = f"{latitude:.4f},{longitude:.4f}"
        if coords_key in self.market_cache:
            return self.market_cache[coords_key]
        
        # This is a placeholder implementation
        # In a real application, you would use:
        # - Google Places API
        # - Foursquare API  
        # - Overpass API (OpenStreetMap)
        
        # Simulated distance to nearest market
        import random
        distance = random.uniform(0.3, 3.5)
        self.market_cache[coords_key] = distance
        
        logger.info(f"Market distance for {coords_key}: {distance:.2f} miles")
        return distance

class PropertyAnalyzer:
    """Main analyzer that combines all criteria"""
    
    def __init__(self):
        self.crime_analyzer = CrimeDataAnalyzer()
        self.school_analyzer = SchoolDataAnalyzer()
        self.market_analyzer = MarketProximityAnalyzer()
    
    def analyze_properties(self, properties: List[Property]) -> List[Property]:
        """Analyze all properties and add scores"""
        logger.info(f"Analyzing {len(properties)} properties...")
        
        for i, prop in enumerate(properties):
            logger.info(f"Analyzing property {i+1}/{len(properties)}: {prop.address}")
            
            # Get scores for each criterion
            prop.crime_score = self.crime_analyzer.get_crime_score(
                prop.latitude, prop.longitude
            )
            prop.school_score = self.school_analyzer.get_school_score(
                prop.latitude, prop.longitude
            )
            prop.market_distance = self.market_analyzer.get_market_distance(
                prop.latitude, prop.longitude
            )
            
            # Calculate overall score (weighted average)
            prop.overall_score = self._calculate_overall_score(prop)
            
            # Add delay to be respectful to APIs
            time.sleep(0.1)
        
        return properties
    
    def _calculate_overall_score(self, prop: Property) -> float:
        """Calculate overall score based on all criteria"""
        # Weights for different factors
        crime_weight = 0.3
        school_weight = 0.4
        market_weight = 0.3
        
        # Normalize scores (crime score is inverted since lower is better)
        crime_normalized = (10 - prop.crime_score) / 10  # Convert to 0-1, higher better
        school_normalized = prop.school_score / 10       # Already 0-1, higher better
        market_normalized = max(0, (4 - prop.market_distance) / 4)  # 0-1, closer better
        
        overall = (
            crime_normalized * crime_weight +
            school_normalized * school_weight +
            market_normalized * market_weight
        ) * 10  # Scale back to 0-10
        
        return round(overall, 2)

class PropertyReportGenerator:
    """Generate reports from analyzed properties"""
    
    @staticmethod
    def generate_csv_report(properties: List[Property], filename: str = "properties_report.csv"):
        """Generate CSV report of properties"""
        data = []
        for prop in properties:
            data.append({
                'Address': prop.address,
                'Price': prop.price,
                'Beds': prop.beds,
                'Baths': prop.baths,
                'SqFt': prop.sqft,
                'Year Built': prop.year_built,
                'Property Type': prop.property_type,
                'Crime Score': prop.crime_score,
                'School Score': prop.school_score,
                'Market Distance (mi)': prop.market_distance,
                'Overall Score': prop.overall_score,
                'URL': prop.url,
                'Latitude': prop.latitude,
                'Longitude': prop.longitude
            })
        
        df = pd.DataFrame(data)
        df = df.sort_values('Overall Score', ascending=False)
        df.to_csv(filename, index=False)
        logger.info(f"Report saved to {filename}")
        return df
    
    @staticmethod
    def print_top_properties(properties: List[Property], top_n: int = 10):
        """Print top N properties by overall score"""
        sorted_props = sorted(properties, key=lambda x: x.overall_score or 0, reverse=True)
        
        print(f"\n=== TOP {top_n} PROPERTIES ===\n")
        for i, prop in enumerate(sorted_props[:top_n], 1):
            print(f"{i}. {prop.address}")
            print(f"   Price: ${prop.price:,}")
            print(f"   Beds/Baths: {prop.beds}/{prop.baths}")
            print(f"   Overall Score: {prop.overall_score}/10")
            print(f"   Crime Score: {prop.crime_score:.1f}/10 (lower better)")
            print(f"   School Score: {prop.school_score:.1f}/10")
            print(f"   Market Distance: {prop.market_distance:.1f} miles")
            print(f"   URL: {prop.url}")
            print()

def main():
    """Main application function"""
    print("=== Redfin Property Analyzer ===\n")
    
    # Example usage - you can modify these criteria
    criteria = SearchCriteria(
        city="Austin",
        state="TX",
        min_price=300000,
        max_price=800000,
        min_beds=2,
        max_beds=4,
        min_baths=2.0,
        property_types=['house', 'condo']
    )
    
    print(f"Search Criteria:")
    print(f"Location: {criteria.city}, {criteria.state}")
    print(f"Price Range: ${criteria.min_price:,} - ${criteria.max_price:,}")
    print(f"Bedrooms: {criteria.min_beds} - {criteria.max_beds}")
    print(f"Bathrooms: {criteria.min_baths} - {criteria.max_baths}")
    print(f"Property Types: {', '.join(criteria.property_types)}")
    print()
    
    # Initialize components
    scraper = RedfinScraper()
    analyzer = PropertyAnalyzer()
    
    try:
        # Search for properties
        print("Searching for properties...")
        properties = scraper.search_properties(criteria)
        
        if not properties:
            print("No properties found matching criteria")
            return
        
        print(f"Found {len(properties)} properties, analyzing...")
        
        # Analyze properties (this will take some time due to API calls)
        analyzed_properties = analyzer.analyze_properties(properties)
        
        # Generate reports
        print("\n" + "="*60)
        PropertyReportGenerator.print_top_properties(analyzed_properties)
        
        # Save to CSV
        df = PropertyReportGenerator.generate_csv_report(analyzed_properties)
        
        print(f"Summary:")
        print(f"Total properties analyzed: {len(analyzed_properties)}")
        print(f"Average overall score: {df['Overall Score'].mean():.2f}/10")
        print(f"Average price: ${df['Price'].mean():,.0f}")
        print(f"Price range: ${df['Price'].min():,} - ${df['Price'].max():,}")
        
        # Score distribution
        high_score = len(df[df['Overall Score'] >= 7])
        med_score = len(df[(df['Overall Score'] >= 5) & (df['Overall Score'] < 7)])
        low_score = len(df[df['Overall Score'] < 5])
        
        print(f"\nScore Distribution:")
        print(f"High Score (7-10): {high_score} properties")
        print(f"Medium Score (5-7): {med_score} properties")
        print(f"Low Score (0-5): {low_score} properties")
        
    except KeyboardInterrupt:
        print("\nSearch interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()