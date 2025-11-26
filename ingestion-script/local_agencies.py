#!/usr/bin/env python3
"""
eCFR Agencies and Titles Data Ingestion for Local PostgreSQL

Collects agencies data from eCFR Admin API and titles data from eCFR versioner API
and stores in local PostgreSQL database.
Fields collected for agencies: name, display_name, slug, children (JSONB), cfr_references
Fields collected for titles: title_number, title_name, title_abbreviation, chapter_count, is_reserved
"""

import psycopg2
import psycopg2.extras
import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('local_postgres_agencies_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LocalPostgresAgenciesIngestion:
    def __init__(self):
        """Initialize the agencies ingestion system"""
        # API configuration
        self.api_base = "https://www.ecfr.gov/api/admin/v1"
        self.titles_api_url = "https://www.ecfr.gov/api/versioner/v1/titles.json"
        # Local PostgreSQL configuration
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.database_name = os.getenv('DB_NAME', 'ecfrdb')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'XYZ')
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'eCFR-Local-Postgres-Ingestion/1.0',
            'Accept': 'application/json'
        })
        self.db_conn = None
        self.setup_database()
    
    def setup_database(self):
        """Setup local PostgreSQL database"""
        try:
            # Connect to PostgreSQL
            self.db_conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.database_name,
                user=self.db_user,
                password=self.db_password,
                cursor_factory=psycopg2.extras.DictCursor
            )
            self.db_conn.autocommit = True
            
            # Create cursor with dict-like access
            cursor = self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Create agencies table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS agencies (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    display_name TEXT,
                    slug TEXT UNIQUE,
                    children JSONB,
                    cfr_references JSONB,
                    raw_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create agencies table indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_agencies_slug ON agencies(slug)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_agencies_name ON agencies(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_agencies_children_gin ON agencies USING GIN(children)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_agencies_cfr_refs_gin ON agencies USING GIN(cfr_references)')
            # Create titles table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS titles (
            id SERIAL PRIMARY KEY,
            title_number INTEGER UNIQUE NOT NULL,
            title_name TEXT NOT NULL,
            title_abbreviation TEXT,
            chapter_count INTEGER DEFAULT 0,
            is_reserved BOOLEAN DEFAULT FALSE,
            latest_amended_on DATE,
            latest_issue_date DATE,
            up_to_date_as_of DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            # Create titles table indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_titles_number ON titles(title_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_titles_name ON titles(title_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_titles_reserved ON titles(is_reserved)')
            self.db_conn.commit()
            logger.info("Local PostgreSQL database setup completed")
            
        except psycopg2.Error as e:
            logger.error(f"Database setup error: {e}")
            raise
    
    def fetch_agencies(self) -> List[Dict]:
        """Fetch agencies from eCFR Admin API"""
        try:
            logger.info(f"Fetching agencies from {self.api_base}/agencies")
            response = self.session.get(f'{self.api_base}/agencies', timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"API response type: {type(data)}")
            
            # Handle different response formats
            agencies = []
            if isinstance(data, dict):
                logger.info(f"Response keys: {list(data.keys())}")
                if 'agencies' in data and isinstance(data['agencies'], list):
                    agencies = data['agencies']
                elif 'data' in data and isinstance(data['data'], list):
                    agencies = data['data']
                elif 'results' in data and isinstance(data['results'], list):
                    agencies = data['results']
                else:
                    # Single agency object
                    agencies = [data]
            elif isinstance(data, list):
                agencies = data
            else:
                logger.warning(f"Unexpected response format: {type(data)}")
                agencies = []
            
            logger.info(f"Found {len(agencies)} agencies")
            return agencies
            
        except requests.RequestException as e:
            logger.error(f"Error fetching agencies: {e}")
            raise
    
    def extract_cfr_references(self, agency_data: Dict) -> List[Dict]:
        """Extract CFR references from agency data"""
        cfr_refs = []
        
        # Common locations for CFR references
        ref_keys = ['cfr_references', 'cfr_refs', 'references', 'citations']
        
        for key in ref_keys:
            if key in agency_data:
                refs = agency_data[key]
                if isinstance(refs, list):
                    cfr_refs.extend(refs)
                elif isinstance(refs, dict):
                    cfr_refs.append(refs)
        
        # Also check in children recursively
        if 'children' in agency_data and isinstance(agency_data['children'], list):
            for child in agency_data['children']:
                child_refs = self.extract_cfr_references(child)
                cfr_refs.extend(child_refs)
        
        return cfr_refs
    
    def save_agency(self, agency_data: Dict) -> int:
        """Save agency to PostgreSQL database"""
        try:
            cursor = self.db_conn.cursor()
            
            # Extract required fields
            name = agency_data.get('name', '')
            display_name = agency_data.get('display_name', agency_data.get('displayName', name))
            slug = agency_data.get('slug', agency_data.get('short_name', ''))
            
            # Prepare children as JSONB
            children = agency_data.get('children', [])
            if isinstance(children, list):
                children_json = json.dumps(children) if children else None
            else:
                children_json = json.dumps([children]) if children else None
            
            # Extract CFR references
            cfr_refs = self.extract_cfr_references(agency_data)
            cfr_refs_json = json.dumps(cfr_refs) if cfr_refs else None
            
            # Store raw data
            raw_data_json = json.dumps(agency_data)
            
            # Check if agency already exists
            cursor.execute('SELECT id FROM agencies WHERE slug = %s', (slug,))
            existing = cursor.fetchone()
            logger.info(f" Agency slug: {slug}")
            if existing:
                # Update existing agency
                cursor.execute('''
                    UPDATE agencies 
                    SET name = %s, display_name = %s, children = %s,
                        cfr_references = %s, raw_data = %s, updated_at = %s
                    WHERE slug = %s
                ''', (name, display_name, children_json, cfr_refs_json, 
                      raw_data_json, datetime.now(), slug))
                agency_id = existing['id']
                logger.info(f"Updated agency: {name} (ID: {agency_id})")
                return agency_id, 'updated'
            else:
                # Insert new agency
                cursor.execute('''
                    INSERT INTO agencies 
                    (name, display_name, slug, children, cfr_references, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                ''', (name, display_name, slug, children_json, 
                      cfr_refs_json, raw_data_json))
                
                result = cursor.fetchone()
                agency_id = result['id']
                logger.info(f"Inserted agency: {name} (ID: {agency_id})")
                return agency_id, 'inserted'
            
        except Exception as e:
            logger.error(f"Error saving agency: {e}")
            logger.error(f"Agency data: {agency_data}")
            raise
    
    def process_agencies(self, agencies: List[Dict]):
        """Process and save all agencies"""
        logger.info(f"Processing {len(agencies)} agencies...")
        
        saved_count = 0
        for agency in agencies:
            try:
                self.save_agency(agency)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save agency: {e}")
                continue
        
        logger.info(f"Successfully saved {saved_count} agencies")
    
    def get_agency_stats(self) -> Dict:
        """Get statistics about ingested agencies"""
        cursor = self.db_conn.cursor()
        cursor.execute('''
            SELECT 
                COUNT(*) as total_agencies,
                COUNT(DISTINCT slug) as unique_slugs,
                COUNT(children) as agencies_with_children,
                COUNT(cfr_references) as agencies_with_cfr_refs
            FROM agencies
        ''')
        
        stats = cursor.fetchone()
        return dict(stats)
    
    def query_agencies(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        """Query agencies from database"""
        cursor = self.db_conn.cursor()
        cursor.execute('''
            SELECT name, display_name, slug, 
                   children as children,
                   cfr_references as cfr_references,
                   created_at, updated_at
            FROM agencies
            ORDER BY name
            LIMIT %s OFFSET %s
        ''', (limit, offset))
        
        return [dict(row) for row in cursor.fetchall()]

    def fetch_titles(self) -> List[Dict]:
        """Fetch titles from eCFR versioner API"""
        try:
            logger.info(f"Fetching titles from {self.titles_api_url}")
            response = self.session.get(self.titles_api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"API response type: {type(data)}")
            # Handle different response formats
            titles = []
            if isinstance(data, dict):
                logger.info(f"Response keys: {list(data.keys())}")
                if 'titles' in data and isinstance(data['titles'], list):
                    titles = data['titles']
                elif 'data' in data and isinstance(data['data'], list):
                    titles = data['data']
                elif 'results' in data and isinstance(data['results'], list):
                    titles = data['results']
                else:
                    # Single title object or list of titles
                    if isinstance(data, list):
                        titles = data
                    else:
                        titles = [data]
            elif isinstance(data, list):
                titles = data
            else:
                logger.warning(f"Unexpected response format: {type(data)}")
                titles = []
            logger.info(f"Found {len(titles)} titles")
            return titles
        except requests.RequestException as e:
            logger.error(f"Error fetching titles: {e}")
            raise
    
    def get_chapter_count(self, title_number):
        """Get chapter count from the ecfr_chapter_wordcount table"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM ecfr_chapter_wordcount WHERE title = %s",
                (title_number,)
            )
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting chapter count: {e}")
            raise
    def get_chapter_count(self, title_number):
        """Get chapter count from the ecfr_chapter_wordcount table"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM ecfr_chapter_wordcount WHERE title = %s",
                (title_number,)
            )
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting chapter count: {e}")
            raise
    
    def save_title(self, title_data: Dict) -> int:
        """Save title to PostgreSQL database"""
        try:
            cursor = self.db_conn.cursor()
            
            # Extract title information
            title_number = title_data.get('number')
            title_name = title_data.get('name', '')
            
            # Check if title is reserved
            is_reserved = title_data.get('reserved', False)
            
            # Extract date fields
            latest_amended_on = title_data.get('latest_amended_on')
            latest_issue_date = title_data.get('latest_issue_date')
            up_to_date_as_of = title_data.get('up_to_date_as_of')
            
            # Get chapter count from existing data if available
            chapter_count = self.get_chapter_count(title_number)
            
            # Check if title already exists
            cursor.execute('SELECT id FROM titles WHERE title_number = %s', (title_number,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing title
                cursor.execute('''
                    UPDATE titles 
                    SET title_name = %s, chapter_count = %s,
                        is_reserved = %s, latest_amended_on = %s,
                        latest_issue_date = %s, up_to_date_as_of = %s,
                        updated_at = %s
                    WHERE title_number = %s
                ''', (title_name, chapter_count, is_reserved, latest_amended_on,
                      latest_issue_date, up_to_date_as_of, datetime.now(), title_number))
                title_id = existing['id']
                logger.info(f"Updated title: {title_name} (ID: {title_id})")
                return title_id, 'updated'
            else:
                # Insert new title
                cursor.execute('''
                    INSERT INTO titles (title_number, title_name, chapter_count, is_reserved,
                                       latest_amended_on, latest_issue_date, up_to_date_as_of)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                ''', (title_number, title_name, chapter_count, is_reserved,
                      latest_amended_on, latest_issue_date, up_to_date_as_of))
                
                result = cursor.fetchone()
                title_id = result['id']
                logger.info(f"Inserted title: {title_name} (ID: {title_id})")
                return title_id, 'inserted'
            
        except Exception as e:
            logger.error(f"Error saving title: {e}")
            logger.error(f"Title data: {title_data}")
            raise
    
    def process_titles(self, titles: List[Dict]):
        """Process and save all titles"""
        logger.info(f"Processing {len(titles)} titles...")
        
        saved_count = 0
        for title in titles:
            try:
                self.save_title(title)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save title: {e}")
                continue
        
        logger.info(f"Successfully saved {saved_count} titles")
    
    def get_titles_stats(self) -> Dict:
        """Get statistics about ingested titles"""
        cursor = self.db_conn.cursor()
        cursor.execute('''
            SELECT 
                COUNT(*) as total_titles,
                COUNT(CASE WHEN is_reserved THEN 1 END) as reserved_titles,
                SUM(chapter_count) as total_chapters,
                AVG(chapter_count) as avg_chapters
            FROM titles
        ''')
        
        stats = cursor.fetchone()
        return dict(stats)
    
    def run_ingestion(self):
        """Run the complete ingestion process for both agencies and titles"""
        logger.info("Starting agencies and titles data ingestion into local PostgreSQL...")
        
        try:
            # Process agencies
            logger.info("\n=== Processing Agencies ===")
            agencies = self.fetch_agencies()
            
            if not agencies:
                logger.warning("No agencies found in API response")
            else:
                self.process_agencies(agencies)
                
                # Show agencies statistics
                stats = self.get_agency_stats()
                logger.info(f"Agencies ingestion completed. Stats: {stats}")
                
                # Show sample agencies
                sample = self.query_agencies(limit=3)
                logger.info("\nSample agencies:")
                for agency in sample:
                    logger.info(f"- {agency['name']} ({agency['slug']})")
            
            # Process titles
            logger.info("\n=== Processing Titles ===")
            titles = self.fetch_titles()
            
            if not titles:
                logger.warning("No titles found in API response")
            else:
                self.process_titles(titles)
                
                # Show titles statistics
                stats = self.get_titles_stats()
                logger.info(f"Titles ingestion completed. Stats: {stats}")
                
                # Show sample titles
                cursor = self.db_conn.cursor()
                cursor.execute('''
                    SELECT title_number, title_name, chapter_count, is_reserved
                    FROM titles
                    ORDER BY title_number
                    LIMIT 3
                ''')
                sample_titles = cursor.fetchall()
                logger.info("\nSample titles:")
                for title in sample_titles:
                    logger.info(f"- Title {title['title_number']}: {title['title_name']} ({title['chapter_count']} chapters)")
            logger.info("\n🎉 Complete ingestion finished successfully!")
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
        finally:
            if self.db_conn:
                self.db_conn.close()
                logger.info("Database connection closed")

def main():
    """Main entry point"""
    print("eCFR Agencies Data Ingestion for Local PostgreSQL")
    print("=" * 50)
    
    try:
        ingestion = LocalPostgresAgenciesIngestion()
        ingestion.run_ingestion()
        print("\n✅ Agencies ingestion completed successfully!")
        print(f"Database: {ingestion.database_name} at {ingestion.db_host}:{ingestion.db_port}")
    except KeyboardInterrupt:
        print("\n⚠️ Ingestion interrupted by user")
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())