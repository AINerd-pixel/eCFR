#!/usr/bin/env python3
"""
Simple FastAPI server for eCFR agencies
Provides a single endpoint to retrieve all agencies
"""

import os
import psycopg2
import psycopg2.extras
from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import logging
import openai

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY", "your key")

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecfrdb')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'XYZ')

# Pydantic models for API responses
class WordCount(BaseModel):
    id: int
    title: int
    chapter_identifier: str
    chapter_heading: Optional[str] = None
    word_count: int
    character_count: Optional[int] = None
    is_reserved: bool = False
    downloaded_at: datetime

class AgencySummaryRequest(BaseModel):
    agency_name: str
    agency_display_name: Optional[str] = None
    cfr_references: List[Dict]
    description: Optional[str] = None

class AgencySummaryResponse(BaseModel):
    summary: str
    key_responsibilities: List[str]
    regulatory_scope: str
    generated_at: datetime

class Agency(BaseModel):
    id: int
    name: str
    display_name: Optional[str] = None
    slug: str
    children: Optional[List[Dict]] = None
    cfr_references: Optional[List[Dict]] = None
    created_at: datetime
    updated_at: datetime

# Initialize FastAPI app
app = FastAPI(
    title="eCFR Agencies API",
    description="Simple API for retrieving all eCFR agencies",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Database connection
def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/agencies", response_model=List[Agency], tags=["Agencies"], summary="Get all agencies with title information")
async def get_agencies(slug: Optional[str] = None, name: Optional[str] = None):
    """
    Get all agencies with optional filtering by slug and name
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Build query with optional filters
        query = '''
            SELECT id, name, display_name, slug, children, cfr_references,
                   created_at, updated_at
            FROM agencies
        '''
        params = []
        
        if slug and name:
            query += ' WHERE slug = %s AND name ILIKE %s'
            params.extend([slug, f'%{name}%'])
        elif slug:
            query += ' WHERE slug = %s'
            params.append(slug)
        elif name:
            query += ' WHERE name ILIKE %s'
            params.append(f'%{name}%')
        
        query += ' ORDER BY name'
        
        cursor.execute(query, params)
        agencies = cursor.fetchall()
        
        # Process each agency and embed title information in CFR references
        result = []
        for agency in agencies:
            agency_data = dict(agency)
            
            # Embed title information in CFR references
            if agency_data['cfr_references']:
                agency_data['cfr_references'] = enrich_cfr_refs_with_title_info(conn, agency_data['cfr_references'])
            
            result.append(Agency(**agency_data))
        
        return result
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

def enrich_cfr_refs_with_title_info(conn, cfr_references: List[Dict]) -> List[Dict]:
    """
    Enrich CFR references with title information
    """
    if not cfr_references:
        return []
    
    # Extract unique title numbers from CFR references
    titles = set()
    for ref in cfr_references:
        if isinstance(ref, dict) and 'title' in ref:
            titles.add(ref['title'])
        elif isinstance(ref, dict) and 'citation' in ref:
            # Parse title from citation like "Title 1 Chapter I"
            citation = ref['citation']
            if 'Title' in citation:
                parts = citation.split()
                for i, part in enumerate(parts):
                    if part == 'Title' and i + 1 < len(parts):
                        try:
                            title_num = int(parts[i + 1])
                            titles.add(title_num)
                            break
                        except ValueError:
                            pass
    
    # Get title information for these titles
    cursor = conn.cursor()
    title_list = list(titles)
    
    if not title_list:
        return cfr_references
    
    # Build query with IN clause
    placeholders = ','.join(['%s'] * len(title_list))
    cursor.execute(f'''
        SELECT title_number, title_name, latest_amended_on, 
               latest_issue_date, up_to_date_as_of
        FROM titles
        WHERE title_number IN ({placeholders})
        ORDER BY title_number
    ''', title_list)
    
    title_info = cursor.fetchall()
    title_dict = {info['title_number']: dict(info) for info in title_info}
    
    # Enrich each CFR reference with title information
    enriched_refs = []
    for ref in cfr_references:
        ref_copy = dict(ref)
        
        # Add title information if available
        if isinstance(ref, dict) and 'title' in ref:
            title_num = ref['title']
            if title_num in title_dict:
                ref_copy.update({
                    'title_name': title_dict[title_num]['title_name'],
                    'latest_amended_on': title_dict[title_num]['latest_amended_on'],
                    'latest_issue_date': title_dict[title_num]['latest_issue_date'],
                    'up_to_date_as_of': title_dict[title_num]['up_to_date_as_of']
                })
        elif isinstance(ref, dict) and 'citation' in ref:
            # Parse title from citation and add info
            citation = ref['citation']
            if 'Title' in citation:
                parts = citation.split()
                for i, part in enumerate(parts):
                    if part == 'Title' and i + 1 < len(parts):
                        try:
                            title_num = int(parts[i + 1])
                            if title_num in title_dict:
                                ref_copy.update({
                                    'title_name': title_dict[title_num]['title_name'],
                                    'latest_amended_on': title_dict[title_num]['latest_amended_on'],
                                    'latest_issue_date': title_dict[title_num]['latest_issue_date'],
                                    'up_to_date_as_of': title_dict[title_num]['up_to_date_as_of']
                                })
                            break
                        except ValueError:
                            pass
        
        enriched_refs.append(ref_copy)
    
    return enriched_refs

@app.get("/word-count/{title}/{chapter_identifier}", response_model=WordCount, tags=["Word Counts"], summary="Get word count by title and chapter")
async def get_word_count_by_title_and_chapter(title: int, chapter_identifier: str):
    """
    Get word count for a specific title and chapter identifier
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, title, chapter_identifier, chapter_heading, word_count,
                   character_count, is_reserved, downloaded_at
            FROM ecfr_chapter_wordcount
            WHERE title = %s AND chapter_identifier = %s
        ''', (title, chapter_identifier))
        
        word_count = cursor.fetchone()
        
        if not word_count:
            raise HTTPException(
                status_code=404, 
                detail=f"Word count not found for title {title} and chapter {chapter_identifier}"
            )
        
        return WordCount(**dict(word_count))
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

@app.get("/word-counts/{title}", response_model=List[WordCount], tags=["Word Counts"], summary="Get all word counts by title")
async def get_all_word_counts_by_title(title: int):
    """
    Get all word counts for a specific title
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, title, chapter_identifier, chapter_heading, word_count,
                   character_count, is_reserved, downloaded_at
            FROM ecfr_chapter_wordcount
            WHERE title = %s
            ORDER BY chapter_identifier
        ''', (title,))
        
        word_counts = cursor.fetchall()
        return [WordCount(**dict(wc)) for wc in word_counts]
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

@app.post("/ai/agency-summary", response_model=AgencySummaryResponse, tags=["AI"], summary="Generate AI-powered agency summary")
async def generate_agency_summary(request: AgencySummaryRequest):
    """
    Generate an AI-powered summary for an agency based on its name, CFR references, and description
    """
    try:
        # Extract CFR titles and chapters for context
        cfr_context = []
        if request.cfr_references:
            for ref in request.cfr_references[:10]:  # Limit to first 10 references to avoid token limits
                title_info = f"Title {ref.get('title', 'N/A')}: {ref.get('name', 'Unknown Title')}"
                if ref.get('chapter'):
                    title_info += f", Chapter {ref['chapter']}"
                cfr_context.append(title_info)
        
        # Create prompt for OpenAI
        prompt = f"""
        Generate a comprehensive summary for the U.S. government agency: {request.agency_name}
        
        Display Name: {request.agency_display_name or 'N/A'}
        
        CFR References:
        {chr(10).join(cfr_context) if cfr_context else 'No CFR references available'}
        
        Description: {request.description or 'No description available'}
        
        Please provide:
        1. A concise 2-3 sentence summary of the agency's role and mission
        2. A list of 3-5 key responsibilities
        3. A brief description of the agency's regulatory scope
        
        Format the response as JSON with keys: summary, key_responsibilities (array), regulatory_scope
        """
        
        # Call OpenAI API
        from openai import OpenAI
        client = OpenAI(api_key=openai.api_key)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert on U.S. government agencies and federal regulations. Provide accurate, concise information about regulatory agencies."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        # Parse the response
        content = response.choices[0].message.content
        
        # Try to parse as JSON, fallback to text if needed
        try:
            import json
            ai_response = json.loads(content)
            summary = ai_response.get("summary", content)
            key_responsibilities = ai_response.get("key_responsibilities", [])
            regulatory_scope = ai_response.get("regulatory_scope", "Regulatory scope not specified")
        except json.JSONDecodeError:
            # Fallback: use the raw content as summary
            summary = content
            key_responsibilities = ["Key responsibilities not specified"]
            regulatory_scope = "Regulatory scope not specified"
        
        return AgencySummaryResponse(
            summary=summary,
            key_responsibilities=key_responsibilities,
            regulatory_scope=regulatory_scope,
            generated_at=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error generating AI summary: {e}")
        # Return a fallback response instead of failing
        return AgencySummaryResponse(
            summary=f"{request.agency_name} is a U.S. government agency responsible for regulatory oversight and compliance enforcement.",
            key_responsibilities=[
                "Regulatory compliance",
                "Policy enforcement",
                "Industry oversight"
            ],
            regulatory_scope="Federal regulatory jurisdiction as defined by CFR references",
            generated_at=datetime.now()
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)