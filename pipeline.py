#!/usr/bin/env python3
"""
Soccer Data Pipeline - Phase 0

This script implements a lean data pipeline for soccer fixture and box-score data:
1. Bootstraps database schema if needed
2. Fetches upcoming fixtures from SofaScore
3. Scrapes box-score stats from FBref for completed fixtures
4. Upserts all data into PostgreSQL tables with conflict handling

Usage:
    python pipeline.py [--bootstrap-only]

Environment variables (stored in .env):
    DB_HOST: PostgreSQL host
    DB_PORT: PostgreSQL port
    DB_NAME: Database name
    DB_USER: Database username
    DB_PASSWORD: Database password
    SOFASCORE_BASE_URL: Base URL for SofaScore API/scraping
    FBREF_BASE_URL: Base URL for FBref statistics
"""

import argparse
import datetime
import json
import logging
import os
import random
import re
import sys
import time
from typing import Dict, List, Optional, Tuple, Union

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('soccer_pipeline')

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'soccer_pipeline'),
    'user': os.getenv('DB_USER', 'pipeline_user'),
    'password': os.getenv('DB_PASSWORD', '')
}

# API/Scraping parameters
SOFASCORE_BASE_URL = os.getenv('SOFASCORE_BASE_URL', 'https://api.sofascore.com/api/v1')
FBREF_BASE_URL = os.getenv('FBREF_BASE_URL', 'https://fbref.com')

# User agent pool to rotate through
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        raise


def make_request(url: str, retries: int = 3, delay: int = 1) -> Optional[requests.Response]:
    """
    Make an HTTP request with retry logic and exponential backoff.
    
    Args:
        url: The URL to request
        retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        
    Returns:
        Response object or None if all retries failed
    """
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except (requests.RequestException, requests.HTTPError) as e:
            wait_time = delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Request failed (attempt {attempt+1}/{retries}): {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None


def bootstrap_schema(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize the database schema if tables don't exist.
    
    Args:
        conn: Database connection
    """
    with conn.cursor() as cur:
        # Create fixtures table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fixtures (
            id SERIAL PRIMARY KEY,
            external_id VARCHAR(100) UNIQUE,
            home_team VARCHAR(100) NOT NULL,
            away_team VARCHAR(100) NOT NULL,
            competition VARCHAR(100) NOT NULL,
            match_date TIMESTAMP NOT NULL,
            status VARCHAR(50) DEFAULT 'scheduled',
            home_score INTEGER,
            away_score INTEGER,
            match_url VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create box_scores table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS box_scores (
            id SERIAL PRIMARY KEY,
            fixture_id INTEGER REFERENCES fixtures(id),
            team VARCHAR(100) NOT NULL,
            is_home BOOLEAN NOT NULL,
            goals INTEGER,
            shots INTEGER,
            shots_on_target INTEGER,
            possession FLOAT,
            passes INTEGER,
            pass_accuracy FLOAT,
            fouls INTEGER,
            yellow_cards INTEGER,
            red_cards INTEGER,
            offsides INTEGER,
            corners INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fixture_id, team)
        )
        """)

        # Create index for faster lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_status ON fixtures(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_match_date ON fixtures(match_date)")
        
        conn.commit()
        logger.info("Database schema initialized successfully")


def fetch_fixtures(days_ahead: int = 7) -> List[Dict]:
    """
    Fetch upcoming fixtures from SofaScore.
    
    Args:
        days_ahead: Number of days to look ahead for fixtures
        
    Returns:
        List of fixture dictionaries
    """
    fixtures = []
    today = datetime.datetime.now()
    
    # Loop through the next 'days_ahead' days
    for day_offset in range(days_ahead):
        target_date = today + datetime.timedelta(days=day_offset)
        date_str = target_date.strftime("%Y-%m-%d")
        
        # SofaScore API endpoint for daily fixtures (adjust as needed based on API format)
        url = f"{SOFASCORE_BASE_URL}/sport/football/scheduled-events/{date_str}"
        
        logger.info(f"Fetching fixtures for {date_str}...")
        response = make_request(url)
        
        if not response:
            logger.warning(f"Failed to fetch fixtures for {date_str}, skipping...")
            continue
            
        try:
            data = response.json()
            
            # Example parsing logic (adjust based on actual API response)
            for event in data.get('events', []):
                fixture = {
                    'external_id': f"sofascore_{event.get('id')}",
                    'home_team': event.get('homeTeam', {}).get('name'),
                    'away_team': event.get('awayTeam', {}).get('name'),
                    'competition': event.get('tournament', {}).get('name'),
                    'match_date': event.get('startTimestamp'),
                    'status': 'scheduled',
                    'match_url': f"{FBREF_BASE_URL}/search/matches/?query={event.get('homeTeam', {}).get('name')}+vs+{event.get('awayTeam', {}).get('name')}"
                }
                
                # Add scores if available
                home_score = event.get('homeScore', {}).get('current')
                away_score = event.get('awayScore', {}).get('current')
                
                if home_score is not None and away_score is not None:
                    fixture['home_score'] = home_score
                    fixture['away_score'] = away_score
                    fixture['status'] = 'completed' if event.get('status', {}).get('type') == 'finished' else 'scheduled'
                
                fixtures.append(fixture)
            
            # Sleep to be polite with the API
            time.sleep(1)
            
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing fixture data for {date_str}: {e}")
    
    logger.info(f"Fetched {len(fixtures)} fixtures")
    return fixtures


def upsert_fixtures(conn: psycopg2.extensions.connection, fixtures: List[Dict]) -> None:
    """
    Upsert fixtures into the database.
    
    Args:
        conn: Database connection
        fixtures: List of fixture dictionaries
    """
    if not fixtures:
        logger.info("No fixtures to upsert")
        return
        
    with conn.cursor() as cur:
        for fixture in fixtures:
            # Convert Unix timestamp to datetime if needed
            if isinstance(fixture.get('match_date'), int):
                fixture['match_date'] = datetime.datetime.fromtimestamp(fixture['match_date'])
                
            # Upsert query using ON CONFLICT
            cur.execute("""
            INSERT INTO fixtures 
                (external_id, home_team, away_team, competition, match_date, status, 
                 home_score, away_score, match_url)
            VALUES 
                (%(external_id)s, %(home_team)s, %(away_team)s, %(competition)s, %(match_date)s, 
                 %(status)s, %(home_score)s, %(away_score)s, %(match_url)s)
            ON CONFLICT (external_id) DO UPDATE SET
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                competition = EXCLUDED.competition,
                match_date = EXCLUDED.match_date,
                status = EXCLUDED.status,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                match_url = EXCLUDED.match_url,
                updated_at = CURRENT_TIMESTAMP
            """, fixture)
            
        conn.commit()
        logger.info(f"Upserted {len(fixtures)} fixtures into the database")


def get_completed_fixtures(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Retrieve completed fixtures that need box scores.
    
    Args:
        conn: Database connection
        
    Returns:
        List of completed fixture dictionaries
    """
    completed_fixtures = []
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Find completed fixtures that don't have box scores yet
        cur.execute("""
        SELECT f.* 
        FROM fixtures f
        LEFT JOIN (
            SELECT DISTINCT fixture_id 
            FROM box_scores
        ) bs ON f.id = bs.fixture_id
        WHERE f.status = 'completed'
        AND bs.fixture_id IS NULL
        """)
        
        for row in cur.fetchall():
            completed_fixtures.append(dict(row))
    
    logger.info(f"Found {len(completed_fixtures)} completed fixtures without box scores")
    return completed_fixtures


def scrape_box_scores(fixtures: List[Dict]) -> Dict[int, List[Dict]]:
    """
    Scrape box scores from FBref for completed fixtures.
    
    Args:
        fixtures: List of completed fixture dictionaries
        
    Returns:
        Dictionary mapping fixture IDs to lists of box score dictionaries
    """
    box_scores_by_fixture = {}
    
    for fixture in fixtures:
        logger.info(f"Scraping box scores for {fixture['home_team']} vs {fixture['away_team']}")
        
        # Use the match URL if available, otherwise construct a search URL
        match_url = fixture.get('match_url')
        if not match_url:
            query = f"{fixture['home_team']}+vs+{fixture['away_team']}"
            match_url = f"{FBREF_BASE_URL}/search/matches/?query={query}"
        
        # First get the match page
        search_response = make_request(match_url)
        if not search_response:
            logger.warning(f"Could not find match page for {fixture['home_team']} vs {fixture['away_team']}")
            continue
            
        # Parse the search results to find the match page
        soup = BeautifulSoup(search_response.text, 'html.parser')
        
        # Look for match links (adjust selector based on actual FBref HTML)
        match_links = soup.select('div.search-item a')
        actual_match_link = None
        
        # Find a link that contains both team names
        for link in match_links:
            link_text = link.text.lower()
            if (fixture['home_team'].lower() in link_text and 
                fixture['away_team'].lower() in link_text):
                actual_match_link = link.get('href')
                break
        
        if not actual_match_link:
            logger.warning(f"Could not find specific match link for {fixture['home_team']} vs {fixture['away_team']}")
            continue
            
        # Get the full match stats page
        full_match_url = f"{FBREF_BASE_URL}{actual_match_link}"
        match_response = make_request(full_match_url)
        
        if not match_response:
            logger.warning(f"Failed to fetch match stats from {full_match_url}")
            continue
            
        # Parse match stats
        match_soup = BeautifulSoup(match_response.text, 'html.parser')
        
        # Example parsing logic for box scores (adjust based on actual FBref HTML)
        stats_table = match_soup.select_one('table.stats_table')
        if not stats_table:
            logger.warning(f"No stats table found for {fixture['home_team']} vs {fixture['away_team']}")
            continue
            
        # Process home team stats
        home_stats = {
            'team': fixture['home_team'],
            'is_home': True,
            'goals': fixture.get('home_score', 0),
            'shots': extract_stat(match_soup, 'home', 'shots'),
            'shots_on_target': extract_stat(match_soup, 'home', 'shots_on_target'),
            'possession': extract_stat(match_soup, 'home', 'possession'),
            'passes': extract_stat(match_soup, 'home', 'passes'),
            'pass_accuracy': extract_stat(match_soup, 'home', 'pass_accuracy'),
            'fouls': extract_stat(match_soup, 'home', 'fouls'),
            'yellow_cards': extract_stat(match_soup, 'home', 'yellow_cards'),
            'red_cards': extract_stat(match_soup, 'home', 'red_cards'),
            'offsides': extract_stat(match_soup, 'home', 'offsides'),
            'corners': extract_stat(match_soup, 'home', 'corners')
        }
        
        # Process away team stats
        away_stats = {
            'team': fixture['away_team'],
            'is_home': False,
            'goals': fixture.get('away_score', 0),
            'shots': extract_stat(match_soup, 'away', 'shots'),
            'shots_on_target': extract_stat(match_soup, 'away', 'shots_on_target'),
            'possession': extract_stat(match_soup, 'away', 'possession'),
            'passes': extract_stat(match_soup, 'away', 'passes'),
            'pass_accuracy': extract_stat(match_soup, 'away', 'pass_accuracy'),
            'fouls': extract_stat(match_soup, 'away', 'fouls'),
            'yellow_cards': extract_stat(match_soup, 'away', 'yellow_cards'),
            'red_cards': extract_stat(match_soup, 'away', 'red_cards'),
            'offsides': extract_stat(match_soup, 'away', 'offsides'),
            'corners': extract_stat(match_soup, 'away', 'corners')
        }
        
        box_scores_by_fixture[fixture['id']] = [home_stats, away_stats]
        
        # Be polite and delay between requests
        time.sleep(1)
    
    return box_scores_by_fixture


def extract_stat(soup: BeautifulSoup, team_type: str, stat_name: str) -> Optional[Union[int, float]]:
    """
    Extract a specific statistic from the match stats HTML.
    
    Args:
        soup: BeautifulSoup object of the match page
        team_type: 'home' or 'away'
        stat_name: Name of the statistic to extract
        
    Returns:
        Extracted statistic value or None if not found
    """
    # This is a placeholder function - implementation details will vary based on FBref's actual HTML structure
    # In a real implementation, you would need to locate the specific elements containing each stat
    
    # Example implementation
    stat_mapping = {
        'shots': 'Total Shots',
        'shots_on_target': 'Shots on Target',
        'possession': 'Possession',
        'passes': 'Passes',
        'pass_accuracy': 'Pass Accuracy',
        'fouls': 'Fouls',
        'yellow_cards': 'Yellow Cards',
        'red_cards': 'Red Cards',
        'offsides': 'Offsides',
        'corners': 'Corner Kicks'
    }
    
    stat_label = stat_mapping.get(stat_name)
    if not stat_label:
        return None
        
    # Find the row containing this stat
    stat_rows = soup.select('div.scorebox_meta tr')
    for row in stat_rows:
        cells = row.select('td')
        if len(cells) >= 3 and stat_label in cells[1].text:
            try:
                value_text = cells[0].text if team_type == 'home' else cells[2].text
                
                # Handle different stat types
                if stat_name == 'possession':
                    # Extract percentage value (e.g., "60%" -> 60.0)
                    match = re.search(r'(\d+(?:\.\d+)?)', value_text)
                    if match:
                        return float(match.group(1))
                elif stat_name == 'pass_accuracy':
                    # Extract percentage value
                    match = re.search(r'(\d+(?:\.\d+)?)', value_text)
                    if match:
                        return float(match.group(1))
                else:
                    # Extract integer value
                    return int(re.sub(r'[^\d]', '', value_text))
            except (ValueError, IndexError):
                pass
                
    # Fallback to default values if not found
    default_values = {
        'possession': 50.0,
        'pass_accuracy': 0.0
    }
    
    return default_values.get(stat_name, 0)


def upsert_box_scores(conn: psycopg2.extensions.connection, box_scores_by_fixture: Dict[int, List[Dict]]) -> None:
    """
    Upsert box scores into the database.
    
    Args:
        conn: Database connection
        box_scores_by_fixture: Dictionary mapping fixture IDs to lists of box score dictionaries
    """
    if not box_scores_by_fixture:
        logger.info("No box scores to upsert")
        return
        
    total_upserted = 0
    
    with conn.cursor() as cur:
        for fixture_id, box_scores in box_scores_by_fixture.items():
            for box_score in box_scores:
                # Add fixture_id to the box score dictionary
                box_score['fixture_id'] = fixture_id
                
                # Upsert query using ON CONFLICT
                cur.execute("""
                INSERT INTO box_scores 
                    (fixture_id, team, is_home, goals, shots, shots_on_target, possession, 
                     passes, pass_accuracy, fouls, yellow_cards, red_cards, offsides, corners)
                VALUES 
                    (%(fixture_id)s, %(team)s, %(is_home)s, %(goals)s, %(shots)s, %(shots_on_target)s, 
                     %(possession)s, %(passes)s, %(pass_accuracy)s, %(fouls)s, %(yellow_cards)s, 
                     %(red_cards)s, %(offsides)s, %(corners)s)
                ON CONFLICT (fixture_id, team) DO UPDATE SET
                    goals = EXCLUDED.goals,
                    shots = EXCLUDED.shots,
                    shots_on_target = EXCLUDED.shots_on_target,
                    possession = EXCLUDED.possession,
                    passes = EXCLUDED.passes,
                    pass_accuracy = EXCLUDED.pass_accuracy,
                    fouls = EXCLUDED.fouls,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards = EXCLUDED.red_cards,
                    offsides = EXCLUDED.offsides,
                    corners = EXCLUDED.corners,
                    updated_at = CURRENT_TIMESTAMP
                """, box_score)
                
                total_upserted += 1
                
        conn.commit()
        logger.info(f"Upserted {total_upserted} box scores into the database")


def run_pipeline():
    """Execute the full data pipeline."""
    conn = None
    try:
        # Connect to the database
        conn = get_db_connection()
        
        # Bootstrap schema if necessary
        bootstrap_schema(conn)
        
        # Fetch and upsert fixtures
        fixtures = fetch_fixtures()
        upsert_fixtures(conn, fixtures)
        
        # Get completed fixtures without box scores
        completed_fixtures = get_completed_fixtures(conn)
        
        # Scrape and upsert box scores for completed fixtures
        if completed_fixtures:
            box_scores = scrape_box_scores(completed_fixtures)
            upsert_box_scores(conn, box_scores)
            
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Soccer Data Pipeline")
    parser.add_argument('--bootstrap-only', action='store_true', 
                       help='Only bootstrap the database schema')
    args = parser.parse_args()
    
    if args.bootstrap_only:
        conn = get_db_connection()
        bootstrap_schema(conn)
        conn.close()
        logger.info("Database schema bootstrapped successfully")
    else:
        run_pipeline()