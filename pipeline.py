#!/usr/bin/env python3
"""
Soccer Data Pipeline - Phase 0

This script implements a lean data pipeline for soccer fixture and box-score data:
1. Bootstraps database schema (fixtures, teams, match_stats tables)
2. Fetches upcoming fixtures from SofaScore API with team deduplication
3. Scrapes box-score stats from FBref for completed fixtures
4. Implements idempotent database writes with proper conflict handling

Usage:
    python pipeline.py [--bootstrap-only] [--days-ahead DAYS]
    python pipeline.py --fetch-fixtures-only
    python pipeline.py --scrape-stats-only

Environment variables (stored in .env):
    DATABASE_URL: PostgreSQL connection string (e.g. postgresql://user:pass@localhost/dbname)
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
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline')

# API/Scraping parameters
SOFASCORE_BASE_URL = os.getenv('SOFASCORE_BASE_URL', 'https://api.sofascore.com/api/v1')
FBREF_BASE_URL = os.getenv('FBREF_BASE_URL', 'https://fbref.com')

# User agent pool to rotate through for web requests
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

# In-memory cache for team lookups
TEAM_CACHE = {}


def get_db_connection():
    """Establish and return a connection to the PostgreSQL database using DATABASE_URL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
    Designed to align with future SQLAlchemy models in db/models.py.
    
    Args:
        conn: Database connection
    """
    with conn.cursor() as cur:
        # Create teams table - aligned with Team model in db/models.py
        cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE,
            fbref_slug VARCHAR(100) UNIQUE,
            sofascore_id INTEGER UNIQUE
        )
        """)
        
        # Create fixtures table - simplified for Phase 0 but aligned with direction
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fixtures (
            match_id VARCHAR(150) PRIMARY KEY,
            date DATE NOT NULL,
            home_team_id INTEGER REFERENCES teams(team_id) NOT NULL,
            away_team_id INTEGER REFERENCES teams(team_id) NOT NULL,
            competition VARCHAR(100) NOT NULL,
            status VARCHAR(50) DEFAULT 'scheduled',
            home_score SMALLINT,
            away_score SMALLINT,
            match_url VARCHAR(255),
            scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create match_stats table - aligned with MatchStat model in db_models.py
        cur.execute("""
        CREATE TABLE IF NOT EXISTS match_stats (
            match_id VARCHAR(150) REFERENCES fixtures(match_id),
            team_id INTEGER REFERENCES teams(team_id),
            gf SMALLINT,
            ga SMALLINT,
            xg NUMERIC,
            xga NUMERIC,
            sh SMALLINT,
            sot SMALLINT,
            possession NUMERIC,
            passes SMALLINT,
            pass_accuracy NUMERIC,
            fouls SMALLINT,
            yellow_cards SMALLINT,
            red_cards SMALLINT,
            offsides SMALLINT,
            corners SMALLINT,
            scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(match_id, team_id)
        )
        """)

        # Create indexes for faster lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_status ON fixtures(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stats_scrape_date ON match_stats(scrape_date)")
        
        conn.commit()
        logger.info("Database schema initialized successfully")


def get_or_create_team(conn: psycopg2.extensions.connection, team_name: str, sofascore_id: Optional[int] = None) -> int:
    """
    Get an existing team by name or create it if it doesn't exist.
    
    Args:
        conn: Database connection
        team_name: Name of the team
        sofascore_id: Optional SofaScore ID for the team
        
    Returns:
        team_id from the database
    """
    # Check cache first
    if team_name in TEAM_CACHE:
        return TEAM_CACHE[team_name]
        
    with conn.cursor() as cur:
        # Try to find the team by name
        cur.execute("SELECT team_id FROM teams WHERE name = %s", (team_name,))
        result = cur.fetchone()
        
        if result:
            team_id = result[0]
        else:
            # Create a new team
            cur.execute(
                "INSERT INTO teams (name, sofascore_id) VALUES (%s, %s) RETURNING team_id",
                (team_name, sofascore_id)
            )
            team_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Created new team: {team_name} (ID: {team_id})")
        
        # Update cache
        TEAM_CACHE[team_name] = team_id
        return team_id


def fetch_fixtures(conn: psycopg2.extensions.connection, days_ahead: int = 7) -> int:
    """
    Fetch upcoming fixtures from SofaScore and store in the database.
    
    Args:
        conn: Database connection
        days_ahead: Number of days to look ahead for fixtures
        
    Returns:
        Number of fixtures upserted
    """
    today = datetime.datetime.now()
    fixtures_added = 0
    
    # Loop through the next 'days_ahead' days
    for day_offset in range(days_ahead):
        target_date = today + datetime.timedelta(days=day_offset)
        date_str = target_date.strftime("%Y-%m-%d")
        
        # SofaScore API endpoint for daily fixtures
        url = f"{SOFASCORE_BASE_URL}/sport/football/scheduled-events/{date_str}"
        
        logger.info(f"Fetching fixtures for {date_str}...")
        response = make_request(url)
        
        if not response:
            logger.warning(f"Failed to fetch fixtures for {date_str}, skipping...")
            continue
            
        try:
            data = response.json()
            
            # Parse each event from the response
            for event in data.get('events', []):
                # Extract team data
                home_team_name = event.get('homeTeam', {}).get('name')
                away_team_name = event.get('awayTeam', {}).get('name')
                home_team_sofascore_id = event.get('homeTeam', {}).get('id')
                away_team_sofascore_id = event.get('awayTeam', {}).get('id')
                
                if not (home_team_name and away_team_name):
                    logger.warning(f"Skipping event {event.get('id')}: Missing team names")
                    continue
                
                # Get or create teams in database
                home_team_id = get_or_create_team(conn, home_team_name, home_team_sofascore_id)
                away_team_id = get_or_create_team(conn, away_team_name, away_team_sofascore_id)
                
                # Generate a unique match ID
                match_id = f"sofascore_{event.get('id')}"
                
                # Extract match date
                start_timestamp = event.get('startTimestamp')
                if not start_timestamp:
                    logger.warning(f"Skipping event {event.get('id')}: Missing start timestamp")
                    continue
                    
                match_date = datetime.datetime.fromtimestamp(start_timestamp).date()
                
                # Get competition name
                competition = event.get('tournament', {}).get('name', 'Unknown')
                
                # Determine match status and scores
                status = 'scheduled'
                home_score = None
                away_score = None
                
                if event.get('status', {}).get('type') == 'finished':
                    status = 'completed'
                    home_score = event.get('homeScore', {}).get('current')
                    away_score = event.get('awayScore', {}).get('current')
                
                # Construct FBref search URL for this match
                fbref_search_url = f"{FBREF_BASE_URL}/search/matches/?query={home_team_name}+vs+{away_team_name}"
                
                # Upsert fixture into database
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO fixtures 
                        (match_id, date, home_team_id, away_team_id, competition, 
                         status, home_score, away_score, match_url, scrape_date)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (match_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        scrape_date = CURRENT_TIMESTAMP
                    """, (
                        match_id, 
                        match_date, 
                        home_team_id, 
                        away_team_id,
                        competition,
                        status,
                        home_score,
                        away_score,
                        fbref_search_url
                    ))
                    fixtures_added += 1
            
            # Be polite to the API
            time.sleep(1)
            
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing fixture data for {date_str}: {e}")
    
    conn.commit()
    logger.info(f"Upserted {fixtures_added} fixtures into the database")
    return fixtures_added


def get_completed_fixtures(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Retrieve completed fixtures that don't have match stats yet.
    
    Args:
        conn: Database connection
        
    Returns:
        List of completed fixture dictionaries
    """
    completed_fixtures = []
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Find completed fixtures that don't have match stats yet
        cur.execute("""
        SELECT f.match_id, f.date, f.match_url, 
               ht.name as home_team, at.name as away_team,
               f.home_team_id, f.away_team_id,
               f.home_score, f.away_score
        FROM fixtures f
        JOIN teams ht ON f.home_team_id = ht.team_id
        JOIN teams at ON f.away_team_id = at.team_id
        LEFT JOIN (
            SELECT DISTINCT match_id 
            FROM match_stats
        ) ms ON f.match_id = ms.match_id
        WHERE f.status = 'completed'
        AND ms.match_id IS NULL
        ORDER BY f.date DESC
        LIMIT 50  -- Process in batches
        """)
        
        for row in cur.fetchall():
            completed_fixtures.append(dict(row))
    
    logger.info(f"Found {len(completed_fixtures)} completed fixtures without match stats")
    return completed_fixtures


def extract_fbref_stat(soup: BeautifulSoup, team_type: str, stat_name: str) -> Optional[Union[int, float]]:
    """
    Extract a specific statistic from the FBref match HTML.
    
    Args:
        soup: BeautifulSoup object of the match page
        team_type: 'home' or 'away'
        stat_name: Name of the statistic to extract
        
    Returns:
        Extracted statistic value or None if not found
    """
    # This is a placeholder implementation - adjust based on FBref's actual HTML structure
    
    # Map stat names to their labels in FBref HTML
    stat_mapping = {
        'sh': 'Total Shots',
        'sot': 'Shots on Target',
        'possession': 'Possession',
        'passes': 'Passes',
        'pass_accuracy': 'Pass Accuracy',
        'fouls': 'Fouls',
        'yellow_cards': 'Yellow Cards',
        'red_cards': 'Red Cards',
        'offsides': 'Offsides',
        'corners': 'Corner Kicks',
        'xg': 'Expected Goals (xG)',
        'xga': 'Expected Goals Against (xGA)'
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
                
                # Handle percentage values
                if stat_name in ['possession', 'pass_accuracy']:
                    # Extract percentage (e.g., "60%" -> 60.0)
                    match = re.search(r'(\d+(?:\.\d+)?)', value_text)
                    if match:
                        return float(match.group(1))
                # Handle numeric values
                elif stat_name in ['xg', 'xga']:
                    # Extract float value
                    match = re.search(r'(\d+\.\d+)', value_text)
                    if match:
                        return float(match.group(1))
                else:
                    # Extract integer value
                    return int(re.sub(r'[^\d]', '', value_text))
            except (ValueError, IndexError):
                pass
                
    # Return None if not found
    return None


def scrape_match_stats(conn: psycopg2.extensions.connection, fixtures: List[Dict]) -> int:
    """
    Scrape match statistics from FBref for completed fixtures.
    
    Args:
        conn: Database connection
        fixtures: List of completed fixture dictionaries
        
    Returns:
        Number of match stats records created
    """
    stats_created = 0
    
    for fixture in fixtures:
        logger.info(f"Scraping stats for {fixture['home_team']} vs {fixture['away_team']} ({fixture['match_id']})")
        
        # Use the match URL to find the FBref page
        search_response = make_request(fixture['match_url'])
        if not search_response:
            logger.warning(f"Could not access search page for {fixture['match_id']}")
            continue
            
        # Parse the search results to find the match page
        soup = BeautifulSoup(search_response.text, 'html.parser')
        
        # Look for match links in search results
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
            logger.warning(f"Could not find match page for {fixture['match_id']}")
            continue
            
        # Get the full match stats page
        full_match_url = f"{FBREF_BASE_URL}{actual_match_link}"
        match_response = make_request(full_match_url)
        
        if not match_response:
            logger.warning(f"Failed to fetch match stats from {full_match_url}")
            continue
            
        # Parse match stats
        match_soup = BeautifulSoup(match_response.text, 'html.parser')
        
        # Extract home team stats
        home_stats = {
            'match_id': fixture['match_id'],
            'team_id': fixture['home_team_id'],
            'gf': fixture['home_score'],
            'ga': fixture['away_score'],
            'xg': extract_fbref_stat(match_soup, 'home', 'xg'),
            'xga': extract_fbref_stat(match_soup, 'home', 'xga'),
            'sh': extract_fbref_stat(match_soup, 'home', 'sh'),
            'sot': extract_fbref_stat(match_soup, 'home', 'sot'),
            'possession': extract_fbref_stat(match_soup, 'home', 'possession'),
            'passes': extract_fbref_stat(match_soup, 'home', 'passes'),
            'pass_accuracy': extract_fbref_stat(match_soup, 'home', 'pass_accuracy'),
            'fouls': extract_fbref_stat(match_soup, 'home', 'fouls'),
            'yellow_cards': extract_fbref_stat(match_soup, 'home', 'yellow_cards'),
            'red_cards': extract_fbref_stat(match_soup, 'home', 'red_cards'),
            'offsides': extract_fbref_stat(match_soup, 'home', 'offsides'),
            'corners': extract_fbref_stat(match_soup, 'home', 'corners')
        }
        
        # Extract away team stats
        away_stats = {
            'match_id': fixture['match_id'],
            'team_id': fixture['away_team_id'],
            'gf': fixture['away_score'],
            'ga': fixture['home_score'],
            'xg': extract_fbref_stat(match_soup, 'away', 'xg'),
            'xga': extract_fbref_stat(match_soup, 'away', 'xga'),
            'sh': extract_fbref_stat(match_soup, 'away', 'sh'),
            'sot': extract_fbref_stat(match_soup, 'away', 'sot'),
            'possession': extract_fbref_stat(match_soup, 'away', 'possession'),
            'passes': extract_fbref_stat(match_soup, 'away', 'passes'),
            'pass_accuracy': extract_fbref_stat(match_soup, 'away', 'pass_accuracy'),
            'fouls': extract_fbref_stat(match_soup, 'away', 'fouls'),
            'yellow_cards': extract_fbref_stat(match_soup, 'away', 'yellow_cards'),
            'red_cards': extract_fbref_stat(match_soup, 'away', 'red_cards'),
            'offsides': extract_fbref_stat(match_soup, 'away', 'offsides'),
            'corners': extract_fbref_stat(match_soup, 'away', 'corners')
        }
        
        # Insert both team stats
        with conn.cursor() as cur:
            for stats in [home_stats, away_stats]:
                # Filter out None values
                filtered_stats = {k: v for k, v in stats.items() if v is not None}
                
                # Get column names and values
                columns = ', '.join(filtered_stats.keys())
                placeholders = ', '.join(['%s'] * len(filtered_stats))
                values = tuple(filtered_stats.values())
                
                # Construct the query
                query = f"""
                INSERT INTO match_stats ({columns}, scrape_date)
                VALUES ({placeholders}, CURRENT_TIMESTAMP)
                ON CONFLICT (match_id, team_id) DO UPDATE SET
                    scrape_date = CURRENT_TIMESTAMP
                """
                
                # Add each column to update
                update_parts = []
                for col in filtered_stats.keys():
                    if col not in ['match_id', 'team_id']:
                        update_parts.append(f"{col} = EXCLUDED.{col}")
                
                if update_parts:
                    query += ", " + ", ".join(update_parts)
                
                cur.execute(query, values)
                stats_created += 1
        
        # Be polite and delay between requests
        time.sleep(1)
    
    conn.commit()
    logger.info(f"Created/updated {stats_created} match stat records")
    return stats_created


def run_pipeline(args):
    """
    Execute the full data pipeline or specific parts based on arguments.
    
    Args:
        args: Command line arguments
    """
    conn = None
    try:
        # Connect to the database
        conn = get_db_connection()
        
        # Bootstrap schema if necessary
        if args.bootstrap_only:
            bootstrap_schema(conn)
            return
        
        # Always ensure schema exists
        bootstrap_schema(conn)
        
        # Fetch fixtures if requested or running full pipeline
        if args.fetch_fixtures_only or not (args.scrape_stats_only):
            fetch_fixtures(conn, args.days_ahead)
        
        # Scrape stats if requested or running full pipeline
        if args.scrape_stats_only or not (args.fetch_fixtures_only):
            completed_fixtures = get_completed_fixtures(conn)
            if completed_fixtures:
                scrape_match_stats(conn, completed_fixtures)
            
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Soccer Data Pipeline - Phase 0")
    parser.add_argument('--bootstrap-only', action='store_true', 
                       help='Only bootstrap the database schema')
    parser.add_argument('--fetch-fixtures-only', action='store_true',
                       help='Only fetch and update fixtures')
    parser.add_argument('--scrape-stats-only', action='store_true',
                       help='Only scrape stats for completed fixtures')
    parser.add_argument('--days-ahead', type=int, default=7,
                       help='Number of days to look ahead for fixtures')
    
    args = parser.parse_args()
    run_pipeline(args)