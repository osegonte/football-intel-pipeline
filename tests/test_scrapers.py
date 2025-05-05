"""Tests for web scraping functionality."""
import os
from unittest.mock import patch, MagicMock
import pytest
import json
from pipeline import fetch_fixtures, scrape_match_stats

# Set test database URL for all tests
os.environ['DATABASE_URL'] = 'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline_test'

# Sample SofaScore API response
SOFASCORE_SAMPLE = {
    "events": [
        {
            "id": 123456,
            "homeTeam": {"name": "Manchester United", "id": 1001},
            "awayTeam": {"name": "Liverpool", "id": 1002},
            "tournament": {"name": "Premier League"},
            "startTimestamp": 1714931999,  # Example timestamp (future date)
            "status": {"type": "notstarted"}
        },
        {
            "id": 123457,
            "homeTeam": {"name": "Arsenal", "id": 1003},
            "awayTeam": {"name": "Chelsea", "id": 1004},
            "tournament": {"name": "Premier League"},
            "startTimestamp": 1715018399,  # Example timestamp (future date)
            "status": {"type": "notstarted"}
        }
    ]
}

@pytest.fixture
def mock_sofascore_response():
    """Create a mock HTTP response for SofaScore API."""
    mock = MagicMock()
    mock.text = json.dumps(SOFASCORE_SAMPLE)
    mock.json.return_value = SOFASCORE_SAMPLE
    return mock

@patch('pipeline.make_request')
def test_fetch_fixtures(mock_make_request, mock_sofascore_response, db_connection):
    """Test fixture fetching with mocked API responses."""
    # Setup mocked response
    mock_make_request.return_value = mock_sofascore_response
    
    # Execute the function
    fetch_fixtures(db_connection, days_ahead=1)
    
    # Verify API was called
    mock_make_request.assert_called()
    
    # Verify fixtures were added to database
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fixtures")
        count = cur.fetchone()[0]
        
        # We should have at least our 2 mock fixtures
        assert count >= 2
        
        # Check one specific fixture
        cur.execute("""
            SELECT f.match_id, ht.name, at.name 
            FROM fixtures f
            JOIN teams ht ON f.home_team_id = ht.team_id
            JOIN teams at ON f.away_team_id = at.team_id
            WHERE f.match_id = 'sofascore_123456'
        """)
        result = cur.fetchone()
        
        if result:
            assert result[0] == 'sofascore_123456'
            assert result[1] == 'Manchester United'
            assert result[2] == 'Liverpool'