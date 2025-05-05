"""Tests for the soccer data pipeline."""
import os
import pytest
from pipeline import (
    bootstrap_schema, 
    get_or_create_team, 
    fetch_fixtures, 
    get_completed_fixtures
)

# Set test database URL for all tests
os.environ['DATABASE_URL'] = 'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline_test'

def test_bootstrap_schema(db_connection):
    """Test that the schema can be created correctly."""
    bootstrap_schema(db_connection)
    
    # Verify tables exist
    with db_connection.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'teams')")
        assert cur.fetchone()[0] is True
        
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'fixtures')")
        assert cur.fetchone()[0] is True
        
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'match_stats')")
        assert cur.fetchone()[0] is True

def test_get_or_create_team(db_connection):
    """Test team creation and retrieval."""
    # Setup - ensure test team doesn't exist
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM teams WHERE name = 'Test Team'")
        db_connection.commit()
    
    # Test creating a new team
    team_id = get_or_create_team(db_connection, "Test Team", 12345)
    
    # Verify team was created
    with db_connection.cursor() as cur:
        cur.execute("SELECT team_id, sofascore_id FROM teams WHERE name = 'Test Team'")
        result = cur.fetchone()
    
    assert result is not None
    assert result[0] == team_id
    assert result[1] == 12345
    
    # Test getting an existing team
    same_team_id = get_or_create_team(db_connection, "Test Team", 99999)
    
    # Verify the same ID is returned and sofascore_id is not updated
    assert same_team_id == team_id
    
    with db_connection.cursor() as cur:
        cur.execute("SELECT sofascore_id FROM teams WHERE name = 'Test Team'")
        sofascore_id = cur.fetchone()[0]
    
    assert sofascore_id == 12345  # Original value should be preserved