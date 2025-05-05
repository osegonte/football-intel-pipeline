# tests/test_db.py
import pytest
import psycopg2
from pipeline import get_or_create_team

def test_get_or_create_team_creates_new(db_connection):
    """Test team creation when it doesn't exist."""
    # Setup - ensure team doesn't exist
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM teams WHERE name = 'Test Team'")
        db_connection.commit()
    
    # Execute
    team_id = get_or_create_team(db_connection, "Test Team", 12345)
    
    # Verify
    with db_connection.cursor() as cur:
        cur.execute("SELECT team_id, sofascore_id FROM teams WHERE name = 'Test Team'")
        result = cur.fetchone()
        
    assert result is not None
    assert result[0] == team_id
    assert result[1] == 12345

def test_get_or_create_team_returns_existing(db_connection):
    """Test team lookup when it already exists."""
    # Setup - ensure team exists
    with db_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO teams (name, sofascore_id) 
            VALUES ('Existing Team', 54321)
            ON CONFLICT (name) DO NOTHING
            RETURNING team_id
        """)
        existing_id = cur.fetchone()[0]
        db_connection.commit()
    
    # Execute
    team_id = get_or_create_team(db_connection, "Existing Team", 99999)
    
    # Verify - ID matches existing and sofascore_id wasn't changed
    assert team_id == existing_id
    
    with db_connection.cursor() as cur:
        cur.execute("SELECT sofascore_id FROM teams WHERE name = 'Existing Team'")
        sofascore_id = cur.fetchone()[0]
    
    assert sofascore_id == 54321  # Original value preserved