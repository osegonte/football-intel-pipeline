"""Shared test fixtures for pipeline tests."""
import os
import pytest
import psycopg2

@pytest.fixture(scope="function")
def db_connection():
    """Create a test database connection that's rolled back after each test."""
    # Use test database
    conn = psycopg2.connect(os.environ.get(
        'DATABASE_URL', 
        'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline_test'
    ))
    conn.autocommit = False
    
    yield conn
    
    # Rollback any changes
    conn.rollback()
    conn.close()

@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    """Set up test database once for all tests."""
    conn = psycopg2.connect(os.environ.get(
        'DATABASE_URL', 
        'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline_test'
    ))
    conn.autocommit = True
    
    with conn.cursor() as cur:
        # Drop tables if they exist
        cur.execute("DROP TABLE IF EXISTS match_stats CASCADE")
        cur.execute("DROP TABLE IF EXISTS fixtures CASCADE")
        cur.execute("DROP TABLE IF EXISTS teams CASCADE")
    
    conn.close()
    
    # Run bootstrap_schema to create tables
    from pipeline import bootstrap_schema
    conn = psycopg2.connect(os.environ.get(
        'DATABASE_URL', 
        'postgresql://pipeline_user:password@localhost:5432/soccer_pipeline_test'
    ))
    bootstrap_schema(conn)
    conn.commit()
    conn.close()