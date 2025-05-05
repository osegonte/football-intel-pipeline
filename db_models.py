import os
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, Boolean,
    Numeric, SmallInteger, ForeignKey, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()  # loads .env into os.environ

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Lookup tables
class Team(Base):
    __tablename__ = "teams"
    team_id    = Column(Integer, primary_key=True, autoincrement=True)
    name       = Column(String, unique=True, nullable=False)
    fbref_slug = Column(String, unique=True, nullable=False)
    sofascore_id = Column(Integer, unique=True)

# Fixtures
class Fixture(Base):
    __tablename__ = "fixtures"
    match_id    = Column(String, primary_key=True)
    date        = Column(Date, nullable=False)
    home_team_id = Column(Integer, ForeignKey("teams.team_id"), nullable=False)
    away_team_id = Column(Integer, ForeignKey("teams.team_id"), nullable=False)
    scrape_date = Column(DateTime, default=datetime.utcnow, nullable=False)

# Box-score stats
class MatchStat(Base):
    __tablename__ = "match_stats"
    match_id      = Column(String, ForeignKey("fixtures.match_id"), primary_key=True)
    team_id       = Column(Integer, ForeignKey("teams.team_id"), primary_key=True)
    gf            = Column(SmallInteger)
    ga            = Column(SmallInteger)
    xg            = Column(Numeric)
    xga           = Column(Numeric)
    sh            = Column(SmallInteger)
    sot           = Column(SmallInteger)
    possession    = Column(Numeric)
    scrape_date   = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (Index("idx_stats_scrape_date", "scrape_date"),)

def init_db():
    """Create all tables."""
    Base.metadata.create_all(bind=engine)
