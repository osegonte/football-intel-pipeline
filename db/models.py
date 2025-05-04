from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, Numeric, SmallInteger, ForeignKey, Text, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# Lookup Tables
class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    fbref_slug = Column(String(100), nullable=False, unique=True)
    sofascore_id = Column(Integer, nullable=True, unique=True)
    
    matches = relationship("Match", back_populates="team")

class Competition(Base):
    __tablename__ = 'competitions'
    comp_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    fixtures = relationship("Fixture", back_populates="competition")

class Season(Base):
    __tablename__ = 'seasons'
    season_id = Column(Integer, primary_key=True)
    year = Column(String(20), nullable=False, unique=True)
    
    fixtures = relationship("Fixture", back_populates="season")

class Venue(Base):
    __tablename__ = 'venues'
    venue_id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    
    fixtures = relationship("Fixture", back_populates="venue")

class League(Base):
    __tablename__ = 'leagues'
    league_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)

# Fixtures Table
class Fixture(Base):
    __tablename__ = 'fixtures'
    match_id = Column(String(150), primary_key=True)
    date = Column(Date, nullable=False)
    home_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=False)
    away_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=False)
    venue_id = Column(Integer, ForeignKey('venues.venue_id'), nullable=False)
    comp_id = Column(Integer, ForeignKey('competitions.comp_id'), nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.season_id'), nullable=False)
    round = Column(String(50), nullable=True)
    scrape_date = Column(DateTime, nullable=False)

    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    competition = relationship("Competition", back_populates="fixtures")
    season = relationship("Season", back_populates="fixtures")
    venue = relationship("Venue", back_populates="fixtures")

# Matches Table (Box-score stats)
class Match(Base):
    __tablename__ = 'matches'
    match_id = Column(String(150), ForeignKey('fixtures.match_id'), primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'), primary_key=True)
    opponent_id = Column(Integer, ForeignKey('teams.team_id'), nullable=False)
    is_home = Column(Boolean, nullable=False)
    result = Column(String(1), nullable=False)
    gf = Column(SmallInteger, nullable=False)
    ga = Column(SmallInteger, nullable=False)
    points = Column(SmallInteger, nullable=False)
    xg = Column(Numeric, nullable=True)
    xga = Column(Numeric, nullable=True)
    sh = Column(SmallInteger, nullable=True)
    sot = Column(SmallInteger, nullable=True)
    dist = Column(Numeric, nullable=True)
    fk = Column(SmallInteger, nullable=True)
    pk = Column(SmallInteger, nullable=True)
    pkatt = Column(SmallInteger, nullable=True)
    possession = Column(Numeric, nullable=True)
    corners_for = Column(SmallInteger, nullable=True)
    corners_against = Column(SmallInteger, nullable=True)
    league_id = Column(Integer, ForeignKey('leagues.league_id'), nullable=False)
    league_name = Column(String(100), nullable=False)
    scrape_date = Column(DateTime, nullable=False)

    team = relationship("Team", foreign_keys=[team_id], back_populates="matches")

    __table_args__ = (
        Index('idx_matches_scrape_date', 'scrape_date'),
    )
