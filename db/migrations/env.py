import os
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from pathlib import Path
import yaml

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
fileConfig(config.config_file_name)

# Load our application’s metadata
from db.models import Base  # adjust import to match your package layout
target_metadata = Base.metadata

# Load DB URL from YAML config
# Assumes your working directory is project root
env = os.getenv("FLASK_ENV", "dev")
cfg_file = Path(f"config/{env}.yml")
cfg = yaml.safe_load(cfg_file.read_text())
config.set_main_option("sqlalchemy.url", cfg["database"]["url"])

def run_migrations_offline():
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
