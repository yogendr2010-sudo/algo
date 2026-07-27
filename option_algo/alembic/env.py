"""
Alembic environment configuration — async compatible.

Uses the same DATABASE_URL as the application (via backend.config)
so migrations stay in sync with the actual database, regardless of
whether the app runs on SQLite (dev) or Postgres (production).

The migration itself runs in a sync context (Alembic's run_migrations
is sync), but this config creates both an async engine (for autogenerate)
and a sync engine (for actual migration execution) from the same URL.
"""

import asyncio
from logging.config import fileConfig

from alembic import context

# Alembic Config object, which provides access to .ini file values.
config = context.config

# Set up Python loggers from the .ini file.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── Import ALL models so Alembic's autogenerate can detect them ──
from backend.db.database import Base
from backend.db import models  # noqa: F401 — registers all ORM models

# ── Database URL from application config ─────────────────────────
# Use the same async URL the app uses. Alembic can handle both
# async and sync drivers; for migration execution we use the sync
# variant (sqlite / psycopg2) since Alembic's migration runner is
# synchronous.
from backend.config import get_settings

settings = get_settings()
DATABASE_URL = settings.DATABASE_URL

# Build a sync-compatible URL from the async URL.
#   asyncpg  → psycopg2  (for Postgres)
#   aiosqlite → pysqlite  (for SQLite via pysqlite — no extra driver needed)
SYNC_DATABASE_URL = (
    DATABASE_URL.replace("postgresql+asyncpg", "postgresql+psycopg2")
    if "postgresql" in DATABASE_URL
    else DATABASE_URL.replace("sqlite+aiosqlite", "sqlite")
)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    Configures the context with just a URL and not an Engine,
    though an Engine is acceptable here as well.  By skipping the
    Engine creation we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given SQL to a
    script file (sqlalchemy.output / stdout by default).
    """
    url = config.get_main_option("sqlalchemy.url", SYNC_DATABASE_URL)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    """Run migrations against the given sync connection."""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    Creates a sync engine from the sync-compatible URL and runs
    the migration against the live database.  Works for both
    SQLite (dev) and Postgres (production).
    """
    from sqlalchemy import create_engine

    connect_args = {}
    if "sqlite" in SYNC_DATABASE_URL:
        connect_args["check_same_thread"] = False

    engine = create_engine(SYNC_DATABASE_URL, connect_args=connect_args)

    with engine.connect() as connection:
        do_run_migrations(connection)

    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

