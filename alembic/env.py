"""Alembic migration environment (async).

Imports every module's models so ``--autogenerate`` sees the full metadata, and
ensures each module's Postgres schema exists before migrating.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import get_settings
from app.db import Base

# Import models for autogenerate metadata (side-effect imports).
from app.audit import models as _audit_models  # noqa: F401
from app.ingestion import models as _ingestion_models  # noqa: F401
from app.recipients import models as _recipient_models  # noqa: F401

target_metadata = Base.metadata
SCHEMAS = ("ingestion", "recipients", "audit")


def _url() -> str:
    return get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection) -> None:
    for schema in SCHEMAS:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    context.configure(
        connection=connection, target_metadata=target_metadata, include_schemas=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(_url())
    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)
        await connection.commit()
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
