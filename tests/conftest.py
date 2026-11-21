"""Shared fixtures.

Integration tests run against a REAL PostgreSQL (row locks, ON CONFLICT, SKIP
LOCKED and partial indexes are exactly the behaviour under test; SQLite or mocks
would prove nothing). The schema is built by running the real Alembic migrations,
so the migrations are tested too. The broker is replaced by fakes at the port
boundary (EventPublisher / IncomingMessage), which is what the ports are for.
"""

import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from control_plane.config import Settings
from control_plane.db.session import SessionFactory, create_session_factory
from control_plane.main import create_app
from control_plane.services.task_update_service import TaskUpdateService
from control_plane.services.tenant_service import TenantService

ROOT = Path(__file__).resolve().parent.parent
TEST_DB_URL = os.environ.get(
    "CP_TEST_DATABASE_URL",
    "postgresql+asyncpg://control_plane:control_plane@localhost:5432/control_plane_test",
)


@pytest.fixture(scope="session")
def migrated_db() -> Iterator[str]:
    """Reset the test database and apply all migrations once per test session."""

    async def _reset() -> None:
        engine = create_async_engine(TEST_DB_URL)
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
        await engine.dispose()

    import asyncio

    asyncio.run(_reset())
    # Run Alembic in a subprocess: its env.py owns an event loop of its own.
    subprocess.run(  # fixed argv, no shell
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env={**os.environ, "CP_DATABASE_URL": TEST_DB_URL},
        check=True,
        capture_output=True,
    )
    yield TEST_DB_URL


@pytest.fixture(scope="session")
async def engine(migrated_db: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_db, pool_size=20, max_overflow=20)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> AsyncIterator[SessionFactory]:
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE tenants, tasks, outbox, processed_messages RESTART IDENTITY CASCADE")
        )
    yield create_session_factory(engine)


@pytest.fixture
def tenant_service(session_factory: SessionFactory) -> TenantService:
    return TenantService(session_factory)


@pytest.fixture
def update_service(session_factory: SessionFactory) -> TaskUpdateService:
    return TaskUpdateService(session_factory)


@pytest.fixture
async def app(session_factory: SessionFactory, migrated_db: str) -> AsyncIterator[FastAPI]:
    # session_factory dependency: guarantees the tables are truncated first.
    settings = Settings(database_url=migrated_db, enable_messaging=False, log_level="WARNING")
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
