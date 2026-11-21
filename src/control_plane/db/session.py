"""Engine and session factory.

The `AsyncSession` is the unit of work: each use case opens one transaction
(`async with session.begin()`), every repository call inside it shares that
transaction, and it commits or rolls back as a whole.
"""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from control_plane.config import Settings

SessionFactory = async_sessionmaker[AsyncSession]


def create_engine(settings: Settings) -> AsyncEngine:
    # pool_pre_ping: survive a DB restart without handing out a dead connection.
    return create_async_engine(settings.database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    # expire_on_commit=False: we read returned rows after commit to build responses.
    return async_sessionmaker(engine, expire_on_commit=False)
