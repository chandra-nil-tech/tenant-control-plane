"""Tenant persistence.

The key pattern is the *conditional update*:

    UPDATE tenants SET ... WHERE id = :id AND status IN (...) [AND version = :v]
    RETURNING *

The guard ("is this transition allowed?") and the write are ONE atomic SQL
statement. Two concurrent PATCHes with the same version both run it: Postgres
row-locks the tenant, the first bumps `version`, the second re-checks its WHERE
against the new row, matches nothing, and we report a conflict. No read-then-write
race and no application-level lock.
"""

import uuid
from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.tables import TenantRow
from control_plane.domain.models import TenantStatus


class TenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_slug_free(
        self, *, slug: str, name: str, status: TenantStatus
    ) -> TenantRow | None:
        """Insert a tenant, or return None if the slug is taken.

        With `ON CONFLICT DO NOTHING`, if two transactions insert the same slug, the
        second waits on the unique index until the first commits, then inserts
        nothing. Exactly one winner, without catching an IntegrityError.
        """
        stmt = (
            insert(TenantRow)
            .values(id=uuid.uuid4(), slug=slug, name=name, status=status, version=1)
            .on_conflict_do_nothing(index_elements=[TenantRow.slug])
            .returning(TenantRow)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get(self, tenant_id: uuid.UUID) -> TenantRow | None:
        return await self._session.get(TenantRow, tenant_id, populate_existing=True)

    async def get_for_update(self, tenant_id: uuid.UUID) -> TenantRow | None:
        """Row-lock the tenant for the rest of the transaction."""
        stmt = select(TenantRow).where(TenantRow.id == tenant_id).with_for_update()
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def transition(
        self,
        tenant_id: uuid.UUID,
        *,
        allowed_from: Iterable[TenantStatus],
        to: TenantStatus,
        expected_version: int | None = None,
        changes: dict[str, Any] | None = None,
    ) -> TenantRow | None:
        """Atomically move a tenant to `to` if its status (and optionally version) match.

        Returns the updated row, or None if nothing matched (the caller then works
        out why: not found, wrong status, or stale version). Always bumps `version`.
        """
        stmt = (
            update(TenantRow)
            .where(TenantRow.id == tenant_id, TenantRow.status.in_(list(allowed_from)))
            .values(
                **(changes or {}),
                status=to,
                version=TenantRow.version + 1,
                updated_at=func.now(),
            )
            .returning(TenantRow)
            .execution_options(synchronize_session=False)
        )
        if expected_version is not None:
            stmt = stmt.where(TenantRow.version == expected_version)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def list(
        self, *, status: TenantStatus | None, limit: int, offset: int
    ) -> list[TenantRow]:
        stmt = select(TenantRow).order_by(TenantRow.created_at.desc(), TenantRow.id)
        if status is not None:
            stmt = stmt.where(TenantRow.status == status)
        return list((await self._session.execute(stmt.limit(limit).offset(offset))).scalars())
