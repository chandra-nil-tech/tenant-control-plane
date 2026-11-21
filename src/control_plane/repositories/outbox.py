"""Transactional outbox persistence."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.tables import OutboxRow


@dataclass(frozen=True, slots=True)
class OutboundEvent:
    event_id: UUID  # == task id (the task *is* the event)
    routing_key: str
    payload: dict[str, Any]


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboundEvent) -> None:
        """Called inside the business transaction: if it rolls back, this row goes
        with it, so an event can never exist for a change that didn't happen."""
        self._session.add(
            OutboxRow(event_id=event.event_id, routing_key=event.routing_key, payload=event.payload)
        )
        await self._session.flush()

    async def claim_batch(self, limit: int) -> list[OutboxRow]:
        """Lock up to `limit` unpublished rows, oldest first.

        `FOR UPDATE SKIP LOCKED` lets several relays (one per API replica) run at
        once: each takes a disjoint batch instead of blocking or double-publishing.
        """
        stmt = (
            select(OutboxRow)
            .where(OutboxRow.published_at.is_(None))
            .order_by(OutboxRow.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self._session.execute(stmt)).scalars())

    async def mark_published(self, row_id: int) -> None:
        await self._session.execute(
            update(OutboxRow).where(OutboxRow.id == row_id).values(published_at=func.now())
        )
