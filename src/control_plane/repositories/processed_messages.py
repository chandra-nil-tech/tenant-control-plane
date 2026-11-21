"""Idempotency keys for inbound messages."""

import uuid

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.tables import ProcessedMessageRow


class ProcessedMessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_record(self, *, message_id: str, task_id: uuid.UUID) -> bool:
        """Claim `message_id`. Returns False if it was already processed.

        If two consumers get the same message at once, both INSERT; the second
        blocks on the primary key until the first commits, then hits the conflict
        and gets False. If the first rolls back instead, the second succeeds.
        Either way the update is applied exactly once.
        """
        stmt = (
            insert(ProcessedMessageRow)
            .values(message_id=message_id, task_id=task_id)
            .on_conflict_do_nothing(index_elements=[ProcessedMessageRow.message_id])
            .returning(ProcessedMessageRow.message_id)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None
