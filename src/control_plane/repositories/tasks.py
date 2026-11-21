"""Task persistence."""

import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.tables import TaskRow
from control_plane.domain.models import TaskStatus, TaskType


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, *, tenant_id: uuid.UUID, task_type: TaskType) -> TaskRow:
        task = TaskRow(
            id=uuid.uuid4(), tenant_id=tenant_id, type=task_type, status=TaskStatus.ACCEPTED
        )
        self._session.add(task)
        # INSERT now (inside the open transaction) so created_at is populated.
        await self._session.flush()
        await self._session.refresh(task)
        return task

    async def get(self, task_id: uuid.UUID) -> TaskRow | None:
        return await self._session.get(TaskRow, task_id, populate_existing=True)

    async def get_for_update(self, task_id: uuid.UUID) -> TaskRow | None:
        """Row-lock the task: serialises concurrent updates for the same task
        (e.g. `in_progress` and `done` handled at the same time)."""
        stmt = select(TaskRow).where(TaskRow.id == task_id).with_for_update()
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def set_status(self, task_id: uuid.UUID, status: TaskStatus) -> TaskRow:
        stmt = (
            update(TaskRow)
            .where(TaskRow.id == task_id)
            .values(status=status, updated_at=func.now())
            .returning(TaskRow)
            .execution_options(synchronize_session=False)
        )
        return (await self._session.execute(stmt)).scalar_one()

    async def list(
        self,
        *,
        tenant_id: uuid.UUID | None,
        status: TaskStatus | None,
        task_type: TaskType | None,
        limit: int,
        offset: int,
    ) -> list[TaskRow]:
        stmt = select(TaskRow).order_by(TaskRow.created_at.desc(), TaskRow.id)
        if tenant_id is not None:
            stmt = stmt.where(TaskRow.tenant_id == tenant_id)
        if status is not None:
            stmt = stmt.where(TaskRow.status == status)
        if task_type is not None:
            stmt = stmt.where(TaskRow.type == task_type)
        return list((await self._session.execute(stmt.limit(limit).offset(offset))).scalars())
