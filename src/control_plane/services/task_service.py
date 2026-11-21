"""Read-only task queries (tasks are never changed over HTTP)."""

import uuid

from control_plane.db.session import SessionFactory
from control_plane.db.tables import TaskRow
from control_plane.domain.errors import TaskNotFound, TenantNotFound
from control_plane.domain.models import TaskStatus, TaskType
from control_plane.repositories.tasks import TaskRepository
from control_plane.repositories.tenants import TenantRepository


class TaskQueryService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def get(self, task_id: uuid.UUID) -> TaskRow:
        async with self._session_factory() as session:
            task = await TaskRepository(session).get(task_id)
        if task is None:
            raise TaskNotFound(f"task {task_id} not found", task_id=str(task_id))
        return task

    async def list(
        self,
        *,
        tenant_id: uuid.UUID | None = None,
        status: TaskStatus | None = None,
        task_type: TaskType | None = None,
        limit: int,
        offset: int,
    ) -> list[TaskRow]:
        async with self._session_factory() as session:
            # Tasks of a tenant that doesn't exist: 404, not an empty list.
            if tenant_id is not None and await TenantRepository(session).get(tenant_id) is None:
                raise TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
            return await TaskRepository(session).list(
                tenant_id=tenant_id, status=status, task_type=task_type, limit=limit, offset=offset
            )
