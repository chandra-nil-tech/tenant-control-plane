"""Tenant use cases: create, update, delete, read.

Every mutating use case is ONE database transaction:

    1. conditionally write the tenant (guard + write in a single SQL statement)
    2. insert the task (status `accepted`)
    3. insert the outbox row (the event)
  COMMIT  -> the outbox relay publishes the event afterwards

Steps 1-3 commit or roll back together, which is how "publish exactly one event,
only after the write is durably committed, never on rollback" holds: this code
never talks to the broker.
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.session import SessionFactory
from control_plane.db.tables import TaskRow, TenantRow
from control_plane.domain.errors import (
    TenantAlreadyExists,
    TenantNotFound,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.domain.models import TenantStatus
from control_plane.domain.state_machine import (
    CREATE_TRANSITION,
    DELETE_TRANSITION,
    UPDATE_TRANSITION,
    ApiTransition,
)
from control_plane.repositories.outbox import OutboxRepository
from control_plane.repositories.tasks import TaskRepository
from control_plane.repositories.tenants import TenantRepository
from control_plane.services.events import build_task_event

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TenantOperation:
    """Result of a mutating call: the tenant as written, and the task it created."""

    tenant: TenantRow
    task: TaskRow


class TenantService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def create(self, *, slug: str, name: str) -> TenantOperation:
        async with self._session_factory() as session, session.begin():
            tenant = await TenantRepository(session).insert_if_slug_free(
                slug=slug, name=name, status=CREATE_TRANSITION.to
            )
            if tenant is None:
                # Raising inside the `with` block rolls the transaction back.
                raise TenantAlreadyExists(f"a tenant with slug '{slug}' already exists", slug=slug)
            task = await self._enqueue_task(session, tenant, CREATE_TRANSITION)

        log.info(
            "tenant created",
            extra={"tenant_id": tenant.id, "task_id": task.id, "to_status": tenant.status},
        )
        return TenantOperation(tenant, task)

    async def update(
        self, tenant_id: uuid.UUID, *, expected_version: int, changes: dict[str, Any]
    ) -> TenantOperation:
        return await self._mutate(
            tenant_id, UPDATE_TRANSITION, expected_version=expected_version, changes=changes
        )

    async def delete(self, tenant_id: uuid.UUID) -> TenantOperation:
        # Soft delete: the row stays as a `destroyed` tombstone so its slug stays
        # reserved (spec: unique "including against destroyed tenants").
        return await self._mutate(tenant_id, DELETE_TRANSITION, expected_version=None, changes=None)

    async def get(self, tenant_id: uuid.UUID) -> TenantRow:
        async with self._session_factory() as session:
            tenant = await TenantRepository(session).get(tenant_id)
        if tenant is None:
            raise TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
        return tenant

    async def list(
        self, *, status: TenantStatus | None, limit: int, offset: int
    ) -> list[TenantRow]:
        async with self._session_factory() as session:
            return await TenantRepository(session).list(status=status, limit=limit, offset=offset)

    # ---------------------------------------------------------------- internals

    async def _mutate(
        self,
        tenant_id: uuid.UUID,
        transition: ApiTransition,
        *,
        expected_version: int | None,
        changes: dict[str, Any] | None,
    ) -> TenantOperation:
        async with self._session_factory() as session, session.begin():
            tenants = TenantRepository(session)
            tenant = await tenants.transition(
                tenant_id,
                allowed_from=transition.allowed_from,
                to=transition.to,
                expected_version=expected_version,
                changes=changes,
            )
            if tenant is None:
                # The atomic UPDATE matched nothing; re-read only to say *why*.
                raise await self._explain_rejection(
                    tenants, tenant_id, transition, expected_version
                )
            task = await self._enqueue_task(session, tenant, transition)

        log.info(
            "tenant transition accepted",
            extra={
                "tenant_id": tenant.id,
                "task_id": task.id,
                "task_type": task.type,
                "to_status": tenant.status,
            },
        )
        return TenantOperation(tenant, task)

    @staticmethod
    async def _enqueue_task(
        session: AsyncSession, tenant: TenantRow, transition: ApiTransition
    ) -> TaskRow:
        task = await TaskRepository(session).add(
            tenant_id=tenant.id, task_type=transition.task_type
        )
        await OutboxRepository(session).add(build_task_event(task))
        return task

    @staticmethod
    async def _explain_rejection(
        tenants: TenantRepository,
        tenant_id: uuid.UUID,
        transition: ApiTransition,
        expected_version: int | None,
    ) -> Exception:
        current = await tenants.get(tenant_id)
        if current is None:
            return TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
        # A forbidden status wins over a stale version: if the tenant is `updating`,
        # "refresh your version and retry" would fail again anyway.
        if current.status not in transition.allowed_from:
            log.info(
                "transition rejected: status",
                extra={"tenant_id": tenant_id, "from_status": current.status},
            )
            return TenantUpdateNotAllowed(
                f"tenant is '{current.status}'; allowed from: "
                f"{', '.join(sorted(s.value for s in transition.allowed_from))}",
                tenant_id=str(tenant_id),
                current_status=current.status.value,
            )
        log.info("transition rejected: stale version", extra={"tenant_id": tenant_id})
        return TenantVersionConflict(
            f"version {expected_version} is stale; current version is {current.version}",
            tenant_id=str(tenant_id),
            expected_version=expected_version,
            current_version=current.version,
        )
