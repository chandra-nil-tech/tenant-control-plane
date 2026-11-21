"""Applies worker progress updates, all inside ONE transaction:

* Idempotent     - the `message_id` is inserted into `processed_messages` (primary
                   key). Seen before -> no-op. It commits together with the change.
* Order-tolerant - the task row is locked and status only moves *forward*:
                   `in_progress` after `done`, or anything for a terminal task, is
                   stale and changes nothing.
* Concurrency-safe - `SELECT ... FOR UPDATE` on the task serialises updates for the
                   same task, even across consumer replicas.

Lock order is always task -> tenant. The API path never locks an existing task
row, so the two paths can't deadlock.
"""

import logging
from enum import StrEnum
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.db.session import SessionFactory
from control_plane.domain.errors import UnknownTaskInUpdate
from control_plane.domain.models import TaskStatus, TaskType
from control_plane.domain.state_machine import should_apply_task_update, tenant_status_after_outcome
from control_plane.messaging.contracts import TaskUpdateMessage
from control_plane.repositories.processed_messages import ProcessedMessageRepository
from control_plane.repositories.tasks import TaskRepository
from control_plane.repositories.tenants import TenantRepository

log = logging.getLogger(__name__)


class UpdateOutcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"  # this message_id was processed before
    STALE = "stale"  # new message_id, but it would move the task backwards


class TaskUpdateService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def apply(self, update: TaskUpdateMessage) -> UpdateOutcome:
        incoming = TaskStatus(update.status)
        ctx = {"task_id": update.task_id, "message_id": update.message_id, "to_status": incoming}

        async with self._session_factory() as session, session.begin():
            tasks = TaskRepository(session)
            task = await tasks.get_for_update(update.task_id)
            if task is None:
                # Tasks are only published after commit, so a real worker can't know
                # an id we don't: this message is malformed (poison).
                raise UnknownTaskInUpdate(
                    f"unknown task {update.task_id}", task_id=str(update.task_id)
                )

            first_time = await ProcessedMessageRepository(session).try_record(
                message_id=update.message_id, task_id=update.task_id
            )
            if not first_time:
                log.info("task update ignored: duplicate", extra={**ctx, "outcome": "duplicate"})
                return UpdateOutcome.DUPLICATE

            if not should_apply_task_update(task.status, incoming):
                # The message_id stays recorded, so a redelivery is a cheap duplicate.
                log.info(
                    "task update ignored: stale",
                    extra={**ctx, "from_status": task.status, "outcome": "stale"},
                )
                return UpdateOutcome.STALE

            previous = task.status
            task = await tasks.set_status(task.id, incoming)
            log.info("task status changed", extra={**ctx, "from_status": previous})

            if incoming.is_terminal:
                await self._apply_outcome_to_tenant(session, task.tenant_id, task.type, incoming)

        return UpdateOutcome.APPLIED

    @staticmethod
    async def _apply_outcome_to_tenant(
        session: AsyncSession, tenant_id: UUID, task_type: TaskType, outcome: TaskStatus
    ) -> None:
        tenants = TenantRepository(session)
        tenant = await tenants.get_for_update(tenant_id)
        if tenant is None:  # impossible: tasks.tenant_id is a foreign key
            raise RuntimeError(f"task references missing tenant {tenant_id}")

        next_status = tenant_status_after_outcome(task_type, outcome, tenant.status)
        if next_status is None:
            # Unreachable given the API guards (see state_machine.py). If it ever
            # happens, keep the task outcome but don't overwrite the tenant blindly.
            log.error(
                "tenant not in the state this task expects; tenant left unchanged",
                extra={
                    "tenant_id": tenant_id,
                    "task_type": task_type,
                    "from_status": tenant.status,
                },
            )
            return

        await tenants.transition(tenant_id, allowed_from={tenant.status}, to=next_status)
        log.info(
            "tenant status changed by task outcome",
            extra={
                "tenant_id": tenant_id,
                "task_type": task_type,
                "from_status": tenant.status,
                "to_status": next_status,
            },
        )
