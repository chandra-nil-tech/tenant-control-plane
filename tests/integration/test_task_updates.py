"""Inbound update semantics: lifecycle, idempotency, order tolerance, poison."""

import uuid

import pytest
from sqlalchemy import text

from control_plane.db.session import SessionFactory
from control_plane.domain.errors import UnknownTaskInUpdate
from control_plane.domain.models import TaskStatus, TenantStatus
from control_plane.services.task_service import TaskQueryService
from control_plane.services.task_update_service import TaskUpdateService, UpdateOutcome
from control_plane.services.tenant_service import TenantService
from tests.helpers import update_msg

pytestmark = pytest.mark.integration

S = TaskStatus


@pytest.fixture
def tasks(session_factory: SessionFactory) -> TaskQueryService:
    return TaskQueryService(session_factory)


async def test_full_deploy_lifecycle(
    tenant_service: TenantService, update_service: TaskUpdateService, tasks: TaskQueryService
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")

    assert (
        await update_service.apply(update_msg(op.task.id, S.IN_PROGRESS)) is UpdateOutcome.APPLIED
    )
    assert (await tasks.get(op.task.id)).status is S.IN_PROGRESS
    t = await tenant_service.get(op.tenant.id)
    assert (t.status, t.version) == (TenantStatus.PROVISIONING, 1)  # non-terminal: tenant untouched

    assert await update_service.apply(update_msg(op.task.id, S.DONE)) is UpdateOutcome.APPLIED
    assert (await tasks.get(op.task.id)).status is S.DONE
    t = await tenant_service.get(op.tenant.id)
    assert (t.status, t.version) == (TenantStatus.ACTIVE, 2)


@pytest.mark.parametrize(
    ("make_tenant_busy", "expected"),
    [
        ("deploy", TenantStatus.FAILED),
        ("update", TenantStatus.FAILED),
        ("destroy", TenantStatus.FAILED),
    ],
)
async def test_any_failed_outcome_fails_the_tenant(
    make_tenant_busy: str,
    expected: TenantStatus,
    tenant_service: TenantService,
    update_service: TaskUpdateService,
    tasks: TaskQueryService,
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    task_id = op.task.id
    if make_tenant_busy != "deploy":
        await update_service.apply(update_msg(task_id, S.DONE))
        if make_tenant_busy == "update":
            task_id = (
                await tenant_service.update(op.tenant.id, expected_version=2, changes={"name": "B"})
            ).task.id
        else:
            task_id = (await tenant_service.delete(op.tenant.id)).task.id

    await update_service.apply(update_msg(task_id, S.FAILED))
    assert (await tenant_service.get(op.tenant.id)).status is expected
    task = await tasks.get(task_id)
    assert task.status is S.FAILED


async def test_failed_tenant_can_be_destroyed(
    tenant_service: TenantService, update_service: TaskUpdateService
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    await update_service.apply(update_msg(op.task.id, S.FAILED))
    destroy = await tenant_service.delete(op.tenant.id)
    await update_service.apply(update_msg(destroy.task.id, S.DONE))
    assert (await tenant_service.get(op.tenant.id)).status is TenantStatus.DESTROYED


async def test_duplicate_message_changes_nothing(
    tenant_service: TenantService, update_service: TaskUpdateService
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    msg = update_msg(op.task.id, S.DONE, message_id="same-id")
    assert await update_service.apply(msg) is UpdateOutcome.APPLIED
    before = await tenant_service.get(op.tenant.id)
    for _ in range(3):
        assert await update_service.apply(msg) is UpdateOutcome.DUPLICATE
    after = await tenant_service.get(op.tenant.id)
    assert (after.version, after.updated_at) == (before.version, before.updated_at)


async def test_stale_in_progress_after_done_is_ignored(
    tenant_service: TenantService, update_service: TaskUpdateService, tasks: TaskQueryService
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    # Out of order: done overtakes in_progress.
    assert await update_service.apply(update_msg(op.task.id, S.DONE)) is UpdateOutcome.APPLIED
    assert await update_service.apply(update_msg(op.task.id, S.IN_PROGRESS)) is UpdateOutcome.STALE
    assert (await tasks.get(op.task.id)).status is S.DONE
    t = await tenant_service.get(op.tenant.id)
    assert (t.status, t.version) == (TenantStatus.ACTIVE, 2)


async def test_terminal_task_is_never_changed(
    tenant_service: TenantService, update_service: TaskUpdateService, tasks: TaskQueryService
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    await update_service.apply(update_msg(op.task.id, S.FAILED))
    assert await update_service.apply(update_msg(op.task.id, S.DONE)) is UpdateOutcome.STALE
    assert (await tasks.get(op.task.id)).status is S.FAILED
    assert (await tenant_service.get(op.tenant.id)).status is TenantStatus.FAILED


async def test_stale_update_for_old_task_does_not_touch_tenants_new_task(
    tenant_service: TenantService, update_service: TaskUpdateService
) -> None:
    """A very late `done` for the old deploy task must not affect the tenant while
    its *update* task is running."""
    op = await tenant_service.create(slug="acme", name="Acme")
    await update_service.apply(update_msg(op.task.id, S.DONE))
    await tenant_service.update(op.tenant.id, expected_version=2, changes={"name": "B"})

    late = update_msg(op.task.id, S.FAILED)
    assert await update_service.apply(late) is UpdateOutcome.STALE
    assert (await tenant_service.get(op.tenant.id)).status is TenantStatus.UPDATING


async def test_unknown_task_raises_poison_and_records_nothing(
    update_service: TaskUpdateService, session_factory: SessionFactory
) -> None:
    with pytest.raises(UnknownTaskInUpdate):
        await update_service.apply(update_msg(uuid.uuid4(), S.DONE))
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM processed_messages"))).scalar_one() == 0
