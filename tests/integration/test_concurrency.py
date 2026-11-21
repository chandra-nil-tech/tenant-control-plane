"""Race conditions, provoked for real: many coroutines, separate DB connections,
fired at the same instant with asyncio.gather. Each test asserts "exactly one winner"."""

import asyncio
import uuid

import pytest
from sqlalchemy import func, select

from control_plane.db.session import SessionFactory
from control_plane.db.tables import OutboxRow, ProcessedMessageRow, TaskRow
from control_plane.domain.errors import (
    DomainError,
    TenantAlreadyExists,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.domain.models import TaskStatus
from control_plane.services.task_update_service import TaskUpdateService, UpdateOutcome
from control_plane.services.tenant_service import TenantService
from tests.helpers import update_msg

pytestmark = pytest.mark.integration

N = 25


async def _count(session_factory: SessionFactory, model: type, *where: object) -> int:
    async with session_factory() as s:
        return int(
            (await s.execute(select(func.count()).select_from(model).where(*where))).scalar_one()
        )  # type: ignore[arg-type]


async def _active_tenant(
    svc: TenantService, upd: TaskUpdateService, slug: str = "acme"
) -> uuid.UUID:
    op = await svc.create(slug=slug, name="Acme")
    await upd.apply(update_msg(op.task.id, TaskStatus.DONE))
    return op.tenant.id


async def test_concurrent_creates_same_slug_exactly_one_wins(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    results = await asyncio.gather(
        *(tenant_service.create(slug="same-slug", name=f"n{i}") for i in range(N)),
        return_exceptions=True,
    )
    winners = [r for r in results if not isinstance(r, BaseException)]
    losers = [r for r in results if isinstance(r, BaseException)]
    assert len(winners) == 1
    assert len(losers) == N - 1
    assert all(isinstance(e, TenantAlreadyExists) for e in losers)
    # Losers rolled back: exactly one task and one event exist.
    assert await _count(session_factory, TaskRow) == 1
    assert await _count(session_factory, OutboxRow) == 1


async def test_concurrent_patches_same_version_exactly_one_wins(
    tenant_service: TenantService,
    update_service: TaskUpdateService,
    session_factory: SessionFactory,
) -> None:
    tenant_id = await _active_tenant(tenant_service, update_service)  # version 2

    results = await asyncio.gather(
        *(
            tenant_service.update(tenant_id, expected_version=2, changes={"name": f"n{i}"})
            for i in range(N)
        ),
        return_exceptions=True,
    )
    winners = [r for r in results if not isinstance(r, BaseException)]
    losers = [r for r in results if isinstance(r, BaseException)]
    assert len(winners) == 1
    # A loser sees either the bumped version or (more often) the `updating` status.
    assert all(isinstance(e, TenantVersionConflict | TenantUpdateNotAllowed) for e in losers)

    tenant = await tenant_service.get(tenant_id)
    assert tenant.version == 3
    assert tenant.name == winners[0].tenant.name
    # One deploy + exactly one update task, and one event each.
    assert await _count(session_factory, TaskRow) == 2
    assert await _count(session_factory, OutboxRow) == 2


async def test_concurrent_deletes_exactly_one_wins(
    tenant_service: TenantService,
    update_service: TaskUpdateService,
    session_factory: SessionFactory,
) -> None:
    tenant_id = await _active_tenant(tenant_service, update_service)
    results = await asyncio.gather(
        *(tenant_service.delete(tenant_id) for _ in range(N)), return_exceptions=True
    )
    assert sum(not isinstance(r, BaseException) for r in results) == 1
    assert all(
        isinstance(r, TenantUpdateNotAllowed) for r in results if isinstance(r, BaseException)
    )
    assert await _count(session_factory, OutboxRow) == 2


async def test_concurrent_patch_and_delete_never_create_two_open_tasks(
    tenant_service: TenantService,
    update_service: TaskUpdateService,
    session_factory: SessionFactory,
) -> None:
    tenant_id = await _active_tenant(tenant_service, update_service)
    calls = [
        tenant_service.update(tenant_id, expected_version=2, changes={"name": "x"})
        for _ in range(N)
    ]
    calls += [tenant_service.delete(tenant_id) for _ in range(N)]
    results = await asyncio.gather(*calls, return_exceptions=True)
    assert sum(not isinstance(r, BaseException) for r in results) == 1
    assert all(isinstance(r, DomainError) for r in results if isinstance(r, BaseException))
    open_tasks = await _count(
        session_factory, TaskRow, TaskRow.status.in_([TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS])
    )
    assert open_tasks == 1


async def test_same_update_delivered_concurrently_is_applied_once(
    tenant_service: TenantService,
    update_service: TaskUpdateService,
    session_factory: SessionFactory,
) -> None:
    """At-least-once delivery + several consumer replicas: one message processed in parallel."""
    op = await tenant_service.create(slug="acme", name="Acme")
    msg = update_msg(op.task.id, TaskStatus.DONE, message_id="dup-1")

    outcomes = await asyncio.gather(*(update_service.apply(msg) for _ in range(N)))
    assert outcomes.count(UpdateOutcome.APPLIED) == 1
    assert outcomes.count(UpdateOutcome.DUPLICATE) == N - 1
    tenant = await tenant_service.get(op.tenant.id)
    assert (tenant.status, tenant.version) == ("active", 2)  # version bumped exactly once
    assert await _count(session_factory, ProcessedMessageRow) == 1


async def test_racing_terminal_outcomes_first_one_wins(
    tenant_service: TenantService, update_service: TaskUpdateService
) -> None:
    """`done` and `failed` for the same task arriving together (e.g. a redelivered
    task processed twice by the worker): exactly one is applied, the rest are stale."""
    op = await tenant_service.create(slug="acme", name="Acme")
    msgs = [
        update_msg(op.task.id, TaskStatus.DONE if i % 2 else TaskStatus.FAILED) for i in range(N)
    ]
    outcomes = await asyncio.gather(*(update_service.apply(m) for m in msgs))
    assert outcomes.count(UpdateOutcome.APPLIED) == 1
    assert outcomes.count(UpdateOutcome.STALE) == N - 1
    tenant = await tenant_service.get(op.tenant.id)
    assert tenant.status in ("active", "failed")
    assert tenant.version == 2
