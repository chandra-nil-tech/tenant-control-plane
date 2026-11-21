"""Outbound messaging: the event is written atomically with the change, published
only after commit, retried on broker failure, never double-sent by concurrent relays."""

import asyncio
import uuid

import pytest
from sqlalchemy import select

from control_plane.db.session import SessionFactory
from control_plane.db.tables import OutboxRow
from control_plane.domain.errors import TenantAlreadyExists
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.outbox_relay import OutboxRelay
from control_plane.repositories.outbox import OutboundEvent
from control_plane.services.tenant_service import TenantService

pytestmark = pytest.mark.integration


class RecordingPublisher:
    def __init__(self, *, fail_times: int = 0, delay_s: float = 0.0) -> None:
        self.sent: list[OutboundEvent] = []
        self.fail_times = fail_times
        self.delay_s = delay_s

    async def publish(self, event: OutboundEvent) -> None:
        await asyncio.sleep(self.delay_s)
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("broker unavailable")
        self.sent.append(event)


async def _outbox(session_factory: SessionFactory) -> list[OutboxRow]:
    async with session_factory() as s:
        return list((await s.execute(select(OutboxRow).order_by(OutboxRow.id))).scalars())


async def test_create_writes_exactly_one_event_that_carries_the_task(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    op = await tenant_service.create(slug="acme", name="Acme")
    rows = await _outbox(session_factory)
    assert len(rows) == 1
    event = TaskEvent.model_validate(rows[0].payload)
    # A consumer can recover the task's id, type, tenant_id and status from the event.
    assert (event.id, event.type, event.tenant_id, event.status) == (
        op.task.id,
        op.task.type,
        op.tenant.id,
        op.task.status,
    )
    assert rows[0].event_id == op.task.id
    assert rows[0].routing_key == "task.deploy"
    assert rows[0].published_at is None  # the relay publishes it, after commit


async def test_rejected_operation_writes_no_event(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    await tenant_service.create(slug="acme", name="Acme")
    with pytest.raises(TenantAlreadyExists):
        await tenant_service.create(slug="acme", name="Again")
    assert len(await _outbox(session_factory)) == 1


async def test_uncommitted_event_is_invisible_to_relay(session_factory: SessionFactory) -> None:
    """A transaction that wrote its outbox row but has not committed (and then rolls back)."""
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher)

    async with session_factory() as s:
        await s.begin()
        s.add(OutboxRow(event_id=uuid.uuid4(), routing_key="task.deploy", payload={}))
        await s.flush()  # INSERT sent, transaction still open
        assert (await relay.publish_pending()).published == 0
        await s.rollback()

    assert (await relay.publish_pending()).published == 0
    assert publisher.sent == []


async def test_relay_publishes_in_order_and_marks(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    ops = [await tenant_service.create(slug=f"t-{i}x", name="n") for i in range(3)]
    publisher = RecordingPublisher()
    relay = OutboxRelay(session_factory, publisher)

    assert (await relay.publish_pending()).published == 3
    assert [e.event_id for e in publisher.sent] == [op.task.id for op in ops]
    assert all(r.published_at is not None for r in await _outbox(session_factory))
    assert (await relay.publish_pending()).published == 0  # nothing left


async def test_relay_keeps_event_pending_when_broker_fails(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    await tenant_service.create(slug="acme", name="Acme")
    publisher = RecordingPublisher(fail_times=2)
    relay = OutboxRelay(session_factory, publisher)

    for _ in range(2):
        result = await relay.publish_pending()
        assert result.failed and result.published == 0
        assert (await _outbox(session_factory))[0].published_at is None

    assert (await relay.publish_pending()).published == 1
    assert (await _outbox(session_factory))[0].published_at is not None
    assert len(publisher.sent) == 1


async def test_concurrent_relays_never_double_publish(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    """Several relays (one per API replica) over the same outbox: SKIP LOCKED gives
    each a disjoint set of rows."""
    for i in range(20):
        await tenant_service.create(slug=f"tenant-{i:02d}", name="n")
    publisher = RecordingPublisher(delay_s=0.005)
    relays = [OutboxRelay(session_factory, publisher) for _ in range(4)]

    await asyncio.gather(*(r.publish_pending() for r in relays))

    ids = [e.event_id for e in publisher.sent]
    assert len(ids) == 20
    assert len(set(ids)) == 20


async def test_relay_loop_backs_off_and_recovers(
    tenant_service: TenantService, session_factory: SessionFactory
) -> None:
    await tenant_service.create(slug="acme", name="Acme")
    publisher = RecordingPublisher(fail_times=1)
    relay = OutboxRelay(session_factory, publisher, poll_interval_s=0.05)
    stop = asyncio.Event()
    loop_task = asyncio.create_task(relay.run(stop))
    for _ in range(300):
        if publisher.sent:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(loop_task, timeout=2)
    assert len(publisher.sent) == 1
