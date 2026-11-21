"""End-to-end through a REAL RabbitMQ: API service -> outbox -> relay -> broker ->
worker simulator -> broker -> consumer -> DB. Everything runs in this process so
the test can flip the worker's chaos flags and inspect the database directly.

Requires RabbitMQ (CP_TEST_RABBITMQ_URL); `make test` provides it via compose.
"""

import asyncio
import contextlib
import dataclasses
import json
import os
import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest
from sqlalchemy import select

from control_plane.config import Settings
from control_plane.db.session import SessionFactory
from control_plane.db.tables import ProcessedMessageRow, TaskRow, TenantRow
from control_plane.domain.models import TaskStatus, TenantStatus
from control_plane.messaging import contracts as c
from control_plane.messaging.rabbitmq import declare_topology
from control_plane.messaging.runtime import MessagingRuntime
from control_plane.services.tenant_service import TenantService
from worker_sim.simulator import WorkerConfig
from worker_sim.simulator import run as run_worker

pytestmark = pytest.mark.integration

BROKER_URL = os.environ.get("CP_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


@pytest.fixture
async def broker() -> AsyncIterator[aio_pika.abc.AbstractRobustConnection]:
    try:
        conn = await asyncio.wait_for(aio_pika.connect_robust(BROKER_URL), timeout=3)
    except Exception as exc:  # pragma: no cover - only when no broker is around
        if os.environ.get("CP_REQUIRE_BROKER") == "1":
            raise
        pytest.skip(f"RabbitMQ not reachable at {BROKER_URL}: {exc}")
    channel = await conn.channel()
    queues = await declare_topology(channel)
    for name in (c.WORKER_TASKS_QUEUE, c.TASK_UPDATES_QUEUE, c.dlq_name(c.TASK_UPDATES_QUEUE)):
        await queues[name].purge()  # type: ignore[union-attr]
    yield conn
    await conn.close()


@contextlib.asynccontextmanager
async def stack(
    session_factory: SessionFactory, db_url: str, **worker_flags: object
) -> AsyncIterator[TenantService]:
    settings = Settings(database_url=db_url, rabbitmq_url=BROKER_URL, outbox_poll_interval_s=0.2)
    runtime = MessagingRuntime(settings, session_factory)
    await runtime.start()
    stop = asyncio.Event()
    config = WorkerConfig(
        rabbitmq_url=BROKER_URL,
        fail_rate=0.0,
        min_delay_ms=5,
        max_delay_ms=20,
        duplicate_rate=0.0,
        out_of_order=False,
    )
    config = dataclasses.replace(config, **worker_flags)  # type: ignore[arg-type]
    worker = asyncio.create_task(run_worker(config, stop))
    try:
        yield TenantService(session_factory)
    finally:
        stop.set()
        await asyncio.wait_for(worker, timeout=5)
        await runtime.stop()


async def wait_for_status(
    session_factory: SessionFactory,
    tenant_id: uuid.UUID,
    status: TenantStatus,
    within_s: float = 15,
) -> TenantRow:
    deadline = asyncio.get_running_loop().time() + within_s
    while True:
        async with session_factory() as s:
            tenant = await s.get(TenantRow, tenant_id)
        assert tenant is not None
        if tenant.status is status:
            return tenant
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"tenant stuck in {tenant.status}, wanted {status}")
        await asyncio.sleep(0.05)


async def test_full_lifecycle_through_broker(
    broker: object, session_factory: SessionFactory, migrated_db: str
) -> None:
    async with stack(session_factory, migrated_db) as service:
        op = await service.create(slug="e2e-acme", name="Acme")
        tenant = await wait_for_status(session_factory, op.tenant.id, TenantStatus.ACTIVE)
        assert tenant.version == 2

        upd = await service.update(op.tenant.id, expected_version=2, changes={"name": "Acme 2"})
        await wait_for_status(session_factory, op.tenant.id, TenantStatus.ACTIVE)
        await service.delete(op.tenant.id)
        tenant = await wait_for_status(session_factory, op.tenant.id, TenantStatus.DESTROYED)
        # 1 (create) +1 deploy done +1 PATCH +1 update done +1 DELETE +1 destroy done
        assert tenant.version == 6

    async with session_factory() as s:
        statuses = (
            await s.execute(select(TaskRow.type, TaskRow.status).order_by(TaskRow.created_at))
        ).all()
    assert [(t.value, st.value) for t, st in statuses] == [
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    ]
    assert upd.task.type == "update"


async def test_failures_reach_failed_state(
    broker: object, session_factory: SessionFactory, migrated_db: str
) -> None:
    async with stack(session_factory, migrated_db, fail_rate=1.0) as service:
        op = await service.create(slug="e2e-fail", name="Doomed")
        await wait_for_status(session_factory, op.tenant.id, TenantStatus.FAILED)
        await service.delete(op.tenant.id)  # failed -> destroying is allowed ...
        await wait_for_status(
            session_factory, op.tenant.id, TenantStatus.FAILED
        )  # ... and fails again
    async with session_factory() as s:
        statuses = (await s.execute(select(TaskRow.status))).scalars().all()
    assert statuses == [TaskStatus.FAILED, TaskStatus.FAILED]


async def test_duplicates_and_reordering_are_harmless(
    broker: object, session_factory: SessionFactory, migrated_db: str
) -> None:
    """Worker sends every update twice AND sends the terminal update before in_progress."""
    async with stack(
        session_factory, migrated_db, duplicate_rate=1.0, out_of_order=True
    ) as service:
        ops = [await service.create(slug=f"e2e-chaos-{i}", name="n") for i in range(5)]
        for op in ops:
            tenant = await wait_for_status(session_factory, op.tenant.id, TenantStatus.ACTIVE)
            assert tenant.version == 2  # applied exactly once despite 4 deliveries
        await asyncio.sleep(0.5)  # let the late in_progress copies arrive too

    async with session_factory() as s:
        tasks = (await s.execute(select(TaskRow))).scalars().all()
        ledger = (await s.execute(select(ProcessedMessageRow))).scalars().all()
    assert {t.status for t in tasks} == {TaskStatus.DONE}  # late in_progress did not regress
    # 2 distinct message ids per task (done, in_progress), each recorded once despite 4 copies
    assert len(ledger) == 10


async def test_poison_message_is_dead_lettered_and_consumer_survives(
    broker: aio_pika.abc.AbstractRobustConnection, session_factory: SessionFactory, migrated_db: str
) -> None:
    async with stack(session_factory, migrated_db) as service:
        channel = await broker.channel()
        exchange = await channel.get_exchange(c.TASK_UPDATES_EXCHANGE)
        for body in (b"not json at all", json.dumps({"task_id": str(uuid.uuid4())}).encode()):
            await exchange.publish(aio_pika.Message(body=body), routing_key="task_update.done")
        unknown = {
            "message_id": "poison-unknown-task",
            "task_id": str(uuid.uuid4()),
            "status": "done",
        }
        await exchange.publish(
            aio_pika.Message(body=json.dumps(unknown).encode()), routing_key="task_update.done"
        )

        # All three end up in the DLQ (read them back: message counts on quorum
        # queues are reported with a lag, actual messages are not).
        dlq = await channel.get_queue(c.dlq_name(c.TASK_UPDATES_QUEUE))
        dead: list[bytes] = []
        for _ in range(200):
            msg = await dlq.get(no_ack=True, fail=False)
            if msg is not None:
                dead.append(msg.body)
            if len(dead) == 3:
                break
            if msg is None:
                await asyncio.sleep(0.05)
        assert b"not json at all" in dead
        assert len(dead) == 3

        # The consumer is still alive and processing normal traffic.
        op = await service.create(slug="e2e-after-poison", name="n")
        await wait_for_status(session_factory, op.tenant.id, TenantStatus.ACTIVE)
