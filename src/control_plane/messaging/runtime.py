"""Owns the broker connection and the control plane's two background loops:
the outbox relay (outbound) and the task-update consumer (inbound).
Started and stopped by the FastAPI lifespan in main.py.
"""

import asyncio
import contextlib
import logging

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from control_plane.config import Settings
from control_plane.db.session import SessionFactory
from control_plane.messaging import contracts as c
from control_plane.messaging.consumer import TaskUpdateConsumer
from control_plane.messaging.outbox_relay import OutboxRelay, RabbitEventPublisher
from control_plane.messaging.rabbitmq import (
    ConfirmedPublisher,
    connect_with_backoff,
    declare_topology,
)
from control_plane.services.task_update_service import TaskUpdateService

log = logging.getLogger(__name__)

# Max unacked updates held at once: back-pressure so we don't outrun the DB pool.
CONSUMER_PREFETCH = 20


class MessagingRuntime:
    def __init__(self, settings: Settings, session_factory: SessionFactory) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._stop = asyncio.Event()
        self._connection: AbstractRobustConnection | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._relay_task: asyncio.Task[None] | None = None

    @property
    def is_connected(self) -> bool:
        # `is_closed` stays False while a robust connection is reconnecting;
        # the `connected` event says whether the transport is actually up.
        conn = self._connection
        return conn is not None and not conn.is_closed and conn.connected.is_set()

    async def start(self) -> None:
        # Connect in the background: the API serves (and /readyz says "not ready")
        # while the broker boots. Writes still work; the outbox drains later.
        self._connect_task = asyncio.create_task(self._connect_and_run(), name="messaging")

    async def _connect_and_run(self) -> None:
        self._connection = await connect_with_backoff(self._settings.rabbitmq_url)
        await declare_topology(await self._connection.channel())

        publisher = await ConfirmedPublisher.create(self._connection, c.TASKS_EXCHANGE)
        relay = OutboxRelay(
            self._session_factory,
            RabbitEventPublisher(publisher),
            poll_interval_s=self._settings.outbox_poll_interval_s,
        )
        self._relay_task = asyncio.create_task(relay.run(self._stop), name="outbox-relay")

        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=CONSUMER_PREFETCH)
        queue = await channel.get_queue(c.TASK_UPDATES_QUEUE)
        consumer = TaskUpdateConsumer(TaskUpdateService(self._session_factory))

        async def on_message(message: AbstractIncomingMessage) -> None:
            await consumer.handle(message)

        await queue.consume(on_message)
        log.info("consuming task updates")

    async def stop(self) -> None:
        self._stop.set()
        if self._connect_task is not None and not self._connect_task.done():
            self._connect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._connect_task
        if self._relay_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(self._relay_task, timeout=5)
        if self._connection is not None:
            await self._connection.close()
        log.info("messaging stopped")
