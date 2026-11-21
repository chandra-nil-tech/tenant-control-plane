"""RabbitMQ plumbing shared by the control plane and the worker simulator:
connect with backoff, declare the topology, publish with confirms.
"""

import asyncio
import logging
import random
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

from control_plane.messaging import contracts as c

log = logging.getLogger(__name__)


def backoff_delay(attempt: int, *, base: float = 0.5, cap: float = 30.0) -> float:
    """Exponential backoff with full jitter: the randomness stops many clients
    from retrying in lock-step the moment the broker comes back."""
    # Jitter, not cryptography: the stdlib PRNG is the right tool.
    return random.uniform(0, min(cap, base * (2**attempt)))  # noqa: S311  # nosec B311


async def connect_with_backoff(url: str) -> AbstractRobustConnection:
    """Connect, retrying forever with backoff (the broker may still be booting).

    Once connected, `connect_robust` reconnects by itself after a broker restart or
    network blip and restores channels and consumers, so we need no reconnect loop.
    """
    attempt = 0
    while True:
        try:
            connection = await aio_pika.connect_robust(url)
            log.info("connected to broker")
            return connection
        except (OSError, aio_pika.exceptions.AMQPConnectionError) as exc:
            delay = backoff_delay(attempt)
            log.warning(
                "broker connection failed, retrying in %.2fs: %s",
                delay,
                exc,
                extra={"attempt": attempt},
            )
            attempt += 1
            await asyncio.sleep(delay)


def _quorum_args(queue: str) -> dict[str, Any]:
    return {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": c.DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": c.dlq_name(queue),
        "x-delivery-limit": c.DELIVERY_LIMIT,
    }


async def declare_topology(channel: AbstractChannel) -> dict[str, AbstractQueue]:
    """Idempotently declare all exchanges, queues and bindings.

    Both processes call this at startup with identical arguments, so either can
    start first.
    """
    dlx = await channel.declare_exchange(
        c.DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    tasks_x = await channel.declare_exchange(
        c.TASKS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    updates_x = await channel.declare_exchange(
        c.TASK_UPDATES_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )

    queues: dict[str, AbstractQueue] = {}
    for queue_name, exchange, pattern in (
        (c.WORKER_TASKS_QUEUE, tasks_x, "task.*"),
        (c.TASK_UPDATES_QUEUE, updates_x, "task_update.*"),
    ):
        queue = await channel.declare_queue(
            queue_name, durable=True, arguments=_quorum_args(queue_name)
        )
        await queue.bind(exchange, routing_key=pattern)
        dlq = await channel.declare_queue(
            c.dlq_name(queue_name), durable=True, arguments={"x-queue-type": "quorum"}
        )
        await dlq.bind(dlx, routing_key=c.dlq_name(queue_name))
        queues[queue_name] = queue
        queues[c.dlq_name(queue_name)] = dlq
    return queues


class ConfirmedPublisher:
    """Publishes persistent messages and waits for the broker's publisher confirm.

    Without confirms, a returned `publish()` only means "written to the socket".
    With confirms + `mandatory=True` + `on_return_raises=True`, success means the
    broker routed the message to a queue and stored it. Only then is an outbox row
    marked as published.
    """

    def __init__(self, exchange: AbstractExchange) -> None:
        self._exchange = exchange

    @classmethod
    async def create(
        cls, connection: AbstractRobustConnection, exchange_name: str
    ) -> "ConfirmedPublisher":
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        return cls(await channel.get_exchange(exchange_name, ensure=True))

    async def publish_json(self, *, body: bytes, routing_key: str, message_id: str) -> None:
        message = aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
        )
        await self._exchange.publish(message, routing_key=routing_key, mandatory=True, timeout=10)
