"""Outbox relay: publishes committed events from the `outbox` table to the broker.

Why an outbox (the "dual write" problem):
    commit DB, then publish  -> crash in between = state changed, event LOST
    publish, then commit DB  -> commit fails      = event for a change that never happened
Neither order is safe across two systems. So the API writes the event into the
database in the *same* transaction as the state change, and this relay only ever
sees committed rows: it cannot publish before the commit, or after a rollback.

Delivery is at-least-once: a crash after the broker confirms but before we mark the
row resends it after restart. Every event's message_id is the task id, so
consumers dedupe. (Exactly-once delivery doesn't exist; exactly-once *effect* =
at-least-once delivery + idempotent consumers.)
"""

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Protocol

from control_plane.db.session import SessionFactory
from control_plane.messaging.rabbitmq import ConfirmedPublisher, backoff_delay
from control_plane.repositories.outbox import OutboundEvent, OutboxRepository

log = logging.getLogger(__name__)

BATCH_SIZE = 100


class EventPublisher(Protocol):
    async def publish(self, event: OutboundEvent) -> None: ...


class RabbitEventPublisher:
    """Adapter: OutboundEvent -> AMQP message."""

    def __init__(self, publisher: ConfirmedPublisher) -> None:
        self._publisher = publisher

    async def publish(self, event: OutboundEvent) -> None:
        await self._publisher.publish_json(
            body=json.dumps(event.payload).encode(),
            routing_key=event.routing_key,
            message_id=str(event.event_id),
        )


@dataclass(frozen=True, slots=True)
class RelayResult:
    published: int
    failed: bool  # a publish failed; the remaining rows wait for the next attempt


class OutboxRelay:
    def __init__(
        self,
        session_factory: SessionFactory,
        publisher: EventPublisher,
        *,
        poll_interval_s: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._poll_interval_s = poll_interval_s

    async def publish_pending(self) -> RelayResult:
        """Publish one batch of committed, unpublished events, oldest first.

        Holds row locks (SKIP LOCKED) on the batch, so concurrent relays never send
        the same row at the same time. Stops at the first failure (the broker is
        probably down); rows already confirmed in this batch are still committed
        as published.
        """
        published = 0
        async with self._session_factory() as session, session.begin():
            repo = OutboxRepository(session)
            for row in await repo.claim_batch(BATCH_SIZE):
                event = OutboundEvent(
                    event_id=row.event_id, routing_key=row.routing_key, payload=row.payload
                )
                try:
                    await self._publisher.publish(event)
                except Exception as exc:
                    log.warning(
                        "publish failed; will retry", extra={"task_id": row.event_id}, exc_info=exc
                    )
                    return RelayResult(published, failed=True)
                await repo.mark_published(row.id)
                published += 1
                log.info("event published", extra={"task_id": row.event_id})
        return RelayResult(published, failed=False)

    async def run(self, stop: asyncio.Event) -> None:
        """Poll until `stop` is set, backing off while publishing keeps failing."""
        failures = 0
        log.info("outbox relay started")
        while not stop.is_set():
            try:
                result = await self.publish_pending()
            except Exception:
                log.exception("outbox relay iteration failed")  # e.g. DB unavailable
                result = RelayResult(0, failed=True)

            failures = failures + 1 if result.failed else 0
            if result.published >= BATCH_SIZE:
                continue  # probably more waiting
            delay = backoff_delay(failures) if failures else self._poll_interval_s
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
        log.info("outbox relay stopped")
