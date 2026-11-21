"""Inbound consumer: worker progress updates -> TaskUpdateService.

This module only decides what to tell the broker (ack / dead-letter / requeue);
the business rules live in TaskUpdateService. We ack only after the DB commit.

    applied / duplicate / stale       -> ack            handled (duplicate & stale are no-ops)
    invalid JSON / schema violation   -> reject -> DLQ  poison: will never succeed
    references an unknown task        -> reject -> DLQ  poison: will never succeed
    transient error (DB blip)         -> retry inline with backoff, then requeue
    unexpected exception (a bug)      -> requeue; after DELIVERY_LIMIT attempts the
                                         broker dead-letters it

So a poison message never kills the consumer and never blocks the queue.
"""

import asyncio
import logging
from typing import Protocol

from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from control_plane.domain.errors import UnknownTaskInUpdate
from control_plane.messaging.contracts import TaskUpdateMessage
from control_plane.messaging.rabbitmq import backoff_delay
from control_plane.services.task_update_service import TaskUpdateService, UpdateOutcome

log = logging.getLogger(__name__)

MAX_INLINE_RETRIES = 3
_RETRYABLE_SQLSTATES = {"40001", "40P01"}  # serialization failure, deadlock


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, OperationalError | InterfaceError | ConnectionError | TimeoutError):
        return True
    if isinstance(exc, DBAPIError):
        if exc.connection_invalidated:
            return True
        return getattr(exc.orig, "sqlstate", None) in _RETRYABLE_SQLSTATES
    return False


class IncomingMessage(Protocol):
    """The part of aio_pika's IncomingMessage we use (so tests can pass a fake)."""

    body: bytes
    message_id: str | None

    async def ack(self) -> None: ...
    async def reject(self, requeue: bool = False) -> None: ...
    async def nack(self, multiple: bool = False, requeue: bool = True) -> None: ...


class TaskUpdateConsumer:
    def __init__(self, service: TaskUpdateService, *, retry_base_delay_s: float = 0.2) -> None:
        self._service = service
        self._retry_base_delay_s = retry_base_delay_s

    async def handle(self, message: IncomingMessage) -> None:
        try:
            update = TaskUpdateMessage.model_validate_json(message.body)
        except ValidationError as exc:
            # Bad input, not a bug: log why, without a stack trace.
            reasons = "; ".join(
                f"{'.'.join(map(str, e['loc'])) or 'body'}: {e['msg']}" for e in exc.errors()
            )
            log.error(
                "poison message, dead-lettering: %s",
                reasons[:500],
                extra={"message_id": message.message_id},
            )
            await message.reject(requeue=False)
            return

        ctx = {"message_id": update.message_id, "task_id": update.task_id}
        try:
            outcome = await self._apply_with_retry(update)
        except UnknownTaskInUpdate:
            log.error("poison message (unknown task), dead-lettering", extra=ctx)
            await message.reject(requeue=False)
            return
        except Exception:
            log.exception("task update failed; returning it to the broker", extra=ctx)
            await message.nack(requeue=True)
            return

        await message.ack()
        log.debug("task update acked", extra={**ctx, "outcome": outcome})

    async def _apply_with_retry(self, update: TaskUpdateMessage) -> UpdateOutcome:
        """Retry transient DB errors a few times in-process, with backoff, so a short
        DB blip doesn't burn through the broker's delivery limit."""
        attempt = 0
        while True:
            try:
                return await self._service.apply(update)
            except Exception as exc:
                if not is_transient(exc) or attempt >= MAX_INLINE_RETRIES:
                    raise
                delay = backoff_delay(attempt, base=self._retry_base_delay_s, cap=5.0)
                log.warning(
                    "transient error, retrying in %.2fs",
                    delay,
                    extra={"message_id": update.message_id, "attempt": attempt},
                )
                attempt += 1
                await asyncio.sleep(delay)
