"""Ack / reject / nack policy of the inbound consumer, with a fake message and service.

No broker and no DB: we only test the decision of what to tell the broker."""

import uuid
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from control_plane.domain.errors import UnknownTaskInUpdate
from control_plane.messaging.consumer import TaskUpdateConsumer, is_transient
from control_plane.services.task_update_service import UpdateOutcome


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.message_id: str | None = "m-1"
        self.calls: list[tuple[str, Any]] = []

    async def ack(self) -> None:
        self.calls.append(("ack", None))

    async def reject(self, requeue: bool = False) -> None:
        self.calls.append(("reject", requeue))

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.calls.append(("nack", requeue))


class FakeService:
    """Raises the queued exceptions in order, then returns APPLIED."""

    def __init__(self, *errors: BaseException) -> None:
        self.errors = list(errors)
        self.calls = 0

    async def apply(self, update: Any) -> UpdateOutcome:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return UpdateOutcome.APPLIED


def _valid_body() -> bytes:
    return b'{"message_id":"m-1","task_id":"%s","status":"done"}' % str(uuid.uuid4()).encode()


def _transient() -> OperationalError:
    return OperationalError("SELECT 1", {}, ConnectionError("db went away"))


def _consumer(service: FakeService) -> TaskUpdateConsumer:
    return TaskUpdateConsumer(service, retry_base_delay_s=0)  # type: ignore[arg-type]


async def test_success_is_acked() -> None:
    msg = FakeMessage(_valid_body())
    await _consumer(FakeService()).handle(msg)
    assert msg.calls == [("ack", None)]


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"{}",
        b'{"message_id":"x","task_id":"nope","status":"done"}',
        b"\xff\xfe",
    ],
)
async def test_poison_is_dead_lettered_without_calling_service(body: bytes) -> None:
    service = FakeService()
    msg = FakeMessage(body)
    await _consumer(service).handle(msg)
    assert msg.calls == [("reject", False)]
    assert service.calls == 0


async def test_unknown_task_is_dead_lettered() -> None:
    msg = FakeMessage(_valid_body())
    await _consumer(FakeService(UnknownTaskInUpdate("nope"))).handle(msg)
    assert msg.calls == [("reject", False)]


async def test_transient_error_is_retried_inline_then_succeeds() -> None:
    service = FakeService(_transient(), _transient())
    msg = FakeMessage(_valid_body())
    await _consumer(service).handle(msg)
    assert service.calls == 3
    assert msg.calls == [("ack", None)]


async def test_persistent_transient_error_goes_back_to_broker() -> None:
    service = FakeService(*(_transient() for _ in range(5)))
    msg = FakeMessage(_valid_body())
    await _consumer(service).handle(msg)
    assert service.calls == 4  # 1 try + 3 retries
    assert msg.calls == [("nack", True)]


async def test_unexpected_error_is_requeued_not_retried_inline() -> None:
    service = FakeService(RuntimeError("bug"))
    msg = FakeMessage(_valid_body())
    await _consumer(service).handle(msg)
    assert service.calls == 1
    assert msg.calls == [("nack", True)]  # broker's delivery limit will DLQ it


def test_is_transient_classification() -> None:
    assert is_transient(_transient())
    assert is_transient(ConnectionError())
    assert is_transient(TimeoutError())
    assert not is_transient(ValueError())
    assert not is_transient(UnknownTaskInUpdate("x"))
