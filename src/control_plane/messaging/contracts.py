"""Wire contracts between the control plane and the worker (both sides import this).

Topology (RabbitMQ):

    control plane --(tenant.tasks, topic, "task.<type>")--> [worker.tasks] --> worker
    worker --(tenant.task-updates, topic, "task_update.<status>")--> [control-plane.task-updates]
                                                                       --> control plane consumer
    poison / over-redelivered messages --(tenant.dlx, direct)--> [<queue>.dlq]

All queues are quorum queues: durable, and they count redeliveries, so the broker
dead-letters a message that keeps failing (`x-delivery-limit`) instead of looping on it.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, PlainSerializer

from control_plane.domain.models import TaskStatus, TaskType

TASKS_EXCHANGE = "tenant.tasks"
TASK_UPDATES_EXCHANGE = "tenant.task-updates"
DEAD_LETTER_EXCHANGE = "tenant.dlx"

WORKER_TASKS_QUEUE = "worker.tasks"
TASK_UPDATES_QUEUE = "control-plane.task-updates"

# Redeliveries before the broker dead-letters a message.
DELIVERY_LIMIT = 5


def dlq_name(queue: str) -> str:
    return f"{queue}.dlq"


def task_routing_key(task_type: TaskType) -> str:
    return f"task.{task_type.value}"


def update_routing_key(status: TaskStatus) -> str:
    return f"task_update.{status.value}"


def iso_ms(dt: datetime) -> str:
    """ISO 8601, millisecond precision, `Z` suffix (spec requirement)."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


IsoMs = Annotated[datetime, PlainSerializer(iso_ms, return_type=str)]


class TaskEvent(BaseModel):
    """Outbound (control plane -> worker). The task *is* the event: the payload is
    the task itself, and the AMQP message_id is the task id, so a consumer that
    receives it twice (at-least-once delivery) can dedupe."""

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: IsoMs


class TaskUpdateMessage(BaseModel):
    """Inbound (worker -> control plane). `message_id` is the idempotency key;
    `task_id` is the reference to the task being updated."""

    message_id: str = Field(min_length=1, max_length=128)
    task_id: UUID
    status: Literal[TaskStatus.IN_PROGRESS, TaskStatus.DONE, TaskStatus.FAILED]
