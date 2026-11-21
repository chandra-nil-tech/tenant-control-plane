"""Builds the outbound event for a freshly created task."""

from control_plane.db.tables import TaskRow
from control_plane.messaging.contracts import TaskEvent, task_routing_key
from control_plane.repositories.outbox import OutboundEvent


def build_task_event(task: TaskRow) -> OutboundEvent:
    event = TaskEvent(
        id=task.id,
        type=task.type,
        tenant_id=task.tenant_id,
        status=task.status,
        created_at=task.created_at,
    )
    return OutboundEvent(
        event_id=task.id,
        routing_key=task_routing_key(task.type),
        payload=event.model_dump(mode="json"),
    )
