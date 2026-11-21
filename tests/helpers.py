"""Small builders shared by tests."""

import uuid
from typing import Any

from control_plane.domain.models import TaskStatus
from control_plane.messaging.contracts import TaskUpdateMessage


def update_msg(
    task_id: uuid.UUID, status: TaskStatus, *, message_id: str | None = None
) -> TaskUpdateMessage:
    return TaskUpdateMessage(
        message_id=message_id or str(uuid.uuid4()), task_id=task_id, status=status
    )


def body(**overrides: Any) -> dict[str, Any]:
    return {"slug": "acme", "name": "Acme", **overrides}
