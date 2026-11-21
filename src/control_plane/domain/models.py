"""Domain vocabulary. Pure Python: no FastAPI, no SQLAlchemy, no aio-pika.

Keeping the domain free of infrastructure imports is what lets us unit-test the
state machine exhaustively in milliseconds, and is the "D" in SOLID: the
infrastructure depends on the domain, never the other way round.
"""

from enum import StrEnum


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_TASK_STATUSES


TERMINAL_TASK_STATUSES = frozenset({TaskStatus.DONE, TaskStatus.FAILED})


# k8s-namespace compatible: starts with a letter, ends alphanumeric, 3-28 chars total.
# The {1,26} middle + 2 anchored chars gives exactly the 3..28 length range.
SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
