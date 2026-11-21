"""Domain errors.

Each error carries a stable machine-readable `code` (this is a public contract,
clients branch on it) and an HTTP status. The API layer has a single exception
handler that turns any DomainError into the documented error body, so routes
never build error responses by hand.
"""

from typing import Any, ClassVar


class DomainError(Exception):
    code: ClassVar[str] = "internal_error"
    http_status: ClassVar[int] = 500

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class TenantNotFound(DomainError):
    code = "tenant_not_found"
    http_status = 404


class TenantAlreadyExists(DomainError):
    code = "tenant_already_exists"
    http_status = 409


class TenantUpdateNotAllowed(DomainError):
    """The tenant's current status does not allow this mutation (PATCH or DELETE)."""

    code = "tenant_update_not_allowed"
    http_status = 409


class TenantVersionConflict(DomainError):
    """Optimistic-lock failure: the client's `version` is stale."""

    code = "tenant_version_conflict"
    http_status = 409


class TaskNotFound(DomainError):
    code = "task_not_found"
    http_status = 404


# ---- messaging-side errors (never reach HTTP) ----------------------------------------


class UnknownTaskInUpdate(DomainError):
    """A worker update referenced a task id we have never created: a poison message."""

    code = "unknown_task"
