"""HTTP request/response models (the public API contract).

Separate from the DB rows on purpose: the API and the schema can evolve independently.
"""

from typing import Annotated, Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from control_plane.domain.models import SLUG_PATTERN, TaskStatus, TaskType, TenantStatus
from control_plane.messaging.contracts import IsoMs

Slug = Annotated[str, StringConstraints(min_length=3, max_length=28, pattern=SLUG_PATTERN)]
# "Non-empty": whitespace is stripped first, so "   " is rejected too.
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class TenantCreate(BaseModel):
    # forbid: a typo like {"nmae": ...} is a 422, not a silently ignored field.
    model_config = ConfigDict(extra="forbid")

    slug: Slug
    name: Name


class TenantPatch(BaseModel):
    """Partial update. `version` (read from GET) is the optimistic-lock token.
    `slug` is not patchable: it is the tenant's k8s namespace."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    name: Name | None = None

    @model_validator(mode="after")
    def _at_least_one_change(self) -> Self:
        if not self.changes():
            raise ValueError("at least one mutable field (name) must be provided")
        return self

    def changes(self) -> dict[str, Any]:
        return self.model_dump(exclude={"version"}, exclude_unset=True, exclude_none=True)


class _FromRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class TenantOut(_FromRow):
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: IsoMs
    updated_at: IsoMs


class TaskOut(_FromRow):
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: IsoMs
    updated_at: IsoMs


class TenantOperationOut(BaseModel):
    """Returned by POST/PATCH/DELETE: the tenant as written plus the task that
    tracks the asynchronous work."""

    tenant: TenantOut
    task: TaskOut


class PageOut[T](BaseModel):
    items: list[T]
    limit: int
    offset: int


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: str | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail
