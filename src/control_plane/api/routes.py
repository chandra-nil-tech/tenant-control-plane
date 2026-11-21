"""HTTP routes. Thin on purpose: parse -> call one service method -> serialise.

    /v1/tenants              POST (create), GET (list)
    /v1/tenants/{id}         GET, PATCH (partial update), DELETE
    /v1/tenants/{id}/tasks   GET  tasks of one tenant (a sub-resource, not ?tenant_id=)
    /v1/tasks                GET  all tasks, filter by status/type
    /v1/tasks/{id}           GET

POST returns 201 (the tenant exists immediately, as `provisioning`).
PATCH and DELETE return 202 (accepted; the work happens asynchronously).
"""

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Response, status

from control_plane.api.deps import PageDep, TaskServiceDep, TenantServiceDep
from control_plane.api.schemas import (
    ErrorResponse,
    PageOut,
    TaskOut,
    TenantCreate,
    TenantOperationOut,
    TenantOut,
    TenantPatch,
)
from control_plane.domain.models import TaskStatus, TaskType, TenantStatus
from control_plane.services.tenant_service import TenantOperation

tenants = APIRouter(prefix="/v1/tenants", tags=["tenants"])
tasks = APIRouter(prefix="/v1/tasks", tags=["tasks"])

# Documents the structured error body in OpenAPI.
_Responses = dict[int | str, dict[str, Any]]
_404: _Responses = {404: {"model": ErrorResponse}}
_409: _Responses = {409: {"model": ErrorResponse}}
_422: _Responses = {422: {"model": ErrorResponse}}

StatusFilter = Annotated[TenantStatus | None, Query(alias="status")]
TaskStatusFilter = Annotated[TaskStatus | None, Query(alias="status")]
TaskTypeFilter = Annotated[TaskType | None, Query(alias="type")]


def _operation_out(op: TenantOperation) -> TenantOperationOut:
    return TenantOperationOut(
        tenant=TenantOut.model_validate(op.tenant), task=TaskOut.model_validate(op.task)
    )


@tenants.post("", status_code=status.HTTP_201_CREATED, responses={**_409, **_422})
async def create_tenant(
    body: TenantCreate, service: TenantServiceDep, response: Response
) -> TenantOperationOut:
    op = await service.create(slug=body.slug, name=body.name)
    response.headers["Location"] = f"/v1/tenants/{op.tenant.id}"
    return _operation_out(op)


@tenants.get("")
async def list_tenants(
    service: TenantServiceDep, page: PageDep, status_filter: StatusFilter = None
) -> PageOut[TenantOut]:
    rows = await service.list(status=status_filter, limit=page.limit, offset=page.offset)
    return PageOut(
        items=[TenantOut.model_validate(t) for t in rows], limit=page.limit, offset=page.offset
    )


@tenants.get("/{tenant_id}", responses=_404)
async def get_tenant(tenant_id: UUID, service: TenantServiceDep) -> TenantOut:
    return TenantOut.model_validate(await service.get(tenant_id))


@tenants.patch(
    "/{tenant_id}", status_code=status.HTTP_202_ACCEPTED, responses={**_404, **_409, **_422}
)
async def update_tenant(
    tenant_id: UUID, body: TenantPatch, service: TenantServiceDep
) -> TenantOperationOut:
    return _operation_out(
        await service.update(tenant_id, expected_version=body.version, changes=body.changes())
    )


@tenants.delete("/{tenant_id}", status_code=status.HTTP_202_ACCEPTED, responses={**_404, **_409})
async def delete_tenant(tenant_id: UUID, service: TenantServiceDep) -> TenantOperationOut:
    return _operation_out(await service.delete(tenant_id))


@tenants.get("/{tenant_id}/tasks", responses=_404)
async def list_tenant_tasks(
    tenant_id: UUID,
    service: TaskServiceDep,
    page: PageDep,
    status_filter: TaskStatusFilter = None,
    type_filter: TaskTypeFilter = None,
) -> PageOut[TaskOut]:
    rows = await service.list(
        tenant_id=tenant_id,
        status=status_filter,
        task_type=type_filter,
        limit=page.limit,
        offset=page.offset,
    )
    return PageOut(
        items=[TaskOut.model_validate(t) for t in rows], limit=page.limit, offset=page.offset
    )


@tasks.get("")
async def list_tasks(
    service: TaskServiceDep,
    page: PageDep,
    status_filter: TaskStatusFilter = None,
    type_filter: TaskTypeFilter = None,
) -> PageOut[TaskOut]:
    rows = await service.list(
        status=status_filter, task_type=type_filter, limit=page.limit, offset=page.offset
    )
    return PageOut(
        items=[TaskOut.model_validate(t) for t in rows], limit=page.limit, offset=page.offset
    )


@tasks.get("/{task_id}", responses=_404)
async def get_task(task_id: UUID, service: TaskServiceDep) -> TaskOut:
    return TaskOut.model_validate(await service.get(task_id))
