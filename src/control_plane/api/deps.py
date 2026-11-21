"""Dependency injection for routes.

main.py builds one `Container` per app and puts it on `app.state`; routes ask for
a service via `Depends(...)` and never build infrastructure themselves.
"""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Query, Request

from control_plane.services.task_service import TaskQueryService
from control_plane.services.tenant_service import TenantService


@dataclass(frozen=True, slots=True)
class Container:
    tenant_service: TenantService
    task_service: TaskQueryService


def _container(request: Request) -> Container:
    container: Container = request.app.state.container
    return container


def get_tenant_service(container: Annotated[Container, Depends(_container)]) -> TenantService:
    return container.tenant_service


def get_task_service(container: Annotated[Container, Depends(_container)]) -> TaskQueryService:
    return container.task_service


@dataclass(frozen=True, slots=True)
class PageParams:
    limit: int
    offset: int


def page_params(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PageParams:
    return PageParams(limit=limit, offset=offset)


TenantServiceDep = Annotated[TenantService, Depends(get_tenant_service)]
TaskServiceDep = Annotated[TaskQueryService, Depends(get_task_service)]
PageDep = Annotated[PageParams, Depends(page_params)]
