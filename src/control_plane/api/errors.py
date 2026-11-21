"""One place that turns every failure into the documented error body:

    {"error": {"code": "...", "message": "...", "details": {...}, "request_id": "..."}}

Status mapping (also in the README):
    404 tenant_not_found, task_not_found, not_found (unknown route)
    405 method_not_allowed
    409 tenant_already_exists, tenant_update_not_allowed, tenant_version_conflict
    422 validation_error
    500 internal_error   (see middleware.py; never leaks a stack trace, it goes to the logs)
"""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from control_plane.api.schemas import ErrorDetail, ErrorResponse
from control_plane.domain.errors import DomainError
from control_plane.observability import request_id_ctx

_HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def error_response(
    status: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    body = ErrorResponse(
        error=ErrorDetail(
            code=code, message=message, details=details or {}, request_id=request_id_ctx.get()
        )
    )
    return JSONResponse(status_code=status, content=jsonable_encoder(body))


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(_: Request, exc: DomainError) -> JSONResponse:
        return error_response(exc.http_status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        # Keep pydantic's precise locations ("body.slug") but drop the raw input
        # echo, which could reflect large or sensitive payloads back.
        errors = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
        return error_response(
            422, "validation_error", "request validation failed", {"errors": errors}
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_CODES.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail))
