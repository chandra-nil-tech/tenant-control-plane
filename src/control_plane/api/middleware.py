"""Request-id + access-log middleware (pure ASGI, so it also wraps error responses)."""

import logging
import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from control_plane.api.errors import error_response
from control_plane.observability import request_id_ctx

log = logging.getLogger("control_plane.access")

_HEADER = b"x-request-id"


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Honour an upstream id (gateway/load balancer) so traces join up; else mint one.
        incoming = dict(scope["headers"]).get(_HEADER)
        request_id = incoming.decode()[:128] if incoming else str(uuid.uuid4())
        token = request_id_ctx.set(request_id)
        started = time.perf_counter()
        status_code = 500
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                message.setdefault("headers", []).append((_HEADER, request_id.encode()))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # Last line of defence: an unexpected bug. Log the stack trace (with the
            # request id), return the structured 500 body - never the trace itself.
            log.exception(
                "unhandled error", extra={"method": scope.get("method"), "path": scope.get("path")}
            )
            if response_started:
                raise
            response = error_response(500, "internal_error", "an unexpected error occurred")
            await response(scope, receive, send_wrapper)
        finally:
            path = scope.get("path", "")
            if path != "/readyz":  # keep healthcheck noise out of the logs
                log.info(
                    "request",
                    extra={
                        "method": scope.get("method"),
                        "path": path,
                        "status_code": status_code,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    },
                )
            request_id_ctx.reset(token)
