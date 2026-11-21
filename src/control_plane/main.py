"""Composition root: the only place that wires concrete classes together.

`create_app()` builds settings -> engine -> services -> routes, and starts the
messaging runtime (outbox relay + consumer) in the lifespan. Tests call
`create_app(Settings(enable_messaging=False, ...))` and drive the services directly.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from control_plane.api import routes
from control_plane.api.deps import Container
from control_plane.api.errors import error_response, register_error_handlers
from control_plane.api.middleware import RequestContextMiddleware
from control_plane.config import Settings, get_settings
from control_plane.db.session import create_engine, create_session_factory
from control_plane.messaging.runtime import MessagingRuntime
from control_plane.observability import configure_logging
from control_plane.services.task_service import TaskQueryService
from control_plane.services.tenant_service import TenantService

log = logging.getLogger(__name__)

# In a container the server must listen on all interfaces; docker-compose decides
# which port is exposed to the host.
_BIND_ALL_INTERFACES = "0.0.0.0"  # noqa: S104  # nosec B104


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        app.state.engine = engine
        app.state.container = Container(
            tenant_service=TenantService(session_factory),
            task_service=TaskQueryService(session_factory),
        )
        messaging = (
            MessagingRuntime(settings, session_factory) if settings.enable_messaging else None
        )
        if messaging is not None:
            await messaging.start()
        app.state.messaging = messaging
        log.info("control plane started")
        try:
            yield
        finally:
            if messaging is not None:
                await messaging.stop()
            await engine.dispose()

    app = FastAPI(title="Tenant Provisioning Control Plane", version="1.0.0", lifespan=lifespan)
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    app.include_router(routes.tenants)
    app.include_router(routes.tasks)

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> JSONResponse:
        """Readiness (used by the compose healthcheck): DB reachable and broker connected."""
        checks: dict[str, str] = {}
        try:
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["database"] = "ok"
        except Exception:
            checks["database"] = "unavailable"
        messaging: MessagingRuntime | None = request.app.state.messaging
        if messaging is not None:
            checks["broker"] = "ok" if messaging.is_connected else "unavailable"
        if all(v == "ok" for v in checks.values()):
            return JSONResponse({"status": "ready", "checks": checks})
        return error_response(503, "not_ready", "dependencies unavailable", {"checks": checks})

    return app


def run() -> None:
    """Entry point for `control-plane` (see pyproject [project.scripts])."""
    uvicorn.run(
        "control_plane.main:create_app",
        factory=True,
        host=_BIND_ALL_INTERFACES,
        port=8000,
        log_config=None,  # we configure logging ourselves (JSON)
    )
