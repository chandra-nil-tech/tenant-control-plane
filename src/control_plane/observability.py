"""JSON logging.

Every line is one JSON object, so ids passed via `extra={...}` (tenant_id,
task_id, message_id, ...) are searchable fields rather than text to grep.
The HTTP request id lives in a contextvar (task-local in asyncio, so concurrent
requests never mix up ids) and is added to every line logged during a request.
"""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)

_EXTRA_FIELDS = (
    "tenant_id",
    "task_id",
    "message_id",
    "task_type",
    "from_status",
    "to_status",
    "outcome",
    "attempt",
    "method",
    "path",
    "status_code",
    "duration_ms",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if (rid := request_id_ctx.get()) is not None:
            doc["request_id"] = rid
        for field in _EXTRA_FIELDS:
            if (value := getattr(record, field, None)) is not None:
                doc[field] = value if isinstance(value, int | float) else str(value)
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("aio_pika", "aiormq", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
