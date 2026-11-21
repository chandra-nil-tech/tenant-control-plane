"""Database schema (SQLAlchemy 2.0 typed ORM).

* Enums are VARCHAR + CHECK constraint (`native_enum=False`): Postgres rejects
  invalid values, and adding a value later needs no `ALTER TYPE`.
* Timestamps come from the database clock (`now()`), one clock for every process.
* `slug` is unique across all tenants, including `destroyed` ones (we never hard-delete).
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from control_plane.domain.models import TaskStatus, TaskType, TenantStatus


def _enum(enum_cls: type[Any], name: str) -> Enum:
    # values_callable -> store "in_progress", not the member name "IN_PROGRESS".
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=32,
        values_callable=lambda e: [m.value for m in e],
    )


class Base(DeclarativeBase):
    pass


class TenantRow(Base):
    __tablename__ = "tenants"
    __table_args__ = (UniqueConstraint("slug", name="uq_tenants_slug"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(String(28), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[TenantStatus] = mapped_column(
        _enum(TenantStatus, "tenant_status"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TaskRow(Base):
    __tablename__ = "tasks"
    __table_args__ = (Index("ix_tasks_tenant_id", "tenant_id"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type: Mapped[TaskType] = mapped_column(_enum(TaskType, "task_type"), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    status: Mapped[TaskStatus] = mapped_column(_enum(TaskStatus, "task_status"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class OutboxRow(Base):
    """Transactional outbox: the event is written in the *same* transaction as the
    state change and published by the relay *after* commit (messaging/outbox_relay.py)."""

    __tablename__ = "outbox"
    __table_args__ = (
        # The relay only scans unpublished rows; keep that scan small as the table grows.
        Index("ix_outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    routing_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProcessedMessageRow(Base):
    """Idempotency keys of inbound worker updates already handled. Inserted in the
    same transaction that applies the update, so the two can never disagree."""

    __tablename__ = "processed_messages"

    message_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
