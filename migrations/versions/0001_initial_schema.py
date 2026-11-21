"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-11-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("routing_key", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_messages",
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("message_id"),
    )
    op.create_table(
        "tenants",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("slug", sa.String(length=28), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "provisioning",
                "active",
                "updating",
                "destroying",
                "destroyed",
                "failed",
                name="tenant_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_tenants_slug"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column(
            "type",
            sa.Enum(
                "deploy",
                "update",
                "destroy",
                name="task_type",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "accepted",
                "in_progress",
                "done",
                "failed",
                name="task_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_tasks_tenant_id", "tasks", ["tenant_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_tasks_tenant_id", table_name="tasks")
    op.drop_table("tasks")
    op.drop_table("tenants")
    op.drop_table("processed_messages")
    op.drop_index(
        "ix_outbox_unpublished",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
