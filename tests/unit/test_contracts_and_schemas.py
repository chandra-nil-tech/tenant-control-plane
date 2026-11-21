"""Validation rules for HTTP payloads and wire messages."""

import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from control_plane.api.schemas import TenantCreate, TenantPatch
from control_plane.messaging.contracts import TaskUpdateMessage, iso_ms

VALID_SLUGS = ["abc", "a-b", "acme-corp", "a1b", "x" * 28, "tenant-42"]
INVALID_SLUGS = [
    "ab",  # too short
    "x" * 29,  # too long
    "1abc",  # must start with a letter
    "-abc",
    "abc-",  # must end alphanumeric
    "Acme",  # lowercase only
    "ac_me",  # underscore not allowed
    "ac.me",
    "acme corp",
    "",
]


@pytest.mark.parametrize("slug", VALID_SLUGS)
def test_valid_slugs(slug: str) -> None:
    TenantCreate(slug=slug, name="n")


@pytest.mark.parametrize("slug", INVALID_SLUGS)
def test_invalid_slugs(slug: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug=slug, name="n")


@pytest.mark.parametrize("name", ["", "   "])
def test_name_must_be_non_empty(name: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug="acme", name=name)


def test_create_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TenantCreate.model_validate({"slug": "acme", "name": "A", "status": "active"})


def test_patch_requires_version_and_a_change() -> None:
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"name": "x"})  # no version
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"version": 1})  # nothing to change
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"version": 1, "slug": "other"})  # slug immutable
    assert TenantPatch.model_validate({"version": 3, "name": " New "}).changes() == {"name": "New"}


def test_iso_ms_format() -> None:
    dt = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    assert iso_ms(dt) == "2026-01-02T03:04:05.678Z"
    ist = timezone(timedelta(hours=5, minutes=30))
    assert iso_ms(dt.astimezone(ist)) == "2026-01-02T03:04:05.678Z"


def _update(**overrides: object) -> dict[str, object]:
    return {
        "message_id": "m-1",
        "task_id": str(uuid.uuid4()),
        "status": "done",
        **overrides,
    }


def test_update_message_accepts_reportable_statuses() -> None:
    for status in ("in_progress", "done", "failed"):
        assert TaskUpdateMessage.model_validate(_update(status=status)).status == status


@pytest.mark.parametrize(
    "bad",
    [
        {"status": "accepted"},  # workers cannot report `accepted`
        {"status": "exploded"},
        {"task_id": "not-a-uuid"},
        {"message_id": ""},
    ],
)
def test_update_message_rejects_invalid(bad: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TaskUpdateMessage.model_validate(_update(**bad))


def test_update_message_ignores_unknown_fields() -> None:
    TaskUpdateMessage.model_validate(_update(progress_pct=50))
