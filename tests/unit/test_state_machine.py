"""Exhaustive tests of the pure state machines: every (state, event) pair."""

import itertools

import pytest

from control_plane.domain.models import TaskStatus, TaskType, TenantStatus
from control_plane.domain.state_machine import (
    BUSY_TENANT_STATUSES,
    DELETE_TRANSITION,
    TENANT_ON_TASK_OUTCOME,
    UPDATE_TRANSITION,
    should_apply_task_update,
    tenant_status_after_outcome,
)

S = TaskStatus

# The complete truth table for task updates: (current, incoming) -> apply?
TASK_TABLE = {
    (S.ACCEPTED, S.IN_PROGRESS): True,
    (S.ACCEPTED, S.DONE): True,  # forward jump: in_progress was lost/delayed
    (S.ACCEPTED, S.FAILED): True,
    (S.IN_PROGRESS, S.IN_PROGRESS): False,  # duplicate content, new id
    (S.IN_PROGRESS, S.DONE): True,
    (S.IN_PROGRESS, S.FAILED): True,
    (S.DONE, S.IN_PROGRESS): False,  # stale: arrived after done
    (S.DONE, S.DONE): False,
    (S.DONE, S.FAILED): False,  # terminal is final
    (S.FAILED, S.IN_PROGRESS): False,
    (S.FAILED, S.DONE): False,
    (S.FAILED, S.FAILED): False,
}


@pytest.mark.parametrize(("current", "incoming"), list(TASK_TABLE))
def test_task_update_truth_table(current: TaskStatus, incoming: TaskStatus) -> None:
    assert should_apply_task_update(current, incoming) is TASK_TABLE[(current, incoming)]


def test_terminal_flags() -> None:
    assert {s for s in TaskStatus if s.is_terminal} == {S.DONE, S.FAILED}


EXPECTED_OUTCOMES = {
    (TaskType.DEPLOY, S.DONE): (TenantStatus.PROVISIONING, TenantStatus.ACTIVE),
    (TaskType.DEPLOY, S.FAILED): (TenantStatus.PROVISIONING, TenantStatus.FAILED),
    (TaskType.UPDATE, S.DONE): (TenantStatus.UPDATING, TenantStatus.ACTIVE),
    (TaskType.UPDATE, S.FAILED): (TenantStatus.UPDATING, TenantStatus.FAILED),
    (TaskType.DESTROY, S.DONE): (TenantStatus.DESTROYING, TenantStatus.DESTROYED),
    (TaskType.DESTROY, S.FAILED): (TenantStatus.DESTROYING, TenantStatus.FAILED),
}


def test_outcome_table_matches_spec() -> None:
    actual = {k: (v.expected_from, v.to) for k, v in TENANT_ON_TASK_OUTCOME.items()}
    assert actual == EXPECTED_OUTCOMES


@pytest.mark.parametrize(
    ("task_type", "outcome", "tenant_status"),
    list(itertools.product(TaskType, (S.DONE, S.FAILED), TenantStatus)),
)
def test_outcome_only_applies_from_expected_tenant_state(
    task_type: TaskType, outcome: TaskStatus, tenant_status: TenantStatus
) -> None:
    expected_from, to = EXPECTED_OUTCOMES[(task_type, outcome)]
    result = tenant_status_after_outcome(task_type, outcome, tenant_status)
    assert result == (to if tenant_status is expected_from else None)


@pytest.mark.parametrize("non_terminal", [S.ACCEPTED, S.IN_PROGRESS])
def test_non_terminal_outcome_never_moves_tenant(non_terminal: TaskStatus) -> None:
    for task_type, tenant_status in itertools.product(TaskType, TenantStatus):
        assert tenant_status_after_outcome(task_type, non_terminal, tenant_status) is None


def test_api_guards() -> None:
    assert UPDATE_TRANSITION.allowed_from == {TenantStatus.ACTIVE}
    assert DELETE_TRANSITION.allowed_from == {TenantStatus.ACTIVE, TenantStatus.FAILED}


def test_at_most_one_open_task_falls_out_of_guards() -> None:
    """No API transition may start from a busy state (one that has an open task),
    and every API transition lands in a busy state."""
    for transition in (UPDATE_TRANSITION, DELETE_TRANSITION):
        assert not transition.allowed_from & BUSY_TENANT_STATUSES
        assert transition.to in BUSY_TENANT_STATUSES
    # ... and busy states are left only via terminal task outcomes
    assert {v.expected_from for v in TENANT_ON_TASK_OUTCOME.values()} == BUSY_TENANT_STATUSES
