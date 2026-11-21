"""The two state machines from the spec, expressed as data.

Design choice: transitions are *tables*, not if/else chains. That gives us:
  * one place to read the whole machine (it mirrors the spec's diagrams 1:1),
  * exhaustive parametrized tests over every (state, event) pair,
  * Open/Closed: adding a task type means adding rows, not editing branches.

Everything here is pure and synchronous. The services call these functions and
then persist the result with *conditional* SQL updates, so the database stays
the final arbiter under concurrency (see repositories/).
"""

from dataclasses import dataclass

from control_plane.domain.models import TaskStatus, TaskType, TenantStatus

# --------------------------------------------------------------------------------------
# Tenant transitions triggered by the API
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ApiTransition:
    """A user-initiated change: allowed source states, target state, and the task it spawns."""

    allowed_from: frozenset[TenantStatus]
    to: TenantStatus
    task_type: TaskType


# Create has no "from" state: the tenant does not exist yet.
CREATE_TRANSITION = ApiTransition(
    allowed_from=frozenset(), to=TenantStatus.PROVISIONING, task_type=TaskType.DEPLOY
)
UPDATE_TRANSITION = ApiTransition(
    allowed_from=frozenset({TenantStatus.ACTIVE}),
    to=TenantStatus.UPDATING,
    task_type=TaskType.UPDATE,
)
DELETE_TRANSITION = ApiTransition(
    allowed_from=frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}),
    to=TenantStatus.DESTROYING,
    task_type=TaskType.DESTROY,
)

# Because every API transition goes from a "settled" state (active/failed) into a
# "busy" state (provisioning/updating/destroying), and busy states only leave via a
# terminal task outcome, a tenant can never have two open tasks. The "at most one
# open task per tenant" rule *falls out* of these guards - no extra lock needed.
BUSY_TENANT_STATUSES = frozenset(
    {TenantStatus.PROVISIONING, TenantStatus.UPDATING, TenantStatus.DESTROYING}
)


# --------------------------------------------------------------------------------------
# Task transitions reported by the worker
# --------------------------------------------------------------------------------------

# Progress is monotonic. A rank lets us express "only move forward" in one rule and
# is what makes the consumer order-tolerant: anything with rank <= current is stale.
_TASK_RANK: dict[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}


def should_apply_task_update(current: TaskStatus, incoming: TaskStatus) -> bool:
    """Return True if `incoming` moves the task forward.

    * terminal tasks never change (done/failed are final),
    * duplicates (`in_progress` twice) and regressions (`in_progress` after `done`)
      are stale and ignored,
    * a forward *jump* (`accepted` -> `done`, because `in_progress` was delayed or
      lost) is accepted: the terminal outcome is the information that matters, and
      refusing it would leave the tenant stuck forever.
    """
    if current.is_terminal:
        return False
    return _TASK_RANK[incoming] > _TASK_RANK[current]


# --------------------------------------------------------------------------------------
# Tenant transitions triggered by terminal task outcomes
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OutcomeTransition:
    expected_from: TenantStatus
    to: TenantStatus


TENANT_ON_TASK_OUTCOME: dict[tuple[TaskType, TaskStatus], OutcomeTransition] = {
    (TaskType.DEPLOY, TaskStatus.DONE): OutcomeTransition(
        TenantStatus.PROVISIONING, TenantStatus.ACTIVE
    ),
    (TaskType.DEPLOY, TaskStatus.FAILED): OutcomeTransition(
        TenantStatus.PROVISIONING, TenantStatus.FAILED
    ),
    (TaskType.UPDATE, TaskStatus.DONE): OutcomeTransition(
        TenantStatus.UPDATING, TenantStatus.ACTIVE
    ),
    (TaskType.UPDATE, TaskStatus.FAILED): OutcomeTransition(
        TenantStatus.UPDATING, TenantStatus.FAILED
    ),
    (TaskType.DESTROY, TaskStatus.DONE): OutcomeTransition(
        TenantStatus.DESTROYING, TenantStatus.DESTROYED
    ),
    (TaskType.DESTROY, TaskStatus.FAILED): OutcomeTransition(
        TenantStatus.DESTROYING, TenantStatus.FAILED
    ),
}


def tenant_status_after_outcome(
    task_type: TaskType, outcome: TaskStatus, current: TenantStatus
) -> TenantStatus | None:
    """Return the tenant's next status for a terminal task outcome, or None if the
    tenant is not in the state this task expects.

    `None` should be impossible given the guards above; we still check rather than
    blindly overwrite, because a defensive no-op + loud log is far better than a
    silent state corruption if an invariant is ever broken (manual DB edit, bug).
    """
    if not outcome.is_terminal:
        return None
    transition = TENANT_ON_TASK_OUTCOME[(task_type, outcome)]
    if current is not transition.expected_from:
        return None
    return transition.to
