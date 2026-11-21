"""HTTP API behaviour: status codes, error contract, state guards, pagination."""

import uuid

import httpx
import pytest

from control_plane.domain.models import TaskStatus
from control_plane.services.task_update_service import TaskUpdateService
from tests.helpers import body, update_msg

pytestmark = pytest.mark.integration


async def _create(client: httpx.AsyncClient, slug: str = "acme") -> dict:  # type: ignore[type-arg]
    r = await client.post("/v1/tenants", json=body(slug=slug))
    assert r.status_code == 201, r.text
    return r.json()  # type: ignore[no-any-return]


async def _finish(
    update_service: TaskUpdateService, task_id: str, status: TaskStatus = TaskStatus.DONE
) -> None:
    await update_service.apply(update_msg(uuid.UUID(task_id), TaskStatus.IN_PROGRESS))
    await update_service.apply(update_msg(uuid.UUID(task_id), status))


def _assert_error(r: httpx.Response, status: int, code: str) -> None:
    assert r.status_code == status, r.text
    err = r.json()["error"]
    assert err["code"] == code
    assert isinstance(err["message"], str)
    assert err["request_id"] == r.headers["x-request-id"]


# ---------------------------------------------------------------- create / read


async def test_create_returns_tenant_and_task(client: httpx.AsyncClient) -> None:
    r = await client.post("/v1/tenants", json=body(slug="acme-corp", name="Acme Corp"))
    assert r.status_code == 201
    data = r.json()
    tenant, task = data["tenant"], data["task"]
    assert r.headers["location"] == f"/v1/tenants/{tenant['id']}"
    assert tenant["slug"] == "acme-corp"
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert task == {
        **task,
        "type": "deploy",
        "status": "accepted",
        "tenant_id": tenant["id"],
    }
    # ISO 8601, milliseconds, Z
    assert len(tenant["created_at"]) == len("2026-01-01T00:00:00.000Z")
    assert tenant["created_at"].endswith("Z")


async def test_get_and_list(client: httpx.AsyncClient) -> None:
    created = await _create(client)
    tid = created["tenant"]["id"]

    r = await client.get(f"/v1/tenants/{tid}")
    assert r.status_code == 200
    assert r.json() == created["tenant"]

    r = await client.get("/v1/tenants")
    assert [t["id"] for t in r.json()["items"]] == [tid]

    r = await client.get(f"/v1/tenants/{tid}/tasks")
    assert [t["id"] for t in r.json()["items"]] == [created["task"]["id"]]

    r = await client.get(f"/v1/tasks/{created['task']['id']}")
    assert r.json()["type"] == "deploy"


async def test_not_found_errors(client: httpx.AsyncClient) -> None:
    missing = uuid.uuid4()
    _assert_error(await client.get(f"/v1/tenants/{missing}"), 404, "tenant_not_found")
    _assert_error(await client.get(f"/v1/tenants/{missing}/tasks"), 404, "tenant_not_found")
    _assert_error(
        await client.patch(f"/v1/tenants/{missing}", json={"version": 1, "name": "x"}),
        404,
        "tenant_not_found",
    )
    _assert_error(await client.delete(f"/v1/tenants/{missing}"), 404, "tenant_not_found")
    _assert_error(await client.get(f"/v1/tasks/{missing}"), 404, "task_not_found")
    _assert_error(await client.get("/v1/nope"), 404, "not_found")


async def test_validation_errors_are_structured(client: httpx.AsyncClient) -> None:
    r = await client.post("/v1/tenants", json={"slug": "Bad_Slug", "name": ""})
    _assert_error(r, 422, "validation_error")
    locs = {tuple(e["loc"]) for e in r.json()["error"]["details"]["errors"]}
    assert locs == {("body", "slug"), ("body", "name")}

    _assert_error(await client.get("/v1/tenants/not-a-uuid"), 422, "validation_error")
    _assert_error(await client.get("/v1/tenants?status=bogus"), 422, "validation_error")
    _assert_error(await client.put("/v1/tenants"), 405, "method_not_allowed")


async def test_duplicate_slug(client: httpx.AsyncClient) -> None:
    await _create(client, "acme")
    r = await client.post("/v1/tenants", json=body(slug="acme", name="Other"))
    _assert_error(r, 409, "tenant_already_exists")


async def test_slug_stays_reserved_after_destroy(
    client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = await _create(client, "acme")
    await _finish(update_service, created["task"]["id"])
    deleted = (await client.delete(f"/v1/tenants/{created['tenant']['id']}")).json()
    await _finish(update_service, deleted["task"]["id"])
    assert (await client.get(f"/v1/tenants/{created['tenant']['id']}")).json()[
        "status"
    ] == "destroyed"

    _assert_error(
        await client.post("/v1/tenants", json=body(slug="acme")), 409, "tenant_already_exists"
    )


# ---------------------------------------------------------------- update (PATCH)


async def test_patch_happy_path(
    client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = await _create(client)
    tid = created["tenant"]["id"]
    await _finish(update_service, created["task"]["id"])
    active = (await client.get(f"/v1/tenants/{tid}")).json()
    assert active["status"] == "active"
    assert active["version"] == 2  # worker-applied status change bumped the version

    r = await client.patch(f"/v1/tenants/{tid}", json={"version": 2, "name": "Renamed"})
    assert r.status_code == 202
    tenant, task = r.json()["tenant"], r.json()["task"]
    assert (tenant["status"], tenant["name"], tenant["version"]) == ("updating", "Renamed", 3)
    assert (task["type"], task["status"]) == ("update", "accepted")

    await _finish(update_service, task["id"])
    final = (await client.get(f"/v1/tenants/{tid}")).json()
    assert (final["status"], final["version"]) == ("active", 4)


async def test_patch_stale_version(
    client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = await _create(client)
    await _finish(update_service, created["task"]["id"])
    r = await client.patch(
        f"/v1/tenants/{created['tenant']['id']}", json={"version": 1, "name": "x"}
    )
    _assert_error(r, 409, "tenant_version_conflict")
    assert r.json()["error"]["details"]["current_version"] == 2


@pytest.mark.parametrize("state", ["provisioning", "updating", "destroying", "destroyed", "failed"])
async def test_patch_only_allowed_from_active(
    state: str, client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = await _create(client)
    tid, task_id = created["tenant"]["id"], created["task"]["id"]
    if state != "provisioning":
        await _finish(
            update_service, task_id, TaskStatus.FAILED if state == "failed" else TaskStatus.DONE
        )
    if state == "updating":
        await client.patch(f"/v1/tenants/{tid}", json={"version": 2, "name": "x"})
    if state in ("destroying", "destroyed"):
        task_id = (await client.delete(f"/v1/tenants/{tid}")).json()["task"]["id"]
        if state == "destroyed":
            await _finish(update_service, task_id)

    current = (await client.get(f"/v1/tenants/{tid}")).json()
    assert current["status"] == state
    # Even with the *correct* version, the state forbids it.
    r = await client.patch(f"/v1/tenants/{tid}", json={"version": current["version"], "name": "y"})
    _assert_error(r, 409, "tenant_update_not_allowed")
    assert r.json()["error"]["details"]["current_status"] == state


# ---------------------------------------------------------------- delete


@pytest.mark.parametrize("outcome", [TaskStatus.DONE, TaskStatus.FAILED])
async def test_delete_from_active_or_failed(
    outcome: TaskStatus, client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = await _create(client)
    await _finish(update_service, created["task"]["id"], outcome)  # -> active / failed
    r = await client.delete(f"/v1/tenants/{created['tenant']['id']}")
    assert r.status_code == 202
    assert r.json()["tenant"]["status"] == "destroying"
    assert r.json()["task"]["type"] == "destroy"


async def test_delete_not_allowed_while_busy(client: httpx.AsyncClient) -> None:
    created = await _create(client)  # provisioning
    _assert_error(
        await client.delete(f"/v1/tenants/{created['tenant']['id']}"),
        409,
        "tenant_update_not_allowed",
    )


# ---------------------------------------------------------------- listing


async def test_pagination_and_filters(
    client: httpx.AsyncClient, update_service: TaskUpdateService
) -> None:
    created = [await _create(client, f"tenant-{i:02d}") for i in range(7)]
    await _finish(update_service, created[0]["task"]["id"])

    pages = [
        (await client.get("/v1/tenants", params={"limit": 3, "offset": offset})).json()
        for offset in (0, 3, 6)
    ]
    assert [len(p["items"]) for p in pages] == [3, 3, 1]
    assert (pages[1]["limit"], pages[1]["offset"]) == (3, 3)
    seen = [t["slug"] for p in pages for t in p["items"]]
    assert seen == [c["tenant"]["slug"] for c in reversed(created)]  # newest first, no gaps

    active = (await client.get("/v1/tenants", params={"status": "active"})).json()["items"]
    assert [t["slug"] for t in active] == ["tenant-00"]

    done = (await client.get("/v1/tasks", params={"status": "done", "type": "deploy"})).json()[
        "items"
    ]
    assert [t["id"] for t in done] == [created[0]["task"]["id"]]


async def test_limit_bounds(client: httpx.AsyncClient) -> None:
    _assert_error(await client.get("/v1/tenants?limit=0"), 422, "validation_error")
    _assert_error(await client.get("/v1/tenants?limit=201"), 422, "validation_error")
    _assert_error(await client.get("/v1/tenants?offset=-1"), 422, "validation_error")


async def test_readiness(client: httpx.AsyncClient) -> None:
    r = await client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["checks"] == {"database": "ok"}  # messaging disabled in tests
