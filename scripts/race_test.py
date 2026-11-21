"""Hammer the *running* API with concurrent requests and show the race rules hold.

    make race                      # inside the compose network
    uv run python scripts/race_test.py --base-url http://localhost:8000

Scenarios (each fires N requests at the same instant):
  1. N creates with the same slug        -> exactly 1 x 201, N-1 x 409 tenant_already_exists
  2. N PATCHes with the same version     -> exactly 1 x 202, N-1 x 409 (conflict / not allowed)
  3. N DELETEs of the same tenant        -> exactly 1 x 202, N-1 x 409 tenant_update_not_allowed
"""

import argparse
import asyncio
import sys
import time
import uuid
from collections import Counter

import httpx


def _summary(responses: list[httpx.Response]) -> Counter[str]:
    out: Counter[str] = Counter()
    for r in responses:
        code = r.json().get("error", {}).get("code", "") if r.status_code >= 400 else ""
        out[f"{r.status_code} {code}".strip()] += 1
    return out


async def _wait_status(client: httpx.AsyncClient, tenant_id: str, wanted: str) -> dict[str, object]:
    for _ in range(60):
        tenant: dict[str, object] = (await client.get(f"/v1/tenants/{tenant_id}")).json()
        if tenant["status"] == wanted:
            return tenant
        await asyncio.sleep(0.5)
    raise SystemExit(
        f"tenant {tenant_id} never reached {wanted}; is the worker running with --fail-rate=0?"
    )


def _check(name: str, responses: list[httpx.Response], ok_status: int) -> bool:
    summary = _summary(responses)
    winners = sum(1 for r in responses if r.status_code == ok_status)
    passed = winners == 1 and all(r.status_code in (ok_status, 409) for r in responses)
    print(f"{'PASS' if passed else 'FAIL'}  {name}: {dict(summary)}")
    return passed


async def main(base_url: str, n: int) -> int:
    limits = httpx.Limits(max_connections=n, max_keepalive_connections=n)
    async with httpx.AsyncClient(base_url=base_url, limits=limits, timeout=30) as client:
        slug = f"race-{uuid.uuid4().hex[:8]}"
        body = {"slug": slug, "name": "Race"}

        t0 = time.perf_counter()
        creates = await asyncio.gather(*(client.post("/v1/tenants", json=body) for _ in range(n)))
        ok = _check(f"{n} concurrent creates, slug={slug}", creates, 201)

        tenant_id = next(r.json()["tenant"]["id"] for r in creates if r.status_code == 201)
        tenant = await _wait_status(client, tenant_id, "active")

        patches = await asyncio.gather(
            *(
                client.patch(
                    f"/v1/tenants/{tenant_id}", json={"version": tenant["version"], "name": f"n{i}"}
                )
                for i in range(n)
            )
        )
        ok &= _check(f"{n} concurrent PATCH version={tenant['version']}", patches, 202)

        await _wait_status(client, tenant_id, "active")
        deletes = await asyncio.gather(
            *(client.delete(f"/v1/tenants/{tenant_id}") for _ in range(n))
        )
        ok &= _check(f"{n} concurrent DELETE", deletes, 202)

        tasks = (await client.get(f"/v1/tenants/{tenant_id}/tasks")).json()["items"]
        types = [t["type"] for t in reversed(tasks)]
        print(f"tasks created for the tenant: {types} (expected deploy, update, destroy)")
        ok &= types == ["deploy", "update", "destroy"]
        print(f"done in {time.perf_counter() - t0:.1f}s -> {'ALL PASS' if ok else 'FAILURES'}")
        return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", type=int, default=50)
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.base_url, args.n)))
