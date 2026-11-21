"""Worker simulator: consumes tasks, fakes the provisioning work, reports progress.

    worker-sim --fail-rate 0.2 --min-delay-ms 500 --max-delay-ms 3000

Each flag can also be set via an env var (WORKER_FAIL_RATE, ...), so scenarios are
switched by restarting the container with different values. Two extra flags
reproduce the negative scenarios the control plane must survive:

    --duplicate-rate P   publish each update twice with probability P
    --out-of-order       send the terminal update *before* in_progress

Idempotency key: `message_id = uuid5(task_id, status)`. It is deterministic, so if
the worker crashes mid-task and the broker redelivers the task, the re-sent
updates carry the same ids and are recognised as duplicates.
"""

import argparse
import asyncio
import logging
import os
import random
import signal
import uuid
from dataclasses import dataclass

from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from control_plane.domain.models import TaskStatus
from control_plane.messaging import contracts as c
from control_plane.messaging.rabbitmq import (
    ConfirmedPublisher,
    connect_with_backoff,
    declare_topology,
)
from control_plane.observability import configure_logging

log = logging.getLogger("worker_sim")

# `random`, not `secrets`, throughout: this is a simulation, nothing here is security
# sensitive (hence the `nosec B311` markers for the security scanner).

# Fixed namespace so uuid5 ids are the same across processes and restarts.
_UPDATE_ID_NAMESPACE = uuid.UUID("6f1c1f9e-3a5e-4f43-9a55-0b1f7a3e2c11")
_PREFETCH = 10  # tasks processed concurrently


def update_message_id(task_id: uuid.UUID, status: TaskStatus) -> str:
    return str(uuid.uuid5(_UPDATE_ID_NAMESPACE, f"{task_id}:{status.value}"))


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    rabbitmq_url: str
    fail_rate: float
    min_delay_ms: int
    max_delay_ms: int
    duplicate_rate: float
    out_of_order: bool


def _probability(value: str) -> float:
    p = float(value)
    if not 0.0 <= p <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return p


def parse_args(argv: list[str] | None = None) -> WorkerConfig:
    env = os.environ.get
    p = argparse.ArgumentParser(prog="worker-sim")
    p.add_argument(
        "--rabbitmq-url", default=env("WORKER_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    )
    p.add_argument("--fail-rate", type=_probability, default=env("WORKER_FAIL_RATE", "0"))
    p.add_argument("--min-delay-ms", type=int, default=env("WORKER_MIN_DELAY_MS", "500"))
    p.add_argument("--max-delay-ms", type=int, default=env("WORKER_MAX_DELAY_MS", "2000"))
    p.add_argument("--duplicate-rate", type=_probability, default=env("WORKER_DUPLICATE_RATE", "0"))
    p.add_argument(
        "--out-of-order",
        action=argparse.BooleanOptionalAction,
        default=env("WORKER_OUT_OF_ORDER", "false").lower() in ("1", "true", "yes"),
    )
    a = p.parse_args(argv)
    if a.min_delay_ms < 0 or a.max_delay_ms < a.min_delay_ms:
        p.error("need 0 <= --min-delay-ms <= --max-delay-ms")
    return WorkerConfig(
        rabbitmq_url=a.rabbitmq_url,
        fail_rate=a.fail_rate,
        min_delay_ms=a.min_delay_ms,
        max_delay_ms=a.max_delay_ms,
        duplicate_rate=a.duplicate_rate,
        out_of_order=a.out_of_order,
    )


class Worker:
    def __init__(self, config: WorkerConfig, publisher: ConfirmedPublisher) -> None:
        self._cfg = config
        self._publisher = publisher

    async def _delay(self) -> None:
        delay_ms = random.uniform(self._cfg.min_delay_ms, self._cfg.max_delay_ms)  # nosec B311
        await asyncio.sleep(delay_ms / 1000)

    async def _report(self, task_id: uuid.UUID, status: TaskStatus) -> None:
        update = c.TaskUpdateMessage(
            message_id=update_message_id(task_id, status), task_id=task_id, status=status
        )
        copies = 2 if random.random() < self._cfg.duplicate_rate else 1  # nosec B311
        for _ in range(copies):
            await self._publisher.publish_json(
                body=update.model_dump_json().encode(),
                routing_key=c.update_routing_key(status),
                message_id=update.message_id,
            )
        log.info(
            "reported task status",
            extra={"task_id": task_id, "to_status": status, "message_id": update.message_id},
        )

    async def handle(self, message: AbstractIncomingMessage) -> None:
        try:
            try:
                task = c.TaskEvent.model_validate_json(message.body)
            except ValidationError:
                log.error(
                    "poison task message, dead-lettering", extra={"message_id": message.message_id}
                )
                await message.reject(requeue=False)
                return

            log.info(
                "picked up task",
                extra={"task_id": task.id, "task_type": task.type, "tenant_id": task.tenant_id},
            )
            failed = random.random() < self._cfg.fail_rate  # nosec B311
            steps = [TaskStatus.IN_PROGRESS, TaskStatus.FAILED if failed else TaskStatus.DONE]
            if self._cfg.out_of_order:
                steps.reverse()
            for status in steps:
                await self._delay()
                await self._report(task.id, status)

            # Ack only once the outcome is safely on the broker (at-least-once).
            await message.ack()
        except Exception:
            log.exception("task processing failed; returning the task to the queue")
            await message.nack(requeue=True)


async def run(config: WorkerConfig, stop: asyncio.Event | None = None) -> None:
    """Run until SIGINT/SIGTERM (or until `stop` is set, when embedded in tests)."""
    connection = await connect_with_backoff(config.rabbitmq_url)
    async with connection:
        await declare_topology(await connection.channel())
        worker = Worker(
            config, await ConfirmedPublisher.create(connection, c.TASK_UPDATES_EXCHANGE)
        )

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=_PREFETCH)
        queue = await channel.get_queue(c.WORKER_TASKS_QUEUE)
        await queue.consume(worker.handle)
        log.info("worker started with %s", config)

        if stop is None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        log.info("worker stopping")


def main() -> None:
    configure_logging(os.environ.get("WORKER_LOG_LEVEL", "INFO"))
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
