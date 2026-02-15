import asyncio
import os
import time
from typing import Optional

import redis.asyncio as redis

from models import task_from_json, task_to_json

PENDING_QUEUE = "tasks:pending"
PROCESSING_QUEUE = "tasks:processing"
RETRY_SET = "tasks:retry"
DEAD_LETTER_QUEUE = "tasks:dead_letter"
RETRY_DELAY_SECONDS = 30


async def move_due_retries(redis_client: redis.Redis) -> None:
    due_tasks = await redis_client.zrangebyscore(RETRY_SET, min="-inf", max=time.time())
    if not due_tasks:
        return

    pipeline = redis_client.pipeline()
    for raw_task in due_tasks:
        pipeline.lpush(PENDING_QUEUE, raw_task)
        pipeline.zrem(RETRY_SET, raw_task)
    await pipeline.execute()


async def process_task(redis_client: redis.Redis, raw_task: str) -> None:
    task = task_from_json(raw_task)
    try:
        await asyncio.sleep(1)
        if task.payload.get("should_fail"):
            raise RuntimeError("Simulated task failure")
        await redis_client.lrem(PROCESSING_QUEUE, 1, raw_task)
        print(f"Task {task.task_id} processed successfully")
    except Exception:
        task.retries += 1
        updated_task = task_to_json(task)
        pipeline = redis_client.pipeline()
        pipeline.lrem(PROCESSING_QUEUE, 1, raw_task)
        if task.retries < task.max_retries:
            retry_at = time.time() + RETRY_DELAY_SECONDS
            pipeline.zadd(RETRY_SET, {updated_task: retry_at})
            print(f"Task {task.task_id} failed; scheduled retry #{task.retries}")
        else:
            pipeline.lpush(DEAD_LETTER_QUEUE, updated_task)
            print(f"Task {task.task_id} moved to dead letter queue")
        await pipeline.execute()


async def worker_loop(redis_client: redis.Redis, poll_interval: float = 1.0) -> None:
    while True:
        await move_due_retries(redis_client)
        raw_task: Optional[str] = await redis_client.rpoplpush(
            PENDING_QUEUE, PROCESSING_QUEUE
        )
        if raw_task is None:
            await asyncio.sleep(poll_interval)
            continue
        await process_task(redis_client, raw_task)


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = redis.from_url(redis_url, decode_responses=True)
    await worker_loop(client)


if __name__ == "__main__":
    asyncio.run(main())
