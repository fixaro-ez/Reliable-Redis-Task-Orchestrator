import os
from typing import Any, Dict

import redis

from models import Task, task_to_json

PENDING_QUEUE = "tasks:pending"


def enqueue_task(
    redis_client: redis.Redis, payload: Dict[str, Any], max_retries: int = 3
) -> Task:
    task = Task(payload=payload, max_retries=max_retries)
    redis_client.lpush(PENDING_QUEUE, task_to_json(task))
    return task


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    task = enqueue_task(client, payload={"job": "example"}, max_retries=3)
    print(f"Queued task {task.task_id}")


if __name__ == "__main__":
    main()
