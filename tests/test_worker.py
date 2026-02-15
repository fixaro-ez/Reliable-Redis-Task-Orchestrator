import unittest
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from models import Task, task_to_json
from worker import (
    DEAD_LETTER_QUEUE,
    PROCESSING_QUEUE,
    RETRY_SET,
    move_due_retries,
    process_task,
)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_move_due_retries_requeues_and_removes(self) -> None:
        redis_client = MagicMock()
        redis_client.zrangebyscore = AsyncMock(return_value=["task-a"])
        pipeline = MagicMock()
        pipeline.execute = AsyncMock()
        redis_client.pipeline.return_value = pipeline

        await move_due_retries(redis_client)

        redis_client.zrangebyscore.assert_awaited_once()
        pipeline.lpush.assert_called_once_with("tasks:pending", "task-a")
        pipeline.zrem.assert_called_once_with(RETRY_SET, "task-a")
        pipeline.execute.assert_awaited_once()

    async def test_process_task_success_removes_processing_entry(self) -> None:
        redis_client = MagicMock()
        redis_client.lrem = AsyncMock()
        task = Task(payload={"should_fail": False})

        with patch("worker.asyncio.sleep", new=AsyncMock()):
            await process_task(redis_client, task_to_json(task))

        redis_client.lrem.assert_awaited_once_with(
            PROCESSING_QUEUE, 1, task_to_json(task)
        )

    async def test_process_task_failure_retries_then_dead_letters(self) -> None:
        redis_client = MagicMock()
        pipeline = MagicMock()
        pipeline.execute = AsyncMock()
        redis_client.pipeline.return_value = pipeline

        retry_task = Task(payload={"should_fail": True}, retries=0, max_retries=2)
        dead_task = Task(payload={"should_fail": True}, retries=2, max_retries=2)

        with patch("worker.asyncio.sleep", new=AsyncMock()):
            await process_task(redis_client, task_to_json(retry_task))
        pipeline.zadd.assert_called_once()
        pipeline.lpush.assert_not_called()

        pipeline.reset_mock()
        with patch("worker.asyncio.sleep", new=AsyncMock()):
            await process_task(redis_client, task_to_json(dead_task))
        pipeline.lpush.assert_called_once_with(DEAD_LETTER_QUEUE, ANY)


if __name__ == "__main__":
    unittest.main()
