from typing import Any, Dict
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Task(BaseModel):
    task_id: UUID = Field(default_factory=uuid4)
    payload: Dict[str, Any]
    retries: int = 0
    max_retries: int = 3


def task_to_json(task: Task) -> str:
    if hasattr(task, "model_dump_json"):
        return task.model_dump_json()
    return task.json()


def task_from_json(raw_task: str) -> Task:
    if hasattr(Task, "model_validate_json"):
        return Task.model_validate_json(raw_task)
    return Task.parse_raw(raw_task)
