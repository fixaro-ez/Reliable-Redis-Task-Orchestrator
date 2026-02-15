import unittest

from models import Task, task_from_json, task_to_json


class TaskModelTests(unittest.TestCase):
    def test_task_json_roundtrip(self) -> None:
        task = Task(payload={"kind": "sync"}, max_retries=5)
        raw = task_to_json(task)
        decoded = task_from_json(raw)

        self.assertEqual(decoded.task_id, task.task_id)
        self.assertEqual(decoded.payload, {"kind": "sync"})
        self.assertEqual(decoded.max_retries, 5)


if __name__ == "__main__":
    unittest.main()
