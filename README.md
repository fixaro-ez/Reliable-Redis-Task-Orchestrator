# Reliable-Redis-Task-Orchestrator
A high-throughput, fault-tolerant distributed job queue designed for asynchronous data processing and ETL task management.

## Minimal producer/worker structure

- `models.py`: shared Pydantic `Task` schema and JSON helpers.
- `producer.py`: pushes serialized tasks to `tasks:pending` via `LPUSH`.
- `worker.py`: runs an asyncio loop using `RPOPLPUSH` for at-least-once processing, retries with delayed `tasks:retry`, and dead-letter handling in `tasks:dead_letter`.
