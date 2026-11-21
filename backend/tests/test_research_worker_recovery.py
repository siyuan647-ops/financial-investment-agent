"""Two-consumer queue and PG lease coordination regressions."""

from types import SimpleNamespace
from uuid import uuid4

import anyio
import pytest

from app.repositories.research import ResearchRepository
from app.services.research_queue import ResearchQueue, ResearchQueueMessage
from app.workers.research import ResearchWorker


class Queue:
    def __init__(self):
        self.acked = []
        self.refreshed = []

    async def ack(self, message_id):
        self.acked.append(message_id)

    async def refresh(self, worker_id, message_id):
        self.refreshed.append((worker_id, message_id))
        return True


class Repository:
    def __init__(self, status="running", owner="worker-a", lease_valid=True):
        self.status = status
        self.owner = owner
        self.lease_valid = lease_valid
        self.cancel_requested = False

    async def claim(self, task_id, worker_id, lease_seconds):
        del task_id, lease_seconds
        if not self.cancel_requested and (
            self.status == "queued" or (self.status == "running" and not self.lease_valid)
        ):
            self.status = "running"
            self.owner = worker_id
            self.lease_valid = True
            return ("request", "owner", "target", None, None)
        return None

    async def queue_state(self, task_id):
        del task_id
        return self.status, self.owner, "future" if self.lease_valid else "expired"

    async def heartbeat(self, task_id, worker_id, lease_seconds):
        del task_id, lease_seconds
        return self.status == "running" and self.owner == worker_id and self.lease_valid

    async def finish_expired_cancel(self, task_id):
        del task_id
        if self.status != "running" or not self.cancel_requested or self.lease_valid:
            return False
        self.status = "cancelled"
        return True


def worker(worker_id, repository, queue):
    value = object.__new__(ResearchWorker)
    value.worker_id = worker_id
    value.repository = repository
    value.queue = queue
    value.settings = SimpleNamespace(deep_research_lease_seconds=60)
    return value


def test_second_worker_preserves_live_message_then_can_reclaim_after_crash():
    repository = Repository()
    queue = Queue()
    first = worker("worker-a", repository, queue)
    second = worker("worker-b", repository, queue)
    task_id = uuid4()
    message = ResearchQueueMessage("1-0", str(task_id))

    async def scenario():
        assert await first.heartbeat_once(task_id, message.message_id)
        await second.process(message)
        assert queue.acked == []
        repository.lease_valid = False  # A crashed and its PG lease expired.
        assert not await first.heartbeat_once(task_id, message.message_id)
        assert queue.refreshed == [("worker-a", "1-0")]
        assert await repository.claim(task_id, second.worker_id, 60) is not None
        assert repository.owner == "worker-b"

    anyio.run(scenario)


@pytest.mark.parametrize("status", ["succeeded", "partial", "failed", "cancelled"])
def test_reclaimed_terminal_message_is_acknowledged(status):
    repository = Repository(status=status)
    queue = Queue()
    message = ResearchQueueMessage("2-0", str(uuid4()))
    anyio.run(worker("worker-b", repository, queue).process, message)
    assert queue.acked == [message.message_id]


def test_cancel_requested_while_running_keeps_message_until_terminal():
    repository = Repository()
    queue = Queue()
    message = ResearchQueueMessage("3-0", str(uuid4()))

    async def scenario():
        repository.cancel_requested = True
        await worker("worker-b", repository, queue).process(message)
        assert queue.acked == []
        repository.lease_valid = False
        await worker("worker-b", repository, queue).process(message)
        assert repository.status == "cancelled"
        assert queue.acked == [message.message_id]

    anyio.run(scenario)


def test_redis_refresh_uses_pending_message_claim_without_payload():
    class Redis:
        async def xclaim(self, *args, **kwargs):
            assert args == ("research:tasks", "research-workers", "worker-a")
            assert kwargs == {
                "min_idle_time": 0,
                "message_ids": ["4-0"],
                "justid": True,
            }
            return ["4-0"]

    assert anyio.run(ResearchQueue(Redis()).refresh, "worker-a", "4-0")


def test_stale_worker_cannot_commit_after_lease_loss():
    class Cursor:
        rowcount = 0

    class Connection:
        async def execute(self, statement, parameters):
            assert "lease_owner=%s AND lease_until>=now()" in statement
            assert parameters[-3] == "worker-a"
            return Cursor()

    state = {
        "status": "partial",
        **{
            key: SimpleNamespace(model_dump=lambda **kwargs: {})
            for key in ("plan", "security", "execution", "report", "verification")
        },
    }
    with pytest.raises(ValueError, match="research_completion_conflict"):
        anyio.run(ResearchRepository(Connection()).complete, uuid4(), "worker-a", state)
