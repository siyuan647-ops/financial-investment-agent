"""Optional isolated-PostgreSQL crash probe; set TEST_CHECKPOINT_DATABASE_URL."""

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime
from uuid import UUID, uuid4

import anyio
import psycopg
import pytest

from app.repositories.migrate import MIGRATIONS
from app.repositories.research import ResearchRepository
from app.repositories.research_checkpoints import ResearchCheckpointStore

URL = os.getenv("TEST_CHECKPOINT_DATABASE_URL")
ROLES = ("fundamental", "news", "market", "investment", "risk")
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="module", autouse=True)
def schema():
    if not URL:
        return

    async def create():
        async with await psycopg.AsyncConnection.connect(URL, autocommit=True) as connection:
            cursor = await connection.execute("SELECT to_regclass('storage.research_task')")
            if (await cursor.fetchone())[0] is None:
                await connection.execute("CREATE SCHEMA storage")
                await connection.execute((MIGRATIONS / "019_research.up.sql").read_text("utf-8"))
                await connection.execute(
                    (MIGRATIONS / "036_research_checkpoints.up.sql").read_text("utf-8")
                )

    anyio.run(create)


@pytest.mark.skipif(not URL, reason="isolated PostgreSQL URL not configured")
@pytest.mark.parametrize("completed", (1, 3, 4, 5))
def test_abrupt_worker_exit_preserves_completed_nodes(completed):
    async def scenario():
        async with await psycopg.AsyncConnection.connect(URL, autocommit=True) as connection:
            repo = ResearchRepository(connection)
            task_id, _ = await repo.create(
                request_id=uuid4().hex,
                owner_id="checkpoint-probe",
                idempotency_key=uuid4().hex,
                question="probe",
                as_of=datetime(2026, 11, 19, tzinfo=UTC),
            )
            child = subprocess.run(
                [sys.executable, __file__, URL, str(task_id), str(completed)],
                cwd=str(MIGRATIONS.parent),
                check=False,
                timeout=20,
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": str(MIGRATIONS.parent)},
            )
            assert child.returncode == 0, child.stderr
            await asyncio.sleep(1.2)
            claimed = await repo.claim(task_id, "worker-b", 10)
            assert claimed is not None
            generation = await repo.claim_generation(task_id, "worker-b")
            assert generation == 2
            current = ResearchCheckpointStore(connection, task_id, "worker-b", generation)
            previous = ResearchCheckpointStore(connection, task_id, "worker-a", 1)
            for index, role in enumerate(ROLES):
                loaded = await current.load(role, "a" * 64, "probe-v1", 1)
                assert (loaded is not None) == (index < completed)
                if loaded is None:
                    await current.save(role, "a" * 64, "probe-v1", 1, {"role": role}, 1)
            with pytest.raises(ValueError, match="research_lease_lost_or_cancelled"):
                await previous.save("risk", "a" * 64, "probe-v1", 1, {"late": True}, 1)
            assert await current.load("risk", "a" * 64, "probe-v1", 1) != {"late": True}
            await repo.cancel(task_id, "checkpoint-probe")
            with pytest.raises(ValueError, match="research_cancelled"):
                await current.active()
            assert await repo.finish_cancel(task_id, "worker-b")
            assert (await repo.queue_state(task_id))[0] == "cancelled"

    anyio.run(scenario)


async def _crash_worker(url, task_id, completed):
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        repo = ResearchRepository(connection)
        assert await repo.claim(UUID(task_id), "worker-a", 1)
        store = ResearchCheckpointStore(connection, UUID(task_id), "worker-a", 1)
        for role in ROLES[:completed]:
            await store.save(role, "a" * 64, "probe-v1", 1, {"role": role}, 1)
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(_crash_worker(sys.argv[1], sys.argv[2], int(sys.argv[3])))
