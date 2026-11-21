"""Fenced PostgreSQL checkpoints for completed research graph nodes."""

import argparse
import asyncio
import hashlib
import os

import psycopg
from psycopg.types.json import Jsonb

from app.repositories.migrate import MIGRATIONS


async def migrate_research_checkpoints(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256(
        (MIGRATIONS / "036_research_checkpoints.up.sql").read_bytes()
    ).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('019','036')"
        )
        versions = dict(await cursor.fetchall())
        if direction == "up" and "019" not in versions:
            raise ValueError("step19_migration_required")
        if "036" in versions and versions["036"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("036" in versions):
            return False
        await connection.execute(
            (MIGRATIONS / f"036_research_checkpoints.{direction}.sql").read_text("utf-8")
        )
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('036',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='036'")
    return True


class ResearchCheckpointStore:
    def __init__(self, connection, task_id, worker_id, generation):
        self.connection = connection
        self.task_id = task_id
        self.worker_id = worker_id
        self.generation = generation

    async def active(self):
        cursor = await self.connection.execute(
            "SELECT cancel_requested FROM storage.research_task WHERE task_id=%s "
            "AND status='running' AND lease_owner=%s AND attempt_count=%s "
            "AND lease_until>=now()",
            (self.task_id, self.worker_id, self.generation),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ValueError("research_lease_lost")
        if row[0]:
            raise ValueError("research_cancelled")

    async def load(self, role, input_hash, prompt_version, schema_version):
        await self.active()
        cursor = await self.connection.execute(
            "SELECT output FROM storage.research_role_checkpoint WHERE task_id=%s "
            "AND role=%s AND input_hash=%s AND prompt_version=%s AND schema_version=%s",
            (self.task_id, role, input_hash, prompt_version, schema_version),
        )
        row = await cursor.fetchone()
        return None if row is None else row[0]

    async def save(self, role, input_hash, prompt_version, schema_version, output, tokens):
        # Lock the task row in this one statement. A competing claim/cancel cannot
        # interleave between the lease check and checkpoint upsert.
        cursor = await self.connection.execute(
            "INSERT INTO storage.research_role_checkpoint "
            "(task_id,role,input_hash,prompt_version,schema_version,output,"
            "tokens,execution_generation) "
            "SELECT task_id,%s,%s,%s,%s,%s,%s,attempt_count FROM "
            "(SELECT task_id,attempt_count FROM storage.research_task WHERE task_id=%s "
            "AND status='running' AND lease_owner=%s AND attempt_count=%s "
            "AND lease_until>=now() AND NOT cancel_requested FOR UPDATE) active_task "
            "ON CONFLICT (task_id,role) DO UPDATE SET "
            "input_hash=EXCLUDED.input_hash,prompt_version=EXCLUDED.prompt_version,"
            "schema_version=EXCLUDED.schema_version,output=EXCLUDED.output,"
            "tokens=EXCLUDED.tokens,execution_generation=EXCLUDED.execution_generation,"
            "completed_at=now() RETURNING task_id",
            (
                role,
                input_hash,
                prompt_version,
                schema_version,
                Jsonb(output),
                tokens,
                self.task_id,
                self.worker_id,
                self.generation,
            ),
        )
        if await cursor.fetchone() is None:
            raise ValueError("research_lease_lost_or_cancelled")


async def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=("up", "down"))
    parser.add_argument("--allow-data-loss", action="store_true")
    args = parser.parse_args()
    if args.direction == "down" and not args.allow_data_loss:
        parser.error("down requires --allow-data-loss after a verified backup")
    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate_research_checkpoints(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(_main())
