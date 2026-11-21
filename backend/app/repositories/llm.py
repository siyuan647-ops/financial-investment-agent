"""Durable model-invocation metadata without prompts, responses or credentials."""

import argparse
import asyncio
import hashlib
import os
import re
from decimal import Decimal
from uuid import UUID

from app.contracts.llm import ModelPurpose, TokenUsage
from app.repositories.migrate import MIGRATIONS

FINAL_STATES = {"succeeded", "failed", "cancelled"}
SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,79}$")


async def migrate_llm(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "016_llm.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('014','016')"
        )
        versions = dict(await cursor.fetchall())
        if "014" not in versions:
            raise ValueError("step14_migration_required")
        if "016" in versions and versions["016"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("016" in versions):
            return False
        await connection.execute((MIGRATIONS / f"016_llm.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('016',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='016'")
    return True


class ModelInvocationRepository:
    def __init__(self, connection):
        self.connection = connection

    async def start(
        self,
        *,
        invocation_id: UUID,
        request_id: str,
        task_id: str | None,
        purpose: ModelPurpose,
        model: str,
        prompt_version: str,
        stream: bool,
    ) -> None:
        await self.connection.execute(
            "INSERT INTO storage.model_invocation "
            "(invocation_id,request_id,task_id,purpose,provider,model,prompt_version,stream) "
            "VALUES (%s,%s,%s,%s,'qwen',%s,%s,%s)",
            (
                invocation_id,
                request_id,
                task_id,
                purpose.value,
                model,
                prompt_version,
                stream,
            ),
        )

    async def finish(
        self,
        *,
        invocation_id: UUID,
        status: str,
        attempts: int,
        duration_ms: int,
        usage: TokenUsage | None,
        estimated_cost: Decimal | None,
        cost_currency: str | None,
        error_kind: str | None,
    ) -> None:
        if status not in FINAL_STATES:
            raise ValueError("invalid_invocation_status")
        if error_kind is not None and not SAFE_CODE.fullmatch(error_kind):
            raise ValueError("invalid_invocation_error_kind")
        if (estimated_cost is None) != (cost_currency is None):
            raise ValueError("invalid_invocation_cost")
        values = (
            (None, None, None)
            if usage is None
            else (
                usage.input_tokens,
                usage.output_tokens,
                usage.total_tokens,
            )
        )
        cursor = await self.connection.execute(
            "UPDATE storage.model_invocation SET status=%s,attempts=%s,duration_ms=%s,"
            "input_tokens=%s,output_tokens=%s,total_tokens=%s,estimated_cost=%s,"
            "cost_currency=%s,error_kind=%s,completed_at=now() "
            "WHERE invocation_id=%s AND status='running' RETURNING invocation_id",
            (
                status,
                attempts,
                duration_ms,
                *values,
                estimated_cost,
                cost_currency,
                error_kind,
                invocation_id,
            ),
        )
        if await cursor.fetchone() is None:
            raise ValueError("invocation_state_conflict")


async def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=("up", "down"))
    parser.add_argument("--allow-data-loss", action="store_true")
    args = parser.parse_args()
    if args.direction == "down" and not args.allow_data_loss:
        parser.error("down requires --allow-data-loss after a verified backup")
    import psycopg

    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate_llm(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(_main())
