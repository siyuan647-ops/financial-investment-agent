"""Checksummed, transactional Step06 migration; never runs on API startup."""

import argparse
import asyncio
import hashlib
import os
from pathlib import Path

import psycopg

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"


async def migrate(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "006_storage.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        await connection.execute(
            "CREATE TABLE IF NOT EXISTS public.schema_migration "
            "(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz "
            "NOT NULL DEFAULT now())"
        )
        cursor = await connection.execute(
            "SELECT checksum FROM public.schema_migration WHERE version = '006'"
        )
        existing = await cursor.fetchone()
        if existing and existing[0] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up" and existing) or (direction == "down" and not existing):
            return False
        if direction == "down":
            cursor = await connection.execute(
                "SELECT version FROM public.schema_migration WHERE version='007'"
            )
            if await cursor.fetchone():
                raise ValueError("rollback_step07_first")
        await connection.execute((MIGRATIONS / f"006_storage.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version, checksum) VALUES ('006', %s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version = '006'")
    return True


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    args = parser.parse_args()
    if args.direction == "down" and not args.allow_data_loss:
        parser.error("down requires --allow-data-loss after a verified backup")
    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(main())
