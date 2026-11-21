"""Operator CLI for Step12 migration, quality statistics and lineage trace queries."""

import argparse
import asyncio
import json
import os
from uuid import UUID

import psycopg

from app.repositories.quality import NODE_TYPES, QualityRepository, migrate_quality


def _database_url():
    return os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)


async def run(args):
    async with await psycopg.AsyncConnection.connect(
        _database_url(), autocommit=True
    ) as connection:
        if args.command == "migrate":
            changed = await migrate_quality(connection, args.direction)
            return {"changed": changed, "direction": args.direction}
        repository = QualityRepository(connection)
        if args.command == "trace":
            return await repository.trace(args.node_type, args.reference_key)
        return await repository.statistics(UUID(args.run_id))


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("direction", choices=("up", "down"))
    trace = commands.add_parser("trace")
    trace.add_argument("--node-type", required=True, choices=sorted(NODE_TYPES))
    trace.add_argument("--reference-key", required=True)
    stats = commands.add_parser("stats")
    stats.add_argument("--run-id", required=True)
    print(json.dumps(asyncio.run(run(parser.parse_args())), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
