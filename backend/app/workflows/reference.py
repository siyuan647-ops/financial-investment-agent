"""First business Prefect flow; credentials never become flow parameters."""

import argparse
import asyncio
import json
import os
from datetime import date
from pathlib import Path

import psycopg
from prefect import flow

from app.core.config import load_settings
from app.dataops.reference import collect_reference, validate_window
from app.providers.tushare import ProviderAdapter
from app.repositories.raw import RawStore
from app.repositories.reference import ReferenceRepository, migrate_reference
from app.workflows.common import workflow_failure


@flow(name="security-calendar", log_prints=False, persist_result=False, retries=0)
async def security_calendar(codes: list[str], start: date, end: date, include_aliases: bool = True):
    validate_window(codes, start, end)
    settings = load_settings()
    url = settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    try:
        async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
            async with ProviderAdapter(settings) as adapter:
                return await collect_reference(
                    adapter,
                    ReferenceRepository(connection),
                    RawStore(connection, Path(os.environ.get("APP_RAW_ROOT", "/data/raw"))),
                    codes,
                    start,
                    end,
                    include_aliases=include_aliases,
                )
    except Exception as exc:
        # Detailed safe run status is in PG; raw driver messages can contain secrets.
        raise workflow_failure(exc) from None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", nargs="+")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--without-aliases", action="store_true")
    parser.add_argument("--migrate", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.migrate:
        if args.migrate == "down" and not args.allow_data_loss:
            parser.error("down requires --allow-data-loss after a verified backup")
        settings = load_settings()
        url = settings.database_url.get_secret_value().replace(
            "postgresql+psycopg://", "postgresql://", 1
        )
        async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
            await migrate_reference(connection, args.migrate)
        return
    if not args.codes or args.start is None or args.end is None:
        parser.error("--codes, --start and --end are required")
    result = await security_calendar(args.codes, args.start, args.end, not args.without_aliases)
    if args.output:
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "coverage"}))


if __name__ == "__main__":
    asyncio.run(main())
