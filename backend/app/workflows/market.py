"""Prefect entry point and read-only local query CLI for Step08."""

import argparse
import asyncio
import json
import os
from datetime import date, datetime
from pathlib import Path

import psycopg
from prefect import flow

from app.contracts.common import SHANGHAI, AsOfQuery
from app.core.config import load_settings
from app.dataops.market import collect_market, validate_window
from app.providers.tushare import ProviderAdapter
from app.repositories.market import MarketRepository, migrate_market
from app.repositories.raw import RawStore
from app.workflows.common import workflow_failure


def database_url():
    return (
        load_settings()
        .database_url.get_secret_value()
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )


@flow(name="daily-market", log_prints=False, persist_result=False, retries=0)
async def daily_market(code: str, start: date, end: date):
    validate_window(code, start, end)
    settings = load_settings()
    try:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            async with ProviderAdapter(settings) as adapter:
                return await collect_market(
                    adapter,
                    MarketRepository(conn),
                    RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw"))),
                    code,
                    start,
                    end,
                )
    except Exception as exc:
        raise workflow_failure(exc) from None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--query", action="store_true")
    parser.add_argument("--migrate", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.migrate:
        if args.migrate == "down" and not args.allow_data_loss:
            parser.error("down requires --allow-data-loss after a verified backup")
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            await migrate_market(conn, args.migrate)
        return
    if not args.code or args.start is None or args.end is None:
        parser.error("--code, --start and --end are required")
    if args.query:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            result = await MarketRepository(conn).market(
                args.code,
                args.start,
                args.end,
                AsOfQuery(request_id="0" * 32, as_of=datetime.now(SHANGHAI)),
            )
    else:
        result = await daily_market(args.code, args.start, args.end)
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    asyncio.run(main())
