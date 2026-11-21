"""Independent Step10 Prefect flows and bounded read-only local query CLI."""

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
from app.dataops.corporate import collect_dividends, collect_industries, validate_codes
from app.providers.tushare import ProviderAdapter
from app.repositories.corporate import CorporateRepository
from app.repositories.raw import RawStore
from app.workflows.common import workflow_failure
from app.workflows.market import database_url


@flow(name="dividend-updates", log_prints=False, persist_result=False, retries=0)
async def dividend_updates(codes: list[str]):
    validate_codes(codes)
    try:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            async with ProviderAdapter(load_settings()) as adapter:
                return await collect_dividends(
                    adapter,
                    CorporateRepository(conn),
                    RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw"))),
                    codes,
                )
    except Exception as exc:
        raise workflow_failure(exc) from None


@flow(name="industry-updates", log_prints=False, persist_result=False, retries=0)
async def industry_updates(codes: list[str]):
    validate_codes(codes)
    try:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            async with ProviderAdapter(load_settings()) as adapter:
                return await collect_industries(
                    adapter,
                    CorporateRepository(conn),
                    RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw"))),
                    codes,
                )
    except Exception as exc:
        raise workflow_failure(exc) from None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", choices=["dividend", "industry"], required=True)
    parser.add_argument("--codes", nargs="+", required=True)
    parser.add_argument("--query", action="store_true")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--knowledge", choices=["public", "system"], default="public")
    parser.add_argument("--on-date", type=date.fromisoformat)
    parser.add_argument("--classification", choices=["provider_basic", "SW"])
    parser.add_argument("--report-ends", nargs="*", type=date.fromisoformat)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    validate_codes(args.codes)
    if args.query:
        if len(args.codes) != 1:
            parser.error("query requires exactly one code")
        query = AsOfQuery(
            request_id="0" * 32,
            as_of=args.as_of or datetime.now(SHANGHAI),
            knowledge=args.knowledge,
        )
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            repository = CorporateRepository(conn)
            if args.domain == "dividend":
                if args.on_date or args.classification:
                    parser.error("--on-date and --classification are industry query options")
                result = await repository.corporate_actions(
                    args.codes[0],
                    query,
                    history=args.history,
                    report_ends=tuple(args.report_ends or ()),
                )
            else:
                if args.report_ends:
                    parser.error("--report-ends is a dividend query option")
                result = await repository.industries(
                    args.codes[0],
                    query,
                    history=args.history,
                    on_date=args.on_date,
                    classification=args.classification,
                )
    else:
        if any((args.history, args.as_of, args.on_date, args.classification, args.report_ends)):
            parser.error("query filters require --query")
        result = (
            await dividend_updates(args.codes)
            if args.domain == "dividend"
            else await industry_updates(args.codes)
        )
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    asyncio.run(main())
