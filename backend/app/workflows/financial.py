"""Prefect financial statements entry point and bounded read-only query CLI."""

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
from app.dataops.financial import collect_financial, validate_request
from app.providers.tushare import ProviderAdapter
from app.repositories.financial import FinancialRepository
from app.repositories.raw import RawStore
from app.workflows.common import workflow_failure
from app.workflows.market import database_url


@flow(name="financial-statements", log_prints=False, persist_result=False, retries=0)
async def financial_statements(
    codes: list[str], periods: list[date], report_types: tuple[str, ...] = ("1",)
):
    validate_request(codes, periods, report_types)
    try:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            async with ProviderAdapter(load_settings()) as adapter:
                return await collect_financial(
                    adapter,
                    FinancialRepository(conn),
                    RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw"))),
                    codes,
                    periods,
                    report_types,
                )
    except Exception as exc:
        raise workflow_failure(exc) from None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", nargs="+", required=True)
    parser.add_argument("--periods", nargs="+", type=date.fromisoformat, required=True)
    parser.add_argument("--report-types", nargs="+", default=["1"])
    parser.add_argument("--query", action="store_true")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--knowledge", choices=["public", "system"], default="public")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    validate_request(args.codes, args.periods, args.report_types)
    if args.query:
        if len(args.codes) != 1 or len(args.report_types) != 1:
            parser.error("query requires exactly one code and one report type")
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            result = await FinancialRepository(conn).financials(
                args.codes[0],
                args.periods,
                AsOfQuery(
                    request_id="0" * 32,
                    as_of=args.as_of or datetime.now(SHANGHAI),
                    knowledge=args.knowledge,
                ),
                report_type=args.report_types[0],
                history=args.history,
            )
    else:
        if args.history or args.as_of:
            parser.error("--history and --as-of require --query")
        result = await financial_statements(args.codes, args.periods, tuple(args.report_types))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    asyncio.run(main())
