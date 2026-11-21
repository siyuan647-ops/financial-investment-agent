"""Prefect and CLI entry points for five-domain fundamental ingestion."""

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
from app.dataops.fundamental_domains import (
    collect_fundamental_analysis_window,
    collect_fundamental_domains,
    collect_profitability_window,
)
from app.providers.tushare import ProviderAdapter
from app.repositories.fundamental import FundamentalRepository, migrate_fundamental
from app.repositories.raw import RawStore
from app.workflows.common import workflow_failure
from app.workflows.market import database_url


def latest_completed_quarter(day: date) -> date:
    candidates = (
        date(day.year, 3, 31),
        date(day.year, 6, 30),
        date(day.year, 9, 30),
        date(day.year, 12, 31),
    )
    earlier = [candidate for candidate in candidates if candidate <= day]
    if earlier:
        return earlier[-1]
    return date(day.year - 1, 12, 31)


async def refresh_fundamental_agent_domains(
    adapter,
    repository,
    raw_store,
    code: str,
    period: date,
    as_of_date: date,
):
    current = await collect_fundamental_domains(
        adapter, repository, raw_store, code, period, as_of_date
    )
    profitability = await collect_profitability_window(
        adapter, repository, raw_store, code, as_of_date
    )
    analysis = await collect_fundamental_analysis_window(
        adapter, repository, raw_store, code, as_of_date
    )
    return {
        "ts_code": code,
        "current_domains": current,
        "profitability_window": profitability,
        "analysis_window": analysis,
    }


@flow(name="fundamental-five-domains", log_prints=False, persist_result=False, retries=0)
async def fundamental_five_domains(
    codes: list[str], period: date | None = None, market_date: date | None = None
):
    today = datetime.now(SHANGHAI).date()
    period = period or latest_completed_quarter(today)
    market_date = market_date or today
    window_date = market_date
    if not 1 <= len(codes) <= 30 or len(codes) != len(set(codes)):
        raise ValueError("require_one_to_thirty_unique_securities")
    try:
        results = []
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            repository = FundamentalRepository(conn)
            raw_store = RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw")))
            async with ProviderAdapter(load_settings()) as adapter:
                for code in codes:
                    results.append(
                        await refresh_fundamental_agent_domains(
                            adapter,
                            repository,
                            raw_store,
                            code,
                            period,
                            window_date,
                        )
                    )
        return {
            "period": period.isoformat(),
            "as_of_date": window_date.isoformat(),
            "securities": results,
        }
    except Exception as exc:
        raise workflow_failure(exc) from None


@flow(name="profitability-history-window", log_prints=False, persist_result=False, retries=0)
async def profitability_history_window(codes: list[str], as_of_date: date | None = None):
    as_of_date = as_of_date or datetime.now(SHANGHAI).date()
    if not 1 <= len(codes) <= 30 or len(codes) != len(set(codes)):
        raise ValueError("require_one_to_thirty_unique_securities")
    try:
        results = []
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            repository = FundamentalRepository(conn)
            raw_store = RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw")))
            async with ProviderAdapter(load_settings()) as adapter:
                for code in codes:
                    results.append(
                        await collect_profitability_window(
                            adapter, repository, raw_store, code, as_of_date
                        )
                    )
        return {"as_of_date": as_of_date.isoformat(), "securities": results}
    except Exception as exc:
        raise workflow_failure(exc) from None


@flow(name="fundamental-analysis-window", log_prints=False, persist_result=False, retries=0)
async def fundamental_analysis_window(codes: list[str], as_of_date: date | None = None):
    as_of_date = as_of_date or datetime.now(SHANGHAI).date()
    if not 1 <= len(codes) <= 30 or len(codes) != len(set(codes)):
        raise ValueError("require_one_to_thirty_unique_securities")
    try:
        results = []
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            repository = FundamentalRepository(conn)
            raw_store = RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw")))
            async with ProviderAdapter(load_settings()) as adapter:
                for code in codes:
                    results.append(
                        await collect_fundamental_analysis_window(
                            adapter, repository, raw_store, code, as_of_date
                        )
                    )
        return {"as_of_date": as_of_date.isoformat(), "securities": results}
    except Exception as exc:
        raise workflow_failure(exc) from None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--migrate", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    parser.add_argument("--codes", nargs="+")
    parser.add_argument("--period", type=date.fromisoformat)
    parser.add_argument("--market-date", type=date.fromisoformat)
    parser.add_argument("--profitability-window", action="store_true")
    parser.add_argument("--fundamental-analysis-window", action="store_true")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument(
        "--query-domain",
        choices=["company_profile", "profitability", "balance_sheet", "cash_flow", "auxiliary"],
    )
    parser.add_argument("--record-type")
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.migrate:
        if args.migrate == "down" and not args.allow_data_loss:
            parser.error("down requires --allow-data-loss after a verified backup")
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            changed = await migrate_fundamental(conn, args.migrate)
        result = {"migration": "applied" if changed else "unchanged"}
    elif args.query_domain:
        if not args.codes or len(args.codes) != 1:
            parser.error("query requires exactly one code")
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            records = await FundamentalRepository(conn).records(
                args.query_domain,
                args.codes[0],
                AsOfQuery(
                    request_id="0" * 32,
                    as_of=args.as_of or datetime.now(SHANGHAI),
                    knowledge="public",
                ),
                record_type=args.record_type,
            )
        result = [record.model_dump(mode="json") for record in records]
    elif args.fundamental_analysis_window:
        if not args.codes:
            parser.error("--codes is required")
        result = await fundamental_analysis_window(args.codes, args.as_of_date)
    elif args.profitability_window:
        if not args.codes:
            parser.error("--codes is required")
        result = await profitability_history_window(args.codes, args.as_of_date)
    else:
        if not args.codes:
            parser.error("--codes is required")
        result = await fundamental_five_domains(args.codes, args.period, args.market_date)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
