"""Prefect/CLI entry point for the confirmed 000001.SZ technical window."""

import argparse
import asyncio
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import psycopg
from prefect import flow

from app.contracts.business_tools import AgentRole
from app.contracts.common import SHANGHAI, AsOfQuery
from app.core.config import load_settings
from app.dataops.reference import collect_reference
from app.dataops.technical import calculate_technical_indicators, collect_ohlcv_window
from app.providers.tushare import ProviderAdapter
from app.repositories.market import MarketRepository
from app.repositories.raw import RawStore
from app.repositories.reference import ReferenceRepository
from app.repositories.technical import TechnicalRepository, migrate_technical
from app.services.business_tool_dataops import DataOpsBusinessToolFactory
from app.workflows.common import workflow_failure
from app.workflows.market import database_url


@flow(name="technical-market-window", log_prints=False, persist_result=False, retries=0)
async def technical_market_window(code: str, as_of_date: date | None = None):
    as_of_date = as_of_date or datetime.now(SHANGHAI).date()
    calendar_start = as_of_date - timedelta(days=180)
    try:
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            raw_store = RawStore(conn, Path(os.environ.get("APP_RAW_ROOT", "/data/raw")))
            reference_repository = ReferenceRepository(conn)
            query = AsOfQuery(
                request_id="0" * 32,
                as_of=datetime.now(SHANGHAI),
                knowledge="system",
            )
            securities = await reference_repository.snapshot("security", query, codes=[code])
            calendar = await reference_repository.snapshot(
                "trading_calendar",
                query,
                start=calendar_start,
                end=as_of_date,
            )
            async with ProviderAdapter(load_settings()) as adapter:
                if len(securities) != 1 or len(calendar) != (as_of_date - calendar_start).days + 1:
                    reference = await collect_reference(
                        adapter,
                        reference_repository,
                        raw_store,
                        [code],
                        calendar_start,
                        as_of_date,
                        include_aliases=False,
                    )
                    calendar = await reference_repository.snapshot(
                        "trading_calendar",
                        AsOfQuery(
                            request_id="2" * 32,
                            as_of=datetime.now(SHANGHAI),
                            knowledge="system",
                        ),
                        start=calendar_start,
                        end=as_of_date,
                    )
                else:
                    reference = {
                        "records": len(securities) + len(calendar),
                        "inserted": 0,
                    }
                trading_dates = tuple(
                    sorted(item.calendar_date for item in calendar if item.is_open)
                )
                if len(trading_dates) < 90:
                    raise ValueError("fewer_than_90_trading_days")
                ohlcv = await collect_ohlcv_window(
                    adapter,
                    MarketRepository(conn),
                    raw_store,
                    code,
                    trading_dates,
                )

            bars = await MarketRepository(conn).prices(
                code,
                trading_dates[0],
                trading_dates[-1],
                AsOfQuery(
                    request_id="1" * 32,
                    as_of=datetime.now(SHANGHAI),
                    knowledge="system",
                ),
                limit=200,
            )
            bars = bars[-90:]
            if len(bars) != 90:
                raise ValueError("published_ohlcv_window_not_90")
            calculated = calculate_technical_indicators(bars, output_days=30)
            repository = TechnicalRepository(conn)
            inserted = 0
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(600638)")
                for record in calculated:
                    inserted += await repository.publish(record)
            return {
                "ts_code": code,
                "as_of_date": as_of_date.isoformat(),
                "reference": {
                    "records": reference["records"],
                    "inserted": reference["inserted"],
                },
                "ohlcv": ohlcv,
                "price_output_days": 60,
                "warmup_days": 30,
                "indicator_output_days": 30,
                "indicator_records": len(calculated),
                "indicator_inserted": inserted,
            }
    except Exception as exc:
        raise workflow_failure(exc) from None


async def verify_technical_tools(code: str, as_of: datetime | None = None):
    as_of = as_of or datetime.now(SHANGHAI)
    async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
        registry = DataOpsBusinessToolFactory(
            SimpleNamespace(code=code, name="平安银行", exchange="SZSE"),
            as_of=as_of,
            database=conn,
        ).registry()
        stock = await registry.execute(
            {
                "name": "get_stock_data",
                "request_id": "3" * 32,
                "arguments": {
                    "code": code,
                    "as_of": as_of.isoformat(),
                    "as_of_window": as_of.date().isoformat(),
                },
            },
            AgentRole.MARKET,
        )
        indicators = await registry.execute(
            {
                "name": "get_indicators",
                "request_id": "4" * 32,
                "arguments": {
                    "code": code,
                    "as_of": as_of.isoformat(),
                    "as_of_window": as_of.date().isoformat(),
                    "indicators": [
                        "rsi",
                        "macd",
                        "macds",
                        "macdh",
                        "boll",
                        "boll_ub",
                        "boll_lb",
                        "atr",
                    ],
                },
            },
            AgentRole.MARKET,
        )
        return {
            "get_stock_data": {
                "status": stock.status.value,
                "items": len(stock.items),
                "start": stock.coverage.start.isoformat() if stock.coverage.start else None,
                "end": stock.coverage.end.isoformat() if stock.coverage.end else None,
            },
            "get_indicators": {
                "status": indicators.status.value,
                "items": len(indicators.items),
                "start": (
                    indicators.coverage.start.isoformat() if indicators.coverage.start else None
                ),
                "end": (indicators.coverage.end.isoformat() if indicators.coverage.end else None),
                "latest": indicators.items[-1] if indicators.items else None,
            },
        }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", default="000001.SZ")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--migrate", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    parser.add_argument("--verify-tools", action="store_true")
    args = parser.parse_args()
    if args.migrate:
        if args.migrate == "down" and not args.allow_data_loss:
            parser.error("down requires --allow-data-loss after a verified backup")
        async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
            changed = await migrate_technical(conn, args.migrate)
        result = {"migration": "applied" if changed else "unchanged"}
    elif args.verify_tools:
        result = await verify_technical_tools(args.code)
    else:
        result = await technical_market_window(args.code, args.as_of_date)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
