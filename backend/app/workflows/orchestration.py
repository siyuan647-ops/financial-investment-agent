"""Step13 Prefect flows for bounded backfill, incremental scans and recovery."""

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta
from uuid import UUID

import psycopg
from prefect import flow

from app.contracts.common import SHANGHAI
from app.dataops.orchestration import (
    BatchSpec,
    FailurePolicy,
    corporate_batches,
    financial_batches,
    market_batches,
    reference_batch,
)
from app.repositories.orchestration import OrchestrationRepository, migrate_orchestration
from app.workflows.common import WorkflowFailure
from app.workflows.corporate import dividend_updates, industry_updates
from app.workflows.financial import financial_statements
from app.workflows.market import daily_market, database_url
from app.workflows.reference import security_calendar
from app.workflows.technical import technical_market_window


def quarter_ends(as_of: date, count: int):
    if not 1 <= count <= 12:
        raise ValueError("quarter_count_out_of_range")
    candidates = []
    year = as_of.year
    while len(candidates) < count:
        for month, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
            value = date(year, month, day)
            if value <= as_of:
                candidates.append(value)
                if len(candidates) == count:
                    break
        year -= 1
    return sorted(candidates)


async def _dispatch(spec: BatchSpec):
    parameters = spec.parameters
    if spec.domain == "reference":
        return await security_calendar(
            parameters["codes"],
            date.fromisoformat(parameters["start"]),
            date.fromisoformat(parameters["end"]),
        )
    if spec.domain == "market":
        return await daily_market(
            parameters["code"],
            date.fromisoformat(parameters["start"]),
            date.fromisoformat(parameters["end"]),
        )
    if spec.domain == "financial":
        return await financial_statements(
            parameters["codes"],
            [date.fromisoformat(item) for item in parameters["periods"]],
            tuple(parameters["report_types"]),
        )
    if spec.domain == "dividend":
        return await dividend_updates(parameters["codes"])
    if spec.domain == "industry":
        return await industry_updates(parameters["codes"])
    raise ValueError("unsupported_orchestration_domain")


async def execute_batches(
    specs: list[BatchSpec],
    *,
    retry_failed=False,
    force_retry=False,
    not_ready_is_transient=False,
    max_transient_retries=1,
):
    if not 0 <= max_transient_retries <= 3:
        raise ValueError("invalid_orchestration_retry_limit")
    completed, cached, locked, deferred, failures = [], [], [], [], []
    async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
        repository = OrchestrationRepository(conn)
        for spec in specs:
            retry_number = 0
            while True:
                claim = await repository.claim(
                    spec,
                    allow_retry=retry_failed or retry_number > 0,
                    force_retry=force_retry,
                )
                if claim.disposition == "cached":
                    cached.append(spec.idempotency_key)
                    await repository.release(spec)
                    break
                if claim.disposition == "locked":
                    locked.append(spec.idempotency_key)
                    break
                if claim.disposition == "deferred":
                    deferred.append(spec.idempotency_key)
                    await repository.release(spec)
                    break
                if claim.disposition == "blocked":
                    failures.append(
                        {"key": spec.idempotency_key, "reason": "manual_replay_required"}
                    )
                    await repository.release(spec)
                    break
                try:
                    result = await _dispatch(spec)
                except WorkflowFailure as exc:
                    policy = exc.policy
                    if policy.reason == "no_eligible_trading_days":
                        await repository.defer(claim.batch_id, "non_trading_window")
                        await repository.release(spec)
                        deferred.append(spec.idempotency_key)
                        break
                    if (
                        not_ready_is_transient
                        and policy.reason == "missing_dates_status_unverified"
                    ):
                        policy = FailurePolicy("provider_not_ready", "transient", True)
                    await repository.fail(claim.batch_id, policy)
                    await repository.release(spec)
                    if policy.retryable and retry_number < max_transient_retries:
                        retry_number += 1
                        await asyncio.sleep(min(2**retry_number, 4))
                        continue
                    failures.append({"key": spec.idempotency_key, "reason": policy.reason})
                    if policy.kind == "permission":
                        raise WorkflowFailure("permission_denied") from None
                    break
                except asyncio.CancelledError:
                    policy = FailurePolicy("orchestration_interrupted", "transient", True)
                    await repository.fail(claim.batch_id, policy)
                    await repository.release(spec)
                    raise
                except Exception:
                    policy = FailurePolicy("unclassified_failure", "permanent", False)
                    await repository.fail(claim.batch_id, policy)
                    await repository.release(spec)
                    raise
                else:
                    await repository.succeed(spec, claim.batch_id, result)
                    await repository.release(spec)
                    completed.append(spec.idempotency_key)
                    break
    summary = {
        "completed": completed,
        "cached": cached,
        "locked": locked,
        "deferred": deferred,
        "failures": failures,
    }
    if failures:
        raise WorkflowFailure("partial_batch_failure")
    return summary


@flow(name="historical-backfill", log_prints=False, persist_result=False, retries=0)
async def historical_backfill(codes: list[str], start: date, end: date, window_days: int = 31):
    return await execute_batches(market_batches(codes, start, end, window_days=window_days))


@flow(name="reference-refresh", log_prints=False, persist_result=False, retries=0)
async def reference_refresh(codes: list[str], scan_date: date | None = None):
    scan_date = scan_date or datetime.now(SHANGHAI).date()
    spec = reference_batch(
        codes,
        scan_date - timedelta(days=30),
        scan_date + timedelta(days=14),
        scan_on=scan_date,
    )
    return await execute_batches([spec], retry_failed=True)


@flow(name="close-incremental", log_prints=False, persist_result=False, retries=0)
async def close_incremental(
    codes: list[str], trade_date: date | None = None, revision_lookback_days: int = 7
):
    trade_date = trade_date or datetime.now(SHANGHAI).date()
    if not 1 <= revision_lookback_days <= 31:
        raise ValueError("invalid_revision_lookback")
    start = trade_date - timedelta(days=revision_lookback_days - 1)
    specs = market_batches(codes, start, trade_date, scan_on=trade_date)
    publication = await execute_batches(specs, retry_failed=True, not_ready_is_transient=True)
    technical = [await technical_market_window(code, trade_date) for code in sorted(codes)]
    return {"publication": publication, "technical": technical}


@flow(name="financial-rescan", log_prints=False, persist_result=False, retries=0)
async def financial_rescan(
    codes: list[str],
    scan_date: date | None = None,
    lookback_quarters: int = 8,
    report_types: tuple[str, ...] = ("1",),
):
    scan_date = scan_date or datetime.now(SHANGHAI).date()
    specs = financial_batches(
        codes,
        quarter_ends(scan_date, lookback_quarters),
        report_types=report_types,
        scan_on=scan_date,
    )
    publication = await execute_batches(specs, retry_failed=True)
    return {"publication": publication}


@flow(name="corporate-rescan", log_prints=False, persist_result=False, retries=0)
async def corporate_rescan(codes: list[str], scan_date: date | None = None):
    scan_date = scan_date or datetime.now(SHANGHAI).date()
    specs = corporate_batches("dividend", codes, scan_date) + corporate_batches(
        "industry", codes, scan_date
    )
    publication = await execute_batches(specs, retry_failed=True)
    return {"publication": publication}


@flow(name="manual-batch-replay", log_prints=False, persist_result=False, retries=0)
async def manual_batch_replay(batch_id: UUID):
    async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
        row = await OrchestrationRepository(conn).batch(batch_id)
    if row is None:
        raise ValueError("unknown_orchestration_batch")
    domain, partition, parameters, status, _retryable, _result, cursor_kind, cursor_value = row
    if status not in {"failed", "cancelled", "deferred"}:
        raise ValueError("batch_not_replayable")
    spec = BatchSpec(domain, partition, parameters, cursor_kind, cursor_value)
    return await execute_batches([spec], force_retry=True)


async def _run_cli(args):
    async with await psycopg.AsyncConnection.connect(database_url(), autocommit=True) as conn:
        if args.command == "migrate":
            return {"changed": await migrate_orchestration(conn, args.direction)}
        repository = OrchestrationRepository(conn)
        rows = await repository.failed_batches(retryable_only=args.retryable_only)
        return [
            {
                "batch_id": str(row[0]),
                "domain": row[1],
                "partition_key": row[2],
                "parameters": row[3],
                "reason": row[4],
                "retryable": row[5],
                "attempts": row[6],
            }
            for row in rows
        ]


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("direction", choices=("up", "down"))
    failures = commands.add_parser("failures")
    failures.add_argument("--retryable-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(_run_cli(args)), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
