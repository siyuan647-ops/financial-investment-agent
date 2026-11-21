"""Bounded financial statement ingestion; disclosure dates never prove revision timing."""

import asyncio
import hashlib
from datetime import date, datetime
from uuid import uuid4

from app.contracts.common import SHANGHAI, AsOfQuery, Availability, Provenance
from app.contracts.domain import REPORT_TYPES, FinancialFact, FinancialPeriod
from app.contracts.mapping import NUMERIC_FIELDS, content_version, map_measure, upstream_date
from app.dataops.market import decimal_rows
from app.dataops.reference import PROVIDER, security_key
from app.providers.tushare import ProviderError

APIS = ("income", "balancesheet", "cashflow")
COMPANIES = {
    "1": "industrial",
    "2": "bank",
    "3": "insurance",
    "4": "securities",
    "7": "other_finance",
}
FIELDS = {
    api: "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,update_flag,"
    + ",".join(f.upstream for f in NUMERIC_FIELDS if f.api == api)
    for api in APIS
}


class FinancialError(ValueError):
    """Safe fixed reason code."""


def validate_request(codes, periods, report_types):
    if not 1 <= len(codes) <= 3 or len(set(codes)) != len(codes):
        raise FinancialError("require_one_to_three_unique_securities")
    for code in codes:
        security_key(code)
    if not 1 <= len(periods) <= 12 or len(set(periods)) != len(periods):
        raise FinancialError("require_one_to_twelve_unique_periods")
    for period in periods:
        if type(period) is not date or (period.month, period.day) not in {
            (3, 31),
            (6, 30),
            (9, 30),
            (12, 31),
        }:
            raise FinancialError("quarter_end_required")
    if (
        not report_types
        or len(set(report_types)) != len(report_types)
        or any(value not in REPORT_TYPES for value in report_types)
    ):
        raise FinancialError("known_unique_report_types_required")
    if len(codes) * len(periods) * len(report_types) * 3 > 108:
        raise FinancialError("request_budget_exceeds_108")


def normalize(api, row, provenance):
    report_type = row["report_type"]
    if report_type not in REPORT_TYPES or row["comp_type"] not in COMPANIES:
        raise FinancialError("unknown_statement_semantics")
    if row["update_flag"] not in {None, "0", "1"}:
        raise FinancialError("unknown_update_flag")
    scope, flow_basis, revision = REPORT_TYPES[report_type]
    end = upstream_date(row["end_date"])
    if (end.month, end.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        raise FinancialError("quarter_end_required")
    basis = (
        "instant"
        if api == "balancesheet"
        else "quarter"
        if flow_basis == "quarter"
        else "annual"
        if end.month == 12
        else "ytd"
    )
    start = (
        None
        if basis == "instant"
        else date(end.year, end.month - 2 if basis == "quarter" else 1, 1)
    )
    announced = upstream_date(row["ann_date"]) if row["ann_date"] else None
    actual = upstream_date(row["f_ann_date"]) if row["f_ann_date"] else None
    for day in (announced, actual):
        if day and (day < end or day > provenance.timing.fetched_at.date()):
            raise FinancialError("invalid_disclosure_date")
    timing = Availability(
        fetched_at=provenance.timing.fetched_at,
        available_at=provenance.timing.fetched_at,
        disclosed_on=actual or announced,
        basis="observed",
    )
    provenance = provenance.model_copy(update={"timing": timing})
    return [
        FinancialFact(
            security=security_key(row["ts_code"]),
            metric=spec.target,
            statement=api,
            period=FinancialPeriod(start=start, end=end, basis=basis),
            scope=scope,
            company_type=COMPANIES[row["comp_type"]],
            report_type=report_type,
            update_flag=row["update_flag"],
            announced_on=announced,
            actual_announced_on=actual,
            revision=revision,
            amount=map_measure(api, spec.upstream, row),
            provenance=provenance,
        )
        for spec in NUMERIC_FIELDS
        if spec.api == api
    ]


def validate_response(api, rows, code, period, report_type):
    if len(rows) >= 100:
        raise FinancialError("response_limit_or_truncation_unverified")
    unique = {}
    for row in rows:
        if not set(FIELDS[api].split(",")) <= row.keys():
            raise FinancialError("required_projection_missing")
        if row["ts_code"] != code or row["end_date"] != period.strftime("%Y%m%d"):
            raise FinancialError("source_filter_ignored")
        if row["report_type"] != report_type:
            raise FinancialError("report_type_filter_ignored")
        key = tuple(
            row[name]
            for name in (
                "ts_code",
                "end_date",
                "report_type",
                "comp_type",
                "ann_date",
                "f_ann_date",
                "update_flag",
            )
        )
        if key in unique and content_version(unique[key]) != content_version(row):
            raise FinancialError("conflicting_same_revision")
        unique[key] = row
    return list(unique.values())


async def collect_financial(
    adapter, repository, raw_store, codes, periods, report_types=("1",), *, timeout_seconds=600
):
    validate_request(codes, periods, report_types)
    run_id = await repository.create_run(
        "step09:" + uuid4().hex,
        {
            "codes": list(codes),
            "periods": [p.isoformat() for p in periods],
            "report_types": list(report_types),
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    active_api = None
    staged, batches = [], []
    try:
        async with asyncio.timeout(timeout_seconds):
            securities = await repository.snapshot(
                "security",
                AsOfQuery(request_id=run_id.hex, as_of=datetime.now(SHANGHAI)),
                codes=codes,
            )
            if {s.security.ts_code for s in securities} != set(codes):
                raise FinancialError("security_reference_required")
            for code in codes:
                for period in sorted(periods):
                    for report_type in report_types:
                        for api in APIS:
                            active_api = api
                            params = {
                                "ts_code": code,
                                "period": period.strftime("%Y%m%d"),
                                "report_type": report_type,
                            }
                            response = await adapter.query(api, params, FIELDS[api])
                            fetched = datetime.now(SHANGHAI)
                            if not response.raw_content:
                                raise FinancialError("original_response_bytes_required")
                            raw_id = await raw_store.save(
                                run_id=run_id,
                                provider=PROVIDER,
                                source_api=api,
                                parameters={**params, "fields": FIELDS[api]},
                                fetched_at=fetched,
                                content=response.raw_content,
                            )
                            try:
                                rows = validate_response(
                                    api,
                                    decimal_rows(response.raw_content),
                                    code,
                                    period,
                                    report_type,
                                )
                                for row in rows:
                                    provenance = Provenance(
                                        provider=PROVIDER,
                                        source_api=api,
                                        source_ref=str(raw_id),
                                        raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                                        request_id=run_id.hex,
                                        data_version="0" * 64,
                                        mapping_version="step09-v1",
                                        quality="valid",
                                        timing=Availability(
                                            fetched_at=fetched,
                                            available_at=fetched,
                                            basis="observed",
                                        ),
                                    )
                                    staged.extend(
                                        (fact, raw_id) for fact in normalize(api, row, provenance)
                                    )
                            except (ValueError, KeyError, TypeError) as exc:
                                reason = (
                                    str(exc)
                                    if isinstance(exc, FinancialError)
                                    else "invalid_financial_record"
                                )
                                await repository.add_quality_issue(
                                    run_id=run_id,
                                    raw_id=raw_id,
                                    rule="financial_response_invalid",
                                    severity="blocking",
                                    detail={"source_api": api, "reason": reason, **params},
                                )
                                raise FinancialError("invalid_financial_response") from None
                            batches.append(
                                {
                                    **params,
                                    "source_api": api,
                                    "rows": len(rows),
                                    "status": "received" if rows else "no_data_unverified",
                                }
                            )
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600609)")
                inserted = 0
                for fact, raw_id in staged:
                    inserted += await repository.publish_fact(fact, raw_id)
                result = {
                    "run_id": str(run_id),
                    "inserted": inserted,
                    "records": len(staged),
                    "batches": batches,
                    "coverage_complete": all(b["rows"] for b in batches),
                    "availability": "observed_revision_time_only",
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else (str(exc) if isinstance(exc, FinancialError) else type(exc).__name__)
        )
        await repository.transition_run(
            run_id,
            "running",
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            result={"reason": reason, "source_api": active_api},
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise FinancialError(f"financial_pipeline_failed:{reason}") from None
