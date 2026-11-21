"""Step10 dividend and industry ingestion with conservative history semantics."""

import asyncio
import hashlib
from collections import Counter
from datetime import datetime
from uuid import uuid4

from app.contracts.common import SHANGHAI, AsOfQuery, Availability, Provenance
from app.contracts.domain import CorporateAction, IndustryMembership
from app.contracts.mapping import content_version, map_measure, upstream_date
from app.dataops.market import decimal_rows
from app.dataops.reference import PROVIDER, optional_date, security_key
from app.providers.tushare import ProviderError

DIVIDEND_FIELDS = (
    "ts_code,end_date,ann_date,div_proc,cash_div,cash_div_tax,stk_div,"
    "record_date,ex_date,pay_date,imp_ann_date"
)
BASIC_FIELDS = "ts_code,industry"
CATALOG_FIELDS = "index_code,industry_name,parent_code,level,industry_code,is_pub,src"
MEMBER_FIELDS = "ts_code,l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,in_date,out_date,is_new"
SW_VERSION = "SW2021"
BASIC_VERSION = "unversioned_current_snapshot"
STAGES = {
    "预案": "proposed",
    "预披露": "proposed",
    "董事会预案": "proposed",
    "股东大会预案": "proposed",
    "股东大会通过": "approved",
    "决案": "approved",
    "实施": "implemented",
    "实施方案": "implemented",
    "取消": "cancelled",
    "终止": "cancelled",
}


class CorporateError(ValueError):
    """Safe fixed reason code for Step10 workflows."""


def validate_codes(codes):
    if not 1 <= len(codes) <= 3 or len(set(codes)) != len(codes):
        raise CorporateError("require_one_to_three_unique_securities")
    for code in codes:
        security_key(code)


def _provenance(run_id, raw_id, content, fetched, api, *, quality="valid"):
    return Provenance(
        provider=PROVIDER,
        source_api=api,
        source_ref=str(raw_id),
        raw_sha256=hashlib.sha256(content).hexdigest(),
        request_id=run_id.hex,
        data_version="0" * 64,
        mapping_version="step10-v1",
        quality=quality,
        freshness="unknown",
        timing=Availability(fetched_at=fetched, available_at=fetched, basis="observed"),
    )


def dividend_event_id(code, report_end):
    return "dividend:" + content_version(
        {"provider": PROVIDER, "ts_code": code, "report_end": report_end}
    )


def normalize_dividend(row, provenance):
    raw_stage = row["div_proc"]
    stage = STAGES.get(raw_stage, "unknown")
    report_end = upstream_date(row["end_date"])
    announced = optional_date(row["ann_date"])
    implementation = optional_date(row["imp_ann_date"])
    if announced and announced > provenance.timing.fetched_at.date():
        raise CorporateError("invalid_dividend_announcement_date")
    if implementation and implementation > provenance.timing.fetched_at.date():
        raise CorporateError("invalid_dividend_implementation_date")
    if implementation and announced and implementation < announced:
        raise CorporateError("implementation_before_announcement")
    if stage == "implemented" and implementation is None:
        # The source stage is retained, but the absent implementation announcement is explicit.
        provenance = provenance.model_copy(update={"quality": "unverified"})
    if stage == "unknown":
        provenance = provenance.model_copy(update={"quality": "unverified"})
    return CorporateAction(
        provenance=provenance,
        security=security_key(row["ts_code"]),
        event_id=dividend_event_id(row["ts_code"], report_end.isoformat()),
        report_end=report_end,
        announced_on=announced,
        stage=stage,
        raw_stage=raw_stage,
        cash_after_tax=map_measure("dividend", "cash_div", row),
        cash_before_tax=map_measure("dividend", "cash_div_tax", row),
        stock_distribution=map_measure("dividend", "stk_div", row),
        record_date=optional_date(row["record_date"]),
        ex_date=optional_date(row["ex_date"]),
        pay_date=optional_date(row["pay_date"]),
    )


def validate_dividend_response(rows, code):
    if len(rows) >= 2000:
        raise CorporateError("dividend_response_limit_or_truncation_unverified")
    required = set(DIVIDEND_FIELDS.split(","))
    unique = {}
    for row in rows:
        if not required <= row.keys():
            raise CorporateError("required_projection_missing")
        if row["ts_code"] != code:
            raise CorporateError("security_filter_ignored")
        upstream_date(row["end_date"])
        # Multiple source rows in one stage/date slot are retained as source revisions.
        # Exact duplicates collapse; distinct variants must remain queryable, not be rejected.
        unique[content_version(row)] = row
    return list(unique.values())


def validate_catalog(rows, level):
    if len(rows) >= 2000:
        raise CorporateError("industry_catalog_limit_or_truncation_unverified")
    required = set(CATALOG_FIELDS.split(","))
    result = {}
    for row in rows:
        if not required <= row.keys() or row["level"] != level:
            raise CorporateError("invalid_industry_catalog_projection")
        if row["src"] not in {"SW", SW_VERSION}:
            raise CorporateError("unexpected_industry_catalog_source")
        if not row["index_code"] or not row["industry_code"] or not row["industry_name"]:
            raise CorporateError("incomplete_industry_catalog_row")
        previous = result.get(row["index_code"])
        if previous and content_version(previous) != content_version(row):
            raise CorporateError("conflicting_industry_catalog_code")
        result[row["index_code"]] = row
    if not result:
        raise CorporateError("empty_industry_catalog_unverified")
    return result


def validate_hierarchy(catalogs):
    for child_level, parent_level in (("L2", "L1"), ("L3", "L2")):
        parent_ids = {
            value
            for row in catalogs[parent_level].values()
            for value in (row["index_code"], row["industry_code"])
        }
        if any(row["parent_code"] not in parent_ids for row in catalogs[child_level].values()):
            raise CorporateError("industry_catalog_parent_mismatch")


def validate_basic_response(rows, code):
    required = set(BASIC_FIELDS.split(","))
    if len(rows) > 1 or any(not required <= row.keys() for row in rows):
        raise CorporateError("invalid_basic_industry_projection")
    if rows and (rows[0]["ts_code"] != code or not rows[0]["industry"]):
        raise CorporateError("invalid_basic_industry_row")
    return rows


def normalize_basic_industry(row, provenance):
    return IndustryMembership(
        provenance=provenance,
        security=security_key(row["ts_code"]),
        classification="provider_basic",
        classification_version=BASIC_VERSION,
        level=None,
        industry_code=None,
        industry_name=row["industry"],
        effective_from=None,
        effective_to=None,
        history="current_snapshot",
    )


def validate_member_response(rows, code, is_new, catalogs):
    if len(rows) >= 2000:
        raise CorporateError("industry_member_limit_or_truncation_unverified")
    required = set(MEMBER_FIELDS.split(","))
    unique = {}
    for row in rows:
        if not required <= row.keys():
            raise CorporateError("required_projection_missing")
        if row["ts_code"] != code or row["is_new"] != is_new:
            raise CorporateError("industry_member_filter_ignored")
        start, end = optional_date(row["in_date"]), optional_date(row["out_date"])
        if start is None or (end and end < start):
            raise CorporateError("invalid_industry_membership_interval")
        if is_new == "Y" and end is not None:
            raise CorporateError("current_industry_has_end_date")
        if is_new == "N" and end is None:
            raise CorporateError("historical_industry_missing_end_date")
        for number in (1, 2, 3):
            catalog = catalogs[f"L{number}"]
            code_value, name = row[f"l{number}_code"], row[f"l{number}_name"]
            if code_value not in catalog or catalog[code_value]["industry_name"] != name:
                raise CorporateError("membership_not_in_versioned_hierarchy")
        slot = (row["ts_code"], row["in_date"], row["out_date"], row["l3_code"])
        if slot in unique and content_version(unique[slot]) != content_version(row):
            raise CorporateError("conflicting_industry_membership")
        unique[slot] = row
    if is_new == "Y" and len(unique) > 1:
        raise CorporateError("multiple_current_sw_memberships")
    return list(unique.values())


def normalize_memberships(row, provenance):
    return [
        IndustryMembership(
            provenance=provenance,
            security=security_key(row["ts_code"]),
            classification="SW",
            classification_version=SW_VERSION,
            level=number,
            industry_code=row[f"l{number}_code"],
            industry_name=row[f"l{number}_name"],
            effective_from=upstream_date(row["in_date"]),
            effective_to=optional_date(row["out_date"]),
            history="interval_unverified",
        )
        for number in (1, 2, 3)
    ]


async def _require_securities(repository, codes, run_id):
    securities = await repository.snapshot(
        "security", AsOfQuery(request_id=run_id.hex, as_of=datetime.now(SHANGHAI)), codes=codes
    )
    if {item.security.ts_code for item in securities} != set(codes):
        raise CorporateError("security_reference_required")


async def _fail(repository, run_id, exc, active_api):
    reason = (
        exc.kind
        if isinstance(exc, ProviderError)
        else (str(exc) if isinstance(exc, CorporateError) else type(exc).__name__)
    )
    await repository.transition_run(
        run_id,
        "running",
        "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
        result={"reason": reason, "source_api": active_api},
    )
    if isinstance(exc, asyncio.CancelledError):
        raise exc
    raise CorporateError(f"corporate_pipeline_failed:{reason}") from None


async def collect_dividends(adapter, repository, raw_store, codes, *, timeout_seconds=300):
    validate_codes(codes)
    run_id = await repository.create_run(
        "step10-dividend:" + uuid4().hex, {"ts_code": ",".join(codes)}
    )
    await repository.transition_run(run_id, "queued", "running")
    active_api = "dividend"
    staged, batches = [], []
    try:
        async with asyncio.timeout(timeout_seconds):
            await _require_securities(repository, codes, run_id)
            for code in codes:
                params = {"ts_code": code, "limit": 2000, "offset": 0}
                response = await adapter.query(active_api, params, DIVIDEND_FIELDS)
                fetched = datetime.now(SHANGHAI)
                if not response.raw_content:
                    raise CorporateError("original_response_bytes_required")
                raw_id = await raw_store.save(
                    run_id=run_id,
                    provider=PROVIDER,
                    source_api=active_api,
                    parameters={**params, "fields": DIVIDEND_FIELDS},
                    fetched_at=fetched,
                    content=response.raw_content,
                )
                try:
                    rows = validate_dividend_response(decimal_rows(response.raw_content), code)
                    provenance = _provenance(
                        run_id, raw_id, response.raw_content, fetched, active_api
                    )
                    staged.extend((normalize_dividend(row, provenance), raw_id) for row in rows)
                except (ValueError, KeyError, TypeError) as exc:
                    reason = (
                        str(exc) if isinstance(exc, CorporateError) else "invalid_dividend_record"
                    )
                    await repository.add_quality_issue(
                        run_id=run_id,
                        raw_id=raw_id,
                        rule="dividend_response_invalid",
                        severity="blocking",
                        detail={"source_api": active_api, "ts_code": code, "reason": reason},
                    )
                    raise CorporateError("invalid_dividend_response") from None
                batches.append(
                    {
                        "ts_code": code,
                        "rows": len(rows),
                        "status": "received" if rows else "no_data_unverified",
                    }
                )
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600610)")
                inserted = 0
                for action, raw_id in sorted(
                    staged,
                    key=lambda item: (
                        item[0].report_end,
                        item[0].announced_on or item[0].report_end,
                        item[0].stage,
                    ),
                ):
                    inserted += await repository.publish_action(action, raw_id)
                result = {
                    "run_id": str(run_id),
                    "inserted": inserted,
                    "records": len(staged),
                    "events": len({item.event_id for item, _ in staged}),
                    "stages": dict(Counter(item.stage for item, _ in staged)),
                    "batches": batches,
                    "coverage_complete": all(batch["rows"] for batch in batches),
                    "payment_status": "unverified",
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        await _fail(repository, run_id, exc, active_api)


async def collect_industries(adapter, repository, raw_store, codes, *, timeout_seconds=300):
    validate_codes(codes)
    run_id = await repository.create_run(
        "step10-industry:" + uuid4().hex, {"ts_code": ",".join(codes)}
    )
    await repository.transition_run(run_id, "queued", "running")
    active_api = None
    staged, batches, catalogs = [], [], {}
    try:
        async with asyncio.timeout(timeout_seconds):
            await _require_securities(repository, codes, run_id)
            for level in ("L1", "L2", "L3"):
                active_api = "index_classify"
                params = {"level": level, "src": SW_VERSION, "limit": 2000, "offset": 0}
                response = await adapter.query(active_api, params, CATALOG_FIELDS)
                fetched = datetime.now(SHANGHAI)
                if not response.raw_content:
                    raise CorporateError("original_response_bytes_required")
                raw_id = await raw_store.save(
                    run_id=run_id,
                    provider=PROVIDER,
                    source_api=active_api,
                    parameters={**params, "fields": CATALOG_FIELDS},
                    fetched_at=fetched,
                    content=response.raw_content,
                )
                try:
                    catalogs[level] = validate_catalog(decimal_rows(response.raw_content), level)
                except (ValueError, KeyError, TypeError) as exc:
                    reason = (
                        str(exc) if isinstance(exc, CorporateError) else "invalid_industry_catalog"
                    )
                    await repository.add_quality_issue(
                        run_id=run_id,
                        raw_id=raw_id,
                        rule="industry_catalog_invalid",
                        severity="blocking",
                        detail={"level": level, "src": SW_VERSION, "reason": reason},
                    )
                    raise CorporateError("invalid_industry_catalog") from None
            validate_hierarchy(catalogs)
            for code in codes:
                active_api = "stock_basic"
                params = {"ts_code": code}
                response = await adapter.query(active_api, params, BASIC_FIELDS)
                fetched = datetime.now(SHANGHAI)
                if not response.raw_content:
                    raise CorporateError("original_response_bytes_required")
                raw_id = await raw_store.save(
                    run_id=run_id,
                    provider=PROVIDER,
                    source_api=active_api,
                    parameters={**params, "fields": BASIC_FIELDS},
                    fetched_at=fetched,
                    content=response.raw_content,
                )
                try:
                    rows = validate_basic_response(decimal_rows(response.raw_content), code)
                    provenance = _provenance(
                        run_id, raw_id, response.raw_content, fetched, active_api
                    )
                    staged.extend(
                        (normalize_basic_industry(row, provenance), raw_id) for row in rows
                    )
                except (ValueError, KeyError, TypeError) as exc:
                    reason = (
                        str(exc)
                        if isinstance(exc, CorporateError)
                        else "invalid_basic_industry_record"
                    )
                    await repository.add_quality_issue(
                        run_id=run_id,
                        raw_id=raw_id,
                        rule="basic_industry_invalid",
                        severity="blocking",
                        detail={"ts_code": code, "reason": reason},
                    )
                    raise CorporateError("invalid_basic_industry_response") from None
                batches.append(
                    {
                        "source_api": active_api,
                        "ts_code": code,
                        "rows": len(rows),
                        "status": "received" if rows else "no_data_unverified",
                    }
                )
                for is_new in ("Y", "N"):
                    active_api = "index_member_all"
                    params = {"ts_code": code, "is_new": is_new, "limit": 2000, "offset": 0}
                    response = await adapter.query(active_api, params, MEMBER_FIELDS)
                    fetched = datetime.now(SHANGHAI)
                    if not response.raw_content:
                        raise CorporateError("original_response_bytes_required")
                    raw_id = await raw_store.save(
                        run_id=run_id,
                        provider=PROVIDER,
                        source_api=active_api,
                        parameters={**params, "fields": MEMBER_FIELDS},
                        fetched_at=fetched,
                        content=response.raw_content,
                    )
                    try:
                        rows = validate_member_response(
                            decimal_rows(response.raw_content), code, is_new, catalogs
                        )
                        provenance = _provenance(
                            run_id, raw_id, response.raw_content, fetched, active_api
                        )
                        for row in rows:
                            staged.extend(
                                (membership, raw_id)
                                for membership in normalize_memberships(row, provenance)
                            )
                    except (ValueError, KeyError, TypeError) as exc:
                        reason = (
                            str(exc)
                            if isinstance(exc, CorporateError)
                            else "invalid_industry_membership_record"
                        )
                        await repository.add_quality_issue(
                            run_id=run_id,
                            raw_id=raw_id,
                            rule="industry_membership_invalid",
                            severity="blocking",
                            detail={"ts_code": code, "is_new": is_new, "reason": reason},
                        )
                        raise CorporateError("invalid_industry_membership_response") from None
                    batches.append(
                        {
                            "source_api": active_api,
                            "ts_code": code,
                            "is_new": is_new,
                            "rows": len(rows),
                            "status": "received" if rows else "no_data_unverified",
                        }
                    )
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600611)")
                inserted = 0
                for membership, raw_id in staged:
                    inserted += await repository.publish_membership(membership, raw_id)
                current_batches = [
                    batch
                    for batch in batches
                    if batch["source_api"] == "stock_basic" or batch.get("is_new") == "Y"
                ]
                result = {
                    "run_id": str(run_id),
                    "inserted": inserted,
                    "records": len(staged),
                    "batches": batches,
                    "coverage_complete": all(batch["rows"] for batch in current_batches),
                    "classification_version": SW_VERSION,
                    "limitations": [
                        "provider_basic_is_current_snapshot_only",
                        "sw_intervals_are_source_reported_not_independently_verified",
                    ],
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        await _fail(repository, run_id, exc, active_api)
