"""Sample-scoped collect -> retain -> normalize -> validate -> atomic publish."""

import asyncio
import hashlib
from collections import Counter
from datetime import datetime, timedelta
from uuid import uuid4

from psycopg import sql

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.domain import Security, SecurityAlias, SecurityKey, TradingCalendar
from app.contracts.mapping import content_version, upstream_date
from app.providers.tushare import ProviderError
from app.repositories.storage import identity
from app.services.securities import coverage

FIELDS = {
    "stock_basic": "ts_code,name,exchange,list_status,list_date,delist_date",
    "trade_cal": "exchange,cal_date,is_open,pretrade_date",
    "namechange": "ts_code,name,start_date,end_date",
}
STATUSES = {"L": "listed", "D": "delisted", "P": "paused"}
PROVIDER = "tushare_compatible"


class ReferenceError(ValueError):
    """Only fixed reason codes may be exposed to the workflow logger."""


def security_key(code):
    return SecurityKey(ts_code=code, exchange={"SH": "SSE", "SZ": "SZSE"}.get(code[-2:]))


def required_exchanges(codes):
    """Return only exchanges needed by the requested security batch."""
    return tuple(sorted({security_key(code).exchange for code in codes}))


def optional_date(value):
    return None if value in (None, "") else upstream_date(value)


def normalize(api, row, provenance):
    if api == "stock_basic":
        key = security_key(row["ts_code"])
        if row["exchange"] != key.exchange or row["list_status"] not in STATUSES:
            raise ReferenceError("security_exchange_or_status_mismatch")
        return Security(
            security=key,
            name=row["name"],
            list_date=optional_date(row["list_date"]),
            delist_date=optional_date(row["delist_date"]),
            listing_status=STATUSES[row["list_status"]],
            provenance=provenance,
        )
    if api == "namechange":
        return SecurityAlias(
            security=security_key(row["ts_code"]),
            name=row["name"],
            effective_from=upstream_date(row["start_date"]),
            effective_to=optional_date(row["end_date"]),
            provenance=provenance,
        )
    if api == "trade_cal":
        value = row["is_open"]
        if type(value) not in (str, int) or value not in (0, 1, "0", "1"):
            raise ReferenceError("invalid_open_flag")
        return TradingCalendar(
            exchange=row["exchange"],
            calendar_date=upstream_date(row["cal_date"]),
            is_open=value in (1, "1"),
            previous_open_date=optional_date(row["pretrade_date"]),
            provenance=provenance,
        )
    raise ReferenceError("unsupported_reference_api")


def validate_response(api, rows, params):
    required = set(FIELDS[api].split(","))
    if any(not required <= row.keys() for row in rows):
        raise ReferenceError("required_projection_missing")
    if api in {"stock_basic", "namechange"}:
        if any(row["ts_code"] != params["ts_code"] for row in rows):
            raise ReferenceError("security_filter_ignored")
        if api == "stock_basic" and (
            len(rows) > 1 or any(row["list_status"] != params["list_status"] for row in rows)
        ):
            raise ReferenceError("status_filter_or_uniqueness_failed")
        if api == "namechange" and len(rows) >= 1000:
            raise ReferenceError("alias_response_may_be_truncated")
    else:
        expected = set()
        day, end = upstream_date(params["start_date"]), upstream_date(params["end_date"])
        while day <= end:
            expected.add(day)
            day += timedelta(days=1)
        actual = [upstream_date(row["cal_date"]) for row in rows]
        if set(actual) != expected or len(actual) != len(expected):
            raise ReferenceError("calendar_gap_duplicate_or_range_mismatch")
        if any(row["exchange"] != params["exchange"] for row in rows):
            raise ReferenceError("calendar_exchange_filter_ignored")


def validate_window(codes, start, end):
    if not 1 <= len(codes) <= 30 or len(set(codes)) != len(codes):
        raise ReferenceError("require_1_to_30_unique_codes")
    for code in codes:
        security_key(code)
    if not 0 <= (end - start).days <= 1100:
        raise ReferenceError("require_window_up_to_1101_days")


async def collect_reference(
    adapter, repository, raw_store, codes, start, end, *, include_aliases=True, timeout_seconds=600
):
    validate_window(codes, start, end)
    run_id = await repository.create_run(
        "step07:" + uuid4().hex,
        {
            "ts_code": ",".join(codes),
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    staged = []
    active_api = None
    limitations = ["historical_listing_and_daily_suspension_status_unavailable"]

    async def fetch(api, params):
        nonlocal active_api
        active_api = api
        response = await adapter.query(api, params, FIELDS[api])
        fetched = datetime.now(SHANGHAI)
        if not response.raw_content:
            raise ReferenceError("original_response_bytes_required")
        raw_id = await raw_store.save(
            run_id=run_id,
            provider=PROVIDER,
            source_api=api,
            parameters={**params, "fields": FIELDS[api]},
            fetched_at=fetched,
            content=response.raw_content,
        )
        rows = response.rows
        try:
            validate_response(api, rows, params)
            records = []
            for row in rows:
                provenance = Provenance(
                    provider=PROVIDER,
                    source_api=api,
                    source_ref=str(raw_id),
                    raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                    request_id=run_id.hex,
                    data_version="0" * 64,
                    mapping_version="step07-v1",
                    timing=Availability(fetched_at=fetched, available_at=fetched, basis="observed"),
                    quality="valid",
                    freshness="fresh",
                )
                records.append(normalize(api, row, provenance))
            keys = [identity(record)[1] for record in records]
            if len(keys) != len(set(keys)):
                raise ReferenceError("duplicate_reference_key")
        except (ValueError, KeyError, TypeError) as exc:
            await repository.add_quality_issue(
                run_id=run_id,
                rule="reference_response_invalid",
                severity="blocking",
                detail={
                    "source_api": api,
                    "reason": str(exc) if isinstance(exc, ReferenceError) else "invalid_record",
                },
                raw_id=raw_id,
            )
            raise ReferenceError("reference_response_invalid") from None
        staged.extend((record, raw_id) for record in records)
        return records

    try:
        async with asyncio.timeout(timeout_seconds):
            for code in codes:
                found = []
                for status in STATUSES:
                    found.extend(
                        await fetch("stock_basic", {"ts_code": code, "list_status": status})
                    )
                if len(found) != 1:
                    raise ReferenceError("security_missing_or_multiple_statuses")
                if include_aliases:
                    try:
                        aliases = await fetch("namechange", {"ts_code": code})
                        if not aliases:
                            limitations.append(f"alias_history_empty:{code}")
                    except ProviderError as exc:
                        if exc.kind != "permission_denied":
                            raise
                        limitations.append(f"alias_permission_denied:{code}")
                else:
                    limitations.append(f"alias_history_not_requested:{code}")
            for exchange in required_exchanges(codes):
                chunk_start = start
                while chunk_start <= end:
                    chunk_end = min(end, chunk_start + timedelta(days=365))
                    await fetch(
                        "trade_cal",
                        {
                            "exchange": exchange,
                            "start_date": chunk_start.strftime("%Y%m%d"),
                            "end_date": chunk_end.strftime("%Y%m%d"),
                        },
                    )
                    chunk_start = chunk_end + timedelta(days=1)
            securities = [record for record, _ in staged if isinstance(record, Security)]
            calendars = [record for record, _ in staged if isinstance(record, TradingCalendar)]
            manifest = coverage(securities, calendars, start, end)
            # All required source batches must pass before any domain record is published.
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600607)")
                inserted = 0
                for record, raw_id in staged:
                    kind, key, semantic = identity(record)
                    cursor = await repository.connection.execute(
                        "SELECT semantic_hash,data_version FROM storage.record_version "
                        "WHERE kind=%s AND business_key=%s "
                        "ORDER BY available_at DESC,fetched_at DESC,data_version DESC LIMIT 1",
                        (kind, key),
                    )
                    previous = await cursor.fetchone()
                    previous_present = False
                    if previous:
                        cursor = await repository.connection.execute(
                            sql.SQL(
                                "SELECT EXISTS(SELECT 1 FROM storage.{} WHERE data_version=%s)"
                            ).format(sql.Identifier(kind)),
                            (previous[1],),
                        )
                        previous_present = (await cursor.fetchone())[0]
                    version = (
                        previous[1]
                        if previous and previous[0] == semantic and previous_present
                        else content_version(
                            {
                                "semantic": semantic,
                                "observed_at": record.provenance.timing.fetched_at.isoformat(),
                            }
                        )
                    )
                    record = record.model_copy(
                        update={
                            "provenance": record.provenance.model_copy(
                                update={"data_version": version}
                            )
                        }
                    )
                    inserted += await repository.put(record, raw_id)
                result = {
                    "run_id": str(run_id),
                    "inserted": inserted,
                    "records": len(staged),
                    "limitations": limitations,
                    "coverage_counts": dict(Counter(day.state for day in manifest)),
                    "coverage": [day.model_dump(mode="json") for day in manifest],
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else (str(exc) if isinstance(exc, ReferenceError) else type(exc).__name__)
        )
        await repository.transition_run(
            run_id, "running", status, result={"reason": reason, "source_api": active_api}
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        # Do not let DB/validation/transport exceptions leak credentials into Prefect logs.
        raise ReferenceError(f"reference_pipeline_failed:{reason}") from None
