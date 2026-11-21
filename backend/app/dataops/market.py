"""Single-security daily source -> raw -> quality -> atomic publication."""

import asyncio
import hashlib
import json
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from app.contracts.common import SHANGHAI, AsOfQuery, Availability, Provenance
from app.contracts.domain import AdjustmentFactor, PriceBar, Valuation
from app.contracts.mapping import NUMERIC_FIELDS, content_version, map_measure, upstream_date
from app.dataops.reference import PROVIDER, security_key
from app.providers.tushare import ProviderError
from app.repositories.storage import identity
from app.services.securities import SecurityService

MODELS = {"daily": PriceBar, "adj_factor": AdjustmentFactor, "daily_basic": Valuation}
FIELDS = {
    api: "ts_code,trade_date," + ",".join(f.upstream for f in NUMERIC_FIELDS if f.api == api)
    for api in MODELS
}


class MarketError(ValueError):
    """Only fixed, safe reason codes may be logged."""


def decimal_rows(content):
    # Reparse original bytes: the transport's general-purpose rows use binary floats.
    # Converting those floats back to strings would already have lost source precision.
    data = json.loads(content, parse_float=Decimal)["data"]
    fields = data["fields"]
    if len(fields) != len(set(fields)):
        raise MarketError("duplicate_projection_field")
    return [dict(zip(fields, row, strict=True)) for row in data["items"]]


def validate_window(code, start, end):
    security_key(code)
    if not 0 <= (end - start).days <= 45:
        raise MarketError("require_single_security_window_up_to_46_days")


def normalize(api, row, provenance):
    values = {f.target: map_measure(api, f.upstream, row) for f in NUMERIC_FIELDS if f.api == api}
    record = MODELS[api](
        security=security_key(row["ts_code"]),
        trade_date=upstream_date(row["trade_date"]),
        provenance=provenance,
        **values,
    )
    for spec in NUMERIC_FIELDS:
        if spec.api != api:
            continue
        value = getattr(record, spec.target).value
        if spec.required and value is None:
            raise MarketError("required_measure_missing")
        # Negative PE/PB is meaningful and must not be coerced or rejected.
        if (
            value is not None
            and spec.target
            in {
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "total_market_cap",
                "float_market_cap",
            }
            and value < 0
        ):
            raise MarketError("negative_price_quantity_or_cap")
    if api == "daily" and not (
        record.low.value
        <= min(record.open.value, record.close.value)
        <= max(record.open.value, record.close.value)
        <= record.high.value
    ):
        raise MarketError("invalid_ohlc_relationship")
    if api == "adj_factor" and record.factor.value <= 0:
        raise MarketError("nonpositive_adjustment_factor")
    return record


def validate_response(api, rows, code, start, end, expected):
    required = set(FIELDS[api].split(","))
    actual = []
    for row in rows:
        if not required <= row.keys():
            raise MarketError("required_projection_missing")
        if row["ts_code"] != code:
            raise MarketError("security_filter_ignored")
        day = upstream_date(row["trade_date"])
        if not start <= day <= end or day not in expected:
            raise MarketError("date_outside_eligible_range")
        actual.append(day)
    if len(actual) != len(set(actual)):
        raise MarketError("duplicate_market_key")
    if set(actual) != expected:
        # No suspension source exists yet: retain gap as unverified, not zero volume.
        raise MarketError("missing_dates_status_unverified")


async def collect_market(adapter, repository, raw_store, code, start, end, *, timeout_seconds=180):
    validate_window(code, start, end)
    run_id = await repository.create_run(
        "step08:" + uuid4().hex,
        {
            "ts_code": code,
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    active_api = None
    staged = []
    try:
        async with asyncio.timeout(timeout_seconds):
            query = AsOfQuery(request_id=run_id.hex, as_of=datetime.now(SHANGHAI))
            manifest = await SecurityService(repository, [code]).coverage(start, end, query)
            allowed = {
                "expected_status_unverified",
                "non_trading_day",
                "before_listing",
                "after_delisting",
            }
            if any(day.state not in allowed for day in manifest):
                raise MarketError("reference_coverage_incomplete")
            expected = {day.date for day in manifest if day.state == "expected_status_unverified"}
            if not expected:
                raise MarketError("no_eligible_trading_days")
            for api in MODELS:
                active_api = api
                params = {
                    "ts_code": code,
                    "start_date": start.strftime("%Y%m%d"),
                    "end_date": end.strftime("%Y%m%d"),
                }
                response = await adapter.query(api, params, FIELDS[api])
                fetched = datetime.now(SHANGHAI)
                if not response.raw_content:
                    raise MarketError("original_response_bytes_required")
                raw_id = await raw_store.save(
                    run_id=run_id,
                    provider=PROVIDER,
                    source_api=api,
                    parameters={**params, "fields": FIELDS[api]},
                    fetched_at=fetched,
                    content=response.raw_content,
                )
                try:
                    rows = decimal_rows(response.raw_content)
                    validate_response(api, rows, code, start, end, expected)
                    for row in rows:
                        provenance = Provenance(
                            provider=PROVIDER,
                            source_api=api,
                            source_ref=str(raw_id),
                            raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                            request_id=run_id.hex,
                            data_version="0" * 64,
                            mapping_version="step08-v1",
                            timing=Availability(
                                fetched_at=fetched, available_at=fetched, basis="observed"
                            ),
                            quality="valid",
                            freshness="unknown",
                        )
                        staged.append((normalize(api, row, provenance), raw_id))
                except (ValueError, KeyError, TypeError) as exc:
                    reason = str(exc) if isinstance(exc, MarketError) else "invalid_market_record"
                    await repository.add_quality_issue(
                        run_id=run_id,
                        rule="market_response_invalid",
                        severity="blocking",
                        raw_id=raw_id,
                        detail={
                            "source_api": api,
                            "reason": reason,
                            "expected_dates": sorted(day.isoformat() for day in expected),
                        },
                    )
                    raise MarketError(reason) from None
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600608)")
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
                    version = (
                        previous[1]
                        if previous and previous[0] == semantic
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
                    "ts_code": code,
                    "inserted": inserted,
                    "records": len(staged),
                    "trading_days": len(expected),
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "published_through": end.isoformat(),
                    "limitations": [
                        "daily_suspension_status_unverified",
                        "availability_is_observation_time",
                        "watermark_certifies_only_this_window",
                    ],
                }
                await repository.advance_watermark(PROVIDER, code, start, end, run_id)
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else (str(exc) if isinstance(exc, MarketError) else type(exc).__name__)
        )
        await repository.transition_run(
            run_id, "running", status, result={"reason": reason, "source_api": active_api}
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise MarketError(f"market_pipeline_failed:{reason}") from None
