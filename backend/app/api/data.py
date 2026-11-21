"""Unified Step14 data, lineage, status and bounded Prefect task API."""

from datetime import date, datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import AwareDatetime

from app.api.auth import Identity, Role, current_identity, require_role
from app.api.dependencies import Database, PrefectClient, RedisClient
from app.contracts.api import (
    BackfillRequest,
    DataMeta,
    DataPage,
    GapFillRequest,
    Page,
    quarter_ends,
)
from app.contracts.common import SHANGHAI, AsOfQuery
from app.contracts.domain import AdjustmentFactor, FinancialFact, PriceBar
from app.core.errors import AppError
from app.dataops.market import validate_window
from app.dataops.orchestration import market_batches
from app.repositories.api import ApiRepository, task_idempotency_key
from app.repositories.corporate import CorporateRepository
from app.repositories.financial import FinancialRepository
from app.repositories.market import MarketRepository
from app.repositories.orchestration import OrchestrationRepository
from app.repositories.quality import QualityRepository
from app.services.cache import QueryCache
from app.services.metrics import adjusted_prices, financial_ratio
from app.services.prefect import PrefectUnavailable

router = APIRouter(prefix="/api/v1", tags=["data"], dependencies=[Depends(current_identity)])
Code = Annotated[str, Path(pattern=r"^[0-9]{6}\.(SH|SZ)$")]
UnitMode = Literal["standard", "raw"]
Adjustment = Literal["none", "front", "back"]


def _as_of(request, value):
    return AsOfQuery(
        request_id=request.state.request_id,
        as_of=value or datetime.now(SHANGHAI),
        knowledge="public",
    )


def _project_units(value, mode):
    if isinstance(value, list):
        return [_project_units(item, mode) for item in value]
    if isinstance(value, tuple):
        return tuple(_project_units(item, mode) for item in value)
    if not isinstance(value, dict):
        return value
    projected = {key: _project_units(item, mode) for key, item in value.items()}
    if mode == "raw" and {"value", "unit", "raw_value", "raw_unit"} <= value.keys():
        projected["value"] = value["raw_value"]
        projected["unit"] = value["raw_unit"]
    return projected


def _metadata(records):
    versions, freshness, quality = set(), set(), set()

    def visit(value):
        if isinstance(value, (list, tuple)):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            provenance = value.get("provenance")
            if isinstance(provenance, dict):
                versions.add(provenance.get("data_version"))
                freshness.add(provenance.get("freshness"))
                quality.add(provenance.get("quality"))
            if {"data_version", "freshness", "quality"} <= value.keys():
                versions.add(value.get("data_version"))
                freshness.add(value.get("freshness"))
                quality.add(value.get("quality"))
            for item in value.values():
                visit(item)

    visit(records)
    versions.discard(None)
    freshness.discard(None)
    quality.discard(None)
    freshness_value = (
        "stale" if "stale" in freshness else "fresh" if freshness == {"fresh"} else "unknown"
    )
    quality_value = (
        "invalid"
        if "invalid" in quality
        else "unverified"
        if "unverified" in quality
        else "valid"
        if quality == {"valid"}
        else "unknown"
    )
    return tuple(sorted(versions)), freshness_value, quality_value


def _query_error(exc):
    reason = str(exc)
    client_prefixes = ("invalid_", "require_", "quarter_", "financial_range_")
    if reason.startswith(client_prefixes):
        return AppError(422, "INVALID_DATA_QUERY", "The requested data scope is invalid")
    return AppError(409, "DATA_SELECTION_CONFLICT", "Published data cannot be selected uniquely")


async def _cached_page(
    request,
    redis,
    repository,
    *,
    namespace,
    dimensions,
    kinds,
    code,
    query,
    loader,
    start=None,
    end=None,
    unit="standard",
    adjustment=None,
    page=1,
    page_size=100,
):
    if page_size > request.app.state.settings.api_max_page_size:
        raise AppError(422, "PAGE_SIZE_EXCEEDED", "Requested page size exceeds server limit")
    version = await repository.version_token(kinds, code, query.as_of, start=start, end=end)
    cache = QueryCache(redis, request.app.state.settings.query_cache_ttl_seconds)
    payload, cache_state = await cache.get(namespace, dimensions, version)
    if payload is None:
        try:
            payload = _project_units(await loader(), unit)
        except ValueError as exc:
            raise _query_error(exc) from None
        if cache_state != "bypass":
            await cache.set(namespace, dimensions, version, payload)
    total = len(payload)
    offset = (page - 1) * page_size
    selected = payload[offset : offset + page_size]
    versions, freshness, quality = _metadata(payload)
    return DataPage(
        request_id=request.state.request_id,
        data=tuple(selected),
        page=Page(number=page, size=page_size, total=total, has_more=offset + page_size < total),
        meta=DataMeta(
            as_of=query.as_of,
            unit=unit,
            adjustment=adjustment,
            freshness=freshness,
            quality=quality,
            data_versions=versions,
            version_token=version,
            cache=cache_state,
        ),
    )


@router.get("/securities/resolve")
async def resolve_security(
    request: Request,
    database: Database,
    identity: Annotated[Identity, Depends(current_identity)],
    q: Annotated[str, Query(min_length=1, max_length=100)],
    exchange: Literal["SSE", "SZSE"] | None = None,
    on: date | None = None,
    as_of: AwareDatetime | None = None,
):
    del identity
    records = await ApiRepository(database).resolve_security(
        q, _as_of(request, as_of).as_of, exchange=exchange, on_date=on
    )
    if len(records) > 30:
        raise AppError(409, "AMBIGUOUS_SECURITY", "Too many matching securities")
    return {
        "request_id": request.state.request_id,
        "data": [item.model_dump(mode="json") for item in records],
    }


@router.get("/data/prices/{code}", response_model=DataPage)
async def prices(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    start: date,
    end: date,
    as_of: AwareDatetime | None = None,
    adjustment: Adjustment = "none",
    unit: UnitMode = "standard",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    del identity
    try:
        validate_window(code, start, end)
    except ValueError as exc:
        raise _query_error(exc) from None
    query = _as_of(request, as_of)
    market = MarketRepository(database)
    api = ApiRepository(database)

    async def load():
        result = await market.market(code, start, end, query)
        bars = [PriceBar.model_validate(item) for item in result["price_bar"]]
        if adjustment == "none":
            return [item.model_dump(mode="json") for item in bars]
        if not bars:
            return []
        factors = [AdjustmentFactor.model_validate(item) for item in result["adjustment_factor"]]
        series = adjusted_prices(bars, factors, adjustment=adjustment)
        if series.status != "ok":
            raise AppError(409, "ADJUSTMENT_UNAVAILABLE", series.reason or "Adjustment unavailable")
        factor_by_date = {item.trade_date: item for item in factors}
        bar_by_date = {item.trade_date: item for item in bars}
        payload = []
        for point in series.points:
            item = point.model_dump(mode="json")
            item["source_provenance"] = [
                bar_by_date[point.trade_date].provenance.model_dump(mode="json"),
                factor_by_date[point.trade_date].provenance.model_dump(mode="json"),
            ]
            payload.append(item)
        return payload

    return await _cached_page(
        request,
        redis,
        api,
        namespace="prices",
        dimensions={
            "code": code,
            "start": start,
            "end": end,
            "as_of": query.as_of,
            "unit": unit,
            "adjustment": adjustment,
        },
        kinds=("price_bar", "adjustment_factor") if adjustment != "none" else ("price_bar",),
        code=code,
        query=query,
        loader=load,
        start=start,
        end=end,
        unit=unit,
        adjustment=adjustment,
        page=page,
        page_size=page_size,
    )


@router.get("/data/valuations/{code}", response_model=DataPage)
async def valuations(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    start: date,
    end: date,
    as_of: AwareDatetime | None = None,
    unit: UnitMode = "standard",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    del identity
    try:
        validate_window(code, start, end)
    except ValueError as exc:
        raise _query_error(exc) from None
    query = _as_of(request, as_of)
    market, api = MarketRepository(database), ApiRepository(database)

    async def load():
        return (await market.market(code, start, end, query))["valuation"]

    return await _cached_page(
        request,
        redis,
        api,
        namespace="valuations",
        dimensions={"code": code, "start": start, "end": end, "as_of": query.as_of, "unit": unit},
        kinds=("valuation",),
        code=code,
        query=query,
        loader=load,
        start=start,
        end=end,
        unit=unit,
        page=page,
        page_size=page_size,
    )


@router.get("/data/financials/{code}", response_model=DataPage)
async def financials(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    start: date,
    end: date,
    as_of: AwareDatetime | None = None,
    report_type: str = "1",
    history: bool = False,
    unit: UnitMode = "standard",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    del identity
    try:
        periods = quarter_ends(start, end)
    except ValueError as exc:
        raise _query_error(exc) from None
    query = _as_of(request, as_of)
    financial, api = FinancialRepository(database), ApiRepository(database)

    async def load():
        return await financial.financials(
            code, periods, query, report_type=report_type, history=history
        )

    return await _cached_page(
        request,
        redis,
        api,
        namespace="financials",
        dimensions={
            "code": code,
            "periods": periods,
            "report_type": report_type,
            "history": history,
            "as_of": query.as_of,
            "unit": unit,
        },
        kinds=("financial_fact",),
        code=code,
        query=query,
        loader=load,
        start=start,
        end=end,
        unit=unit,
        page=page,
        page_size=page_size,
    )


@router.get("/data/metrics/{code}")
async def metrics(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    period_end: date,
    metric: Literal[
        "net_profit_margin",
        "liability_ratio",
        "operating_cash_flow_margin",
        "operating_cash_flow_to_profit",
    ],
    as_of: AwareDatetime | None = None,
    unit: UnitMode = "standard",
):
    del identity
    query = _as_of(request, as_of)
    financial, api = FinancialRepository(database), ApiRepository(database)

    async def load():
        facts = [
            FinancialFact.model_validate(item)
            for item in await financial.financials(code, [period_end], query)
        ]
        indexed = {item.metric: item for item in facts}
        pairs = {
            "liability_ratio": ("total_liabilities", "total_assets"),
            "operating_cash_flow_margin": ("operating_cash_flow", "revenue"),
            "operating_cash_flow_to_profit": ("operating_cash_flow", "net_profit_parent"),
        }
        if metric == "net_profit_margin":
            profit = indexed.get("net_profit_parent")
            denominator = (
                "total_revenue" if profit and profit.company_type != "industrial" else "revenue"
            )
            pair = ("net_profit_parent", denominator)
        else:
            pair = pairs[metric]
        if any(name not in indexed for name in pair):
            raise AppError(
                404, "METRIC_INPUT_MISSING", "Required published metric inputs are missing"
            )
        result = financial_ratio(metric, indexed[pair[0]], indexed[pair[1]]).model_dump(mode="json")
        result["input_provenance"] = [
            indexed[name].provenance.model_dump(mode="json") for name in pair
        ]
        return [result]

    response = await _cached_page(
        request,
        redis,
        api,
        namespace="metrics",
        dimensions={
            "code": code,
            "period_end": period_end,
            "metric": metric,
            "as_of": query.as_of,
            "unit": unit,
        },
        kinds=("financial_fact",),
        code=code,
        query=query,
        loader=load,
        start=period_end,
        end=period_end,
        unit=unit,
        page=1,
        page_size=1,
    )
    return response


@router.get("/data/dividends/{code}", response_model=DataPage)
async def dividends(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    as_of: AwareDatetime | None = None,
    start: date | None = None,
    end: date | None = None,
    history: bool = False,
    unit: UnitMode = "standard",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    del identity
    if (start is None) != (end is None):
        raise AppError(422, "INVALID_DATA_QUERY", "Dividend start and end must be paired")
    try:
        report_ends = () if start is None else tuple(quarter_ends(start, end))
    except ValueError as exc:
        raise _query_error(exc) from None
    query = _as_of(request, as_of)
    corporate, api = CorporateRepository(database), ApiRepository(database)

    async def load():
        return await corporate.corporate_actions(
            code, query, history=history, report_ends=report_ends
        )

    return await _cached_page(
        request,
        redis,
        api,
        namespace="dividends",
        dimensions={
            "code": code,
            "start": start,
            "end": end,
            "history": history,
            "as_of": query.as_of,
            "unit": unit,
        },
        kinds=("corporate_action",),
        code=code,
        query=query,
        loader=load,
        start=start,
        end=end,
        unit=unit,
        page=page,
        page_size=page_size,
    )


@router.get("/data/industries/{code}", response_model=DataPage)
async def industries(
    request: Request,
    code: Code,
    database: Database,
    redis: RedisClient,
    identity: Annotated[Identity, Depends(current_identity)],
    as_of: AwareDatetime | None = None,
    on: date | None = None,
    classification: Literal["provider_basic", "SW"] | None = None,
    history: bool = False,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    del identity
    query = _as_of(request, as_of)
    corporate, api = CorporateRepository(database), ApiRepository(database)

    async def load():
        return await corporate.industries(
            code, query, history=history, on_date=on, classification=classification
        )

    return await _cached_page(
        request,
        redis,
        api,
        namespace="industries",
        dimensions={
            "code": code,
            "history": history,
            "on": on,
            "classification": classification,
            "as_of": query.as_of,
        },
        kinds=("industry_membership",),
        code=code,
        query=query,
        loader=load,
        page=page,
        page_size=page_size,
    )


@router.get("/data/lineage")
async def lineage(
    request: Request,
    database: Database,
    identity: Annotated[Identity, Depends(current_identity)],
    node_type: Literal["raw", "standard", "derived", "evidence", "report", "alert"],
    reference_key: Annotated[str, Query(min_length=1, max_length=500)],
):
    del identity
    return {
        "request_id": request.state.request_id,
        **await QualityRepository(database).trace(node_type, reference_key),
    }


@router.get("/data/status/{code}")
async def data_status(
    request: Request,
    code: Code,
    database: Database,
    identity: Annotated[Identity, Depends(current_identity)],
):
    del identity
    return {
        "request_id": request.state.request_id,
        **await ApiRepository(database).data_status(code),
    }


async def _submit_task(database, gateway, identity, kind, flow, deployment, parameters):
    repository = ApiRepository(database)
    key = task_idempotency_key(identity.user_id, kind, deployment, parameters)
    task_id, created = await repository.create_task(
        identity.user_id, kind, deployment, parameters, key
    )
    if not created:
        task = await repository.task(task_id)
        return {"task_id": str(task_id), "status": task["status"], "deduplicated": True}
    try:
        flow_run_id = await gateway.submit(flow, deployment, parameters, task_id)
    except PrefectUnavailable:
        await repository.fail_submission(task_id)
        raise AppError(503, "PREFECT_UNAVAILABLE", "Task scheduler is unavailable") from None
    await repository.attach_flow_run(task_id, flow_run_id)
    return {
        "task_id": str(task_id),
        "flow_run_id": str(flow_run_id),
        "status": "submitted",
        "deduplicated": False,
    }


@router.post("/tasks/backfills", status_code=202)
async def submit_backfill(
    payload: BackfillRequest,
    identity: Annotated[Identity, Depends(require_role(Role.DATA_ADMIN))],
    database: Database,
    gateway: PrefectClient,
):
    parameters = {
        "codes": list(payload.codes),
        "start": payload.start.isoformat(),
        "end": payload.end.isoformat(),
        "window_days": payload.window_days,
    }
    return await _submit_task(
        database,
        gateway,
        identity,
        "backfill",
        "historical-backfill",
        "bounded-history",
        parameters,
    )


@router.post("/tasks/gap-fills", status_code=202)
async def submit_gap_fill(
    payload: GapFillRequest,
    database: Database,
    gateway: PrefectClient,
    identity: Annotated[Identity, Depends(current_identity)],
):
    days = (payload.end - payload.start).days
    if identity.role == Role.USER and (len(payload.codes) != 1 or days > 45):
        raise AppError(
            403,
            "GAP_FILL_SCOPE_EXCEEDED",
            "Research users may request one security and up to 46 days",
        )
    if identity.role != Role.USER and days > 365:
        raise AppError(422, "GAP_FILL_SCOPE_EXCEEDED", "Gap fill range may not exceed one year")
    parameters = {
        "codes": list(payload.codes),
        "start": payload.start.isoformat(),
        "end": payload.end.isoformat(),
        "window_days": 31,
    }
    return await _submit_task(
        database,
        gateway,
        identity,
        "gap_fill",
        "historical-backfill",
        "bounded-history",
        parameters,
    )


@router.post("/tasks/retries", status_code=202)
async def submit_retry(
    batch_id: UUID,
    identity: Annotated[Identity, Depends(require_role(Role.DATA_ADMIN))],
    database: Database,
    gateway: PrefectClient,
):
    batch = await OrchestrationRepository(database).batch(batch_id)
    if batch is None:
        raise AppError(404, "BATCH_NOT_FOUND", "Batch not found")
    if batch[3] not in {"failed", "cancelled", "deferred"}:
        raise AppError(409, "BATCH_NOT_REPLAYABLE", "Batch is not replayable")
    return await _submit_task(
        database,
        gateway,
        identity,
        "retry",
        "manual-batch-replay",
        "failed-batch-replay",
        {"batch_id": str(batch_id)},
    )


@router.get("/tasks/{task_id}")
async def task_status(
    task_id: UUID,
    database: Database,
    gateway: PrefectClient,
    identity: Annotated[Identity, Depends(current_identity)],
):
    task = await ApiRepository(database).task(task_id)
    if task is None:
        raise AppError(404, "TASK_NOT_FOUND", "Task not found")
    if identity.role == Role.USER and task["owner_id"] != identity.user_id:
        raise AppError(403, "FORBIDDEN", "Resource belongs to another user")
    remote = None
    if task["flow_run_id"]:
        try:
            remote = await gateway.flow_run(task["flow_run_id"])
        except PrefectUnavailable:
            remote = {"flow_run_id": str(task["flow_run_id"]), "state": "unavailable"}
    repository = ApiRepository(database)
    batches = []
    if task["kind"] in {"backfill", "gap_fill"}:
        parameters = task["parameters"]
        specs = market_batches(
            parameters["codes"],
            date.fromisoformat(parameters["start"]),
            date.fromisoformat(parameters["end"]),
            window_days=parameters["window_days"],
        )
        batches = await repository.orchestration_results([item.idempotency_key for item in specs])
    elif task["kind"] == "retry":
        batch = await OrchestrationRepository(database).batch(UUID(task["parameters"]["batch_id"]))
        batches = (
            [] if batch is None else [{"batch_id": task["parameters"]["batch_id"], "state": batch}]
        )
    return {**task, "remote": remote, "batches": batches}
