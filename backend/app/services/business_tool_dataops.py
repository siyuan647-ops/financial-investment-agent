"""Bind specialist business tools to DataOps and confirmed provider capabilities."""

import hashlib
import json
from datetime import date, datetime, time, timedelta

from app.contracts.business_tools import (
    BusinessToolEvidence,
    BusinessToolGap,
    BusinessToolName,
    BusinessToolResult,
    BusinessToolStatus,
    ToolCoverage,
)
from app.contracts.common import AsOfQuery
from app.contracts.mapping import evidence_id
from app.dataops.fundamental_domains import (
    fundamental_analysis_period_plan,
    profitability_period_plan,
)
from app.providers.tushare import ProviderError
from app.repositories.fundamental import FundamentalRepository
from app.repositories.storage import Repository
from app.repositories.technical import TechnicalRepository
from app.services.business_tools import BusinessToolRegistry

_MAJOR_NEWS_SOURCES = ("财联社", "新浪财经", "同花顺")
_NEWS_FLASH_SOURCES = (
    ("cls", "财联社"),
    ("wallstreetcn", "华尔街见闻"),
    ("sina", "新浪财经"),
    ("10jqka", "同花顺"),
)
_RECENT_INFORMATION_DAYS = 7
_RECENT_INFORMATION_LIMIT = 10
_NEWS_CONTENT_LIMIT = 1_500

_COMPANY_SINGLE_RECORD_TYPES = (
    "security_profile",
    "company_description",
    "sw_industry",
    "citic_industry",
)
_COMPANY_LATEST_PERIOD_TYPES = (
    "business_segment",
    "top10_holder",
    "top10_float_holder",
)
_FUNDAMENTAL_TOOL_DOMAINS = {
    BusinessToolName.GET_COMPANY_PROFILE: "company_profile",
    BusinessToolName.GET_PROFITABILITY: "profitability",
    BusinessToolName.GET_BALANCE_SHEET: "balance_sheet",
    BusinessToolName.GET_CASH_FLOW: "cash_flow",
}


class DataOpsBusinessToolFactory:
    def __init__(
        self,
        security,
        *,
        as_of: datetime,
        provider=None,
        database=None,
    ):
        self.security = security
        self.as_of = as_of
        self.provider = provider
        self.database = database

    def registry(self):
        handlers = {
            BusinessToolName.GET_STOCK_DATA: self._stock_data,
            BusinessToolName.GET_INDICATORS: self._indicators,
            BusinessToolName.GET_STOCK_NEWS: self._stock_news,
            BusinessToolName.GET_ANNOUNCEMENTS: self._announcements,
            BusinessToolName.GET_EARNINGS_EVENTS: self._earnings_events,
            BusinessToolName.GET_COMPANY_PROFILE: self._fundamental_domain,
            BusinessToolName.GET_PROFITABILITY: self._fundamental_domain,
            BusinessToolName.GET_BALANCE_SHEET: self._fundamental_domain,
            BusinessToolName.GET_CASH_FLOW: self._fundamental_domain,
        }
        enabled = set()
        if self.provider is not None:
            enabled.update(
                {
                    BusinessToolName.GET_STOCK_NEWS,
                    BusinessToolName.GET_ANNOUNCEMENTS,
                    BusinessToolName.GET_EARNINGS_EVENTS,
                }
            )
        if self.database is not None:
            enabled.update(
                {
                    BusinessToolName.GET_STOCK_DATA,
                    BusinessToolName.GET_INDICATORS,
                    BusinessToolName.GET_COMPANY_PROFILE,
                    BusinessToolName.GET_PROFITABILITY,
                    BusinessToolName.GET_BALANCE_SHEET,
                    BusinessToolName.GET_CASH_FLOW,
                }
            )
        return BusinessToolRegistry(handlers, enabled_tools=frozenset(enabled))

    async def _stock_data(self, call):
        arguments = call.arguments
        query = AsOfQuery(
            request_id=call.request_id,
            as_of=arguments.as_of or self.as_of,
            knowledge="public",
        )
        start = arguments.as_of_window - timedelta(days=180)
        bars = await Repository(self.database).prices(
            arguments.code,
            start,
            arguments.as_of_window,
            query,
            limit=200,
        )
        bars = bars[-60:]
        if len(bars) != 60:
            return BusinessToolResult(
                request_id=call.request_id,
                tool=call.name,
                status="coverage_incomplete",
                as_of=arguments.as_of or self.as_of,
                coverage=ToolCoverage(
                    complete=False,
                    sources=("storage.price_bar",),
                    start=start,
                    end=arguments.as_of_window,
                    note="fewer_than_60_ohlcv_bars",
                ),
                gaps=(
                    BusinessToolGap(
                        domain="ohlcv",
                        status="coverage_incomplete",
                        reason="fewer_than_60_ohlcv_bars",
                    ),
                ),
            )
        items, evidence = [], []
        for bar in bars:
            version = bar.provenance.data_version
            locator = f"ohlcv:{arguments.code}:{bar.trade_date}"
            identifier = evidence_id((version,), locator)
            items.append(
                {
                    "trade_date": bar.trade_date.isoformat(),
                    "open": str(bar.open.value),
                    "high": str(bar.high.value),
                    "low": str(bar.low.value),
                    "close": str(bar.close.value),
                    "volume": str(bar.volume.value),
                    "evidence_id": identifier,
                }
            )
            evidence.append(
                BusinessToolEvidence(
                    evidence_id=identifier,
                    label=locator,
                    observed_on=bar.trade_date,
                    source="storage.price_bar",
                    quality=bar.provenance.quality,
                )
            )
        return BusinessToolResult(
            request_id=call.request_id,
            tool=call.name,
            status="ok",
            as_of=arguments.as_of or self.as_of,
            items=tuple(items),
            evidence=tuple(evidence),
            coverage=ToolCoverage(
                complete=True,
                sources=("storage.price_bar",),
                start=bars[0].trade_date,
                end=bars[-1].trade_date,
                note="latest_60_trading_days_at_or_before_as_of_window",
            ),
        )

    async def _indicators(self, call):
        arguments = call.arguments
        query = AsOfQuery(
            request_id=call.request_id,
            as_of=arguments.as_of or self.as_of,
            knowledge="public",
        )
        records = await TechnicalRepository(self.database).indicators(
            arguments.code,
            arguments.indicators,
            arguments.as_of_window,
            query,
            days=arguments.output_days,
        )
        grouped = {}
        for record in records:
            grouped.setdefault(record.trade_date, {})[record.indicator] = record
        complete = [
            (day, values)
            for day, values in sorted(grouped.items())
            if set(values) == set(arguments.indicators)
        ]
        price_bars = await Repository(self.database).prices(
            arguments.code,
            arguments.as_of_window - timedelta(days=180),
            arguments.as_of_window,
            query,
            limit=200,
        )
        reason = _indicator_window_gap(complete, price_bars)
        if reason is not None:
            return BusinessToolResult(
                request_id=call.request_id,
                tool=call.name,
                status="coverage_incomplete",
                as_of=arguments.as_of or self.as_of,
                coverage=ToolCoverage(
                    complete=False,
                    sources=("storage.technical_indicator",),
                    end=arguments.as_of_window,
                    note=reason,
                ),
                gaps=(
                    BusinessToolGap(
                        domain="technical_indicators",
                        status="coverage_incomplete",
                        reason=reason,
                    ),
                ),
            )
        items, evidence = [], []
        for day, values in complete:
            versions = tuple(sorted({item.provenance.data_version for item in values.values()}))
            locator = f"technical_indicators:{arguments.code}:{day}"
            identifier = evidence_id(versions, locator)
            items.append(
                {
                    "trade_date": day.isoformat(),
                    "values": {name: str(values[name].value) for name in arguments.indicators},
                    "evidence_id": identifier,
                }
            )
            evidence.append(
                BusinessToolEvidence(
                    evidence_id=identifier,
                    label=locator,
                    observed_on=day,
                    source="storage.technical_indicator",
                    quality="valid",
                )
            )
        return BusinessToolResult(
            request_id=call.request_id,
            tool=call.name,
            status="ok",
            as_of=arguments.as_of or self.as_of,
            items=tuple(items),
            evidence=tuple(evidence),
            coverage=ToolCoverage(
                complete=True,
                sources=("storage.technical_indicator",),
                start=complete[0][0],
                end=complete[-1][0],
                note="latest_30_complete_indicator_dates",
            ),
        )

    async def _fundamental_domain(self, call):
        arguments = call.arguments
        domain = _FUNDAMENTAL_TOOL_DOMAINS[call.name]
        as_of = arguments.as_of or self.as_of
        records = await FundamentalRepository(self.database).records(
            domain,
            arguments.code,
            AsOfQuery(request_id=call.request_id, as_of=as_of, knowledge="public"),
            limit=500,
        )
        selected, missing = _select_fundamental_window(domain, records, as_of.date())
        items = []
        evidence = []
        for record in selected:
            version = record.provenance.data_version
            locator = (
                f"fundamental:{domain}:{arguments.code}:{record.record_type}:"
                f"{record.period_end or record.data_date}:{record.source_key}"
            )
            identifier = evidence_id((version,), locator)
            item = {
                "domain": domain,
                "record_type": record.record_type,
                "data_date": record.data_date.isoformat(),
                "period_end": (
                    None if record.period_end is None else record.period_end.isoformat()
                ),
                "announced_on": (
                    None if record.announced_on is None else record.announced_on.isoformat()
                ),
                "effective_on": (
                    None if record.effective_on is None else record.effective_on.isoformat()
                ),
                "values": record.model_dump(mode="json")["values"],
                "origin": record.origin,
                "formula": record.formula,
                "input_versions": record.input_versions,
                "evidence_id": identifier,
            }
            items.append(item)
            evidence.append(
                BusinessToolEvidence(
                    evidence_id=identifier,
                    label=(
                        f"{domain}:{record.record_type}:{record.period_end or record.data_date}"
                    )[:160],
                    observed_on=record.data_date,
                    source=record.provenance.source_api,
                    quality=record.provenance.quality,
                )
            )
        gaps = tuple(
            BusinessToolGap(
                domain=f"{domain}:{record_type}",
                status=("coverage_incomplete" if items else "no_data"),
                reason="missing_fixed_window_records",
            )
            for record_type in missing
        )
        if not items:
            status = BusinessToolStatus.NO_DATA
        elif gaps:
            status = BusinessToolStatus.PARTIAL
        else:
            status = BusinessToolStatus.OK
        dates = [record.data_date for record in selected]
        sources = tuple(dict.fromkeys(item.source for item in evidence))
        fetched = [record.provenance.timing.fetched_at for record in selected]
        return BusinessToolResult(
            request_id=call.request_id,
            tool=call.name,
            status=status,
            as_of=as_of,
            items=tuple(items),
            evidence=tuple(evidence),
            coverage=ToolCoverage(
                complete=bool(items) and not gaps,
                sources=sources,
                start=min(dates) if dates else None,
                end=max(dates) if dates else None,
                last_synced_at=max(fetched) if fetched else None,
                note=(
                    f"fixed_{domain}_business_window"
                    if items and not gaps
                    else "fixed_fundamental_window_incomplete"
                ),
            ),
            gaps=gaps,
        )

    async def _text_unavailable(self, call):
        return BusinessToolResult(
            request_id=call.request_id,
            tool=call.name,
            status="coverage_incomplete",
            as_of=call.arguments.as_of or self.as_of,
            coverage=ToolCoverage(
                complete=False,
                sources=("provider",),
                note="text_provider_not_bound",
            ),
            gaps=(
                BusinessToolGap(
                    domain=call.name,
                    status="coverage_incomplete",
                    reason="text_provider_not_bound",
                ),
            ),
        )

    async def _stock_news(self, call):
        if self.provider is None:
            return await self._text_unavailable(call)
        arguments = call.arguments
        start, end = _recent_window(arguments.as_of or self.as_of)
        rows = []
        failed_sources = []
        denied_sources = []
        requested_sources = []

        for source in _MAJOR_NEWS_SOURCES:
            source_key = f"major_news:{source}"
            requested_sources.append(source_key)
            try:
                response = await self.provider.query(
                    "major_news",
                    {
                        "src": source,
                        "start_date": _day_boundary(start, start=True),
                        "end_date": _day_boundary(end, start=False),
                        "limit": 20,
                    },
                    "title,content,pub_time,src",
                )
            except ProviderError as exc:
                target = denied_sources if exc.kind == "permission_denied" else failed_sources
                target.append(source_key)
                continue
            rows.extend(
                _normalized_news_row(row, source_api="major_news", source=source)
                for row in response.rows
                if _news_matches(row, self.security, None)
            )

        for source_id, source_name in _NEWS_FLASH_SOURCES:
            source_key = f"news:{source_name}"
            requested_sources.append(source_key)
            try:
                response = await self.provider.query(
                    "news",
                    {
                        "src": source_id,
                        "start_date": _day_boundary(start, start=True),
                        "end_date": _day_boundary(end, start=False),
                        "limit": 20,
                    },
                    "title,content,datetime,channels",
                )
            except ProviderError as exc:
                target = denied_sources if exc.kind == "permission_denied" else failed_sources
                target.append(source_key)
                continue
            rows.extend(
                _normalized_news_row(row, source_api="news", source=source_name)
                for row in response.rows
                if _news_matches(row, self.security, None)
            )

        rows = _dedupe_recent_items(rows, ("title", "published_at"))
        rows.sort(key=lambda row: str(row.get("published_at") or ""), reverse=True)
        selected = rows[: min(arguments.limit, _RECENT_INFORMATION_LIMIT)]
        return _recent_provider_result(
            call,
            selected,
            result_as_of=arguments.as_of or self.as_of,
            start=start,
            end=end,
            requested_sources=tuple(requested_sources),
            failed_sources=tuple(failed_sources),
            denied_sources=tuple(denied_sources),
            evidence_factory=_recent_news_evidence,
            empty_reason="no_company_matched_news_in_bounded_coverage",
            incomplete_reason="selected_news_sources_do_not_prove_complete_coverage",
            always_incomplete=True,
        )

    async def _announcements(self, call):
        if self.provider is None:
            return await self._text_unavailable(call)
        arguments = call.arguments
        start, end = _recent_window(arguments.as_of or self.as_of)
        rows = []
        failed_sources = []
        denied_sources = []
        for offset in range(_RECENT_INFORMATION_DAYS):
            ann_date = start + timedelta(days=offset)
            try:
                response = await self.provider.query(
                    "anns_d",
                    {
                        "ts_code": arguments.code,
                        "ann_date": ann_date.strftime("%Y%m%d"),
                        "limit": 20,
                    },
                    "ann_date,ts_code,name,title,url,rec_time",
                )
            except ProviderError as exc:
                target = denied_sources if exc.kind == "permission_denied" else failed_sources
                target.append(ann_date.isoformat())
                continue
            rows.extend(
                _normalized_announcement_row(row)
                for row in response.rows
                if str(row.get("ts_code") or "") == str(arguments.code)
            )

        rows = _dedupe_recent_items(rows, ("ts_code", "title", "ann_date"))
        rows.sort(
            key=lambda row: (str(row.get("published_at") or ""), str(row.get("ann_date") or "")),
            reverse=True,
        )
        selected = rows[: min(arguments.limit, _RECENT_INFORMATION_LIMIT)]
        return _recent_provider_result(
            call,
            selected,
            result_as_of=arguments.as_of or self.as_of,
            start=start,
            end=end,
            requested_sources=("anns_d",),
            failed_sources=tuple(failed_sources),
            denied_sources=tuple(denied_sources),
            evidence_factory=_announcement_evidence,
            empty_reason="no_announcements_in_seven_day_window",
            incomplete_reason="one_or_more_announcement_days_unavailable",
            all_attempts=_RECENT_INFORMATION_DAYS,
        )

    async def _earnings_events(self, call):
        if self.provider is None:
            return await self._text_unavailable(call)
        arguments = call.arguments
        start, end = _recent_window(arguments.as_of or self.as_of)
        rows = []
        failed_sources = []
        denied_sources = []
        requests = (
            (
                "forecast",
                "ts_code,ann_date,end_date,type,p_change_min,p_change_max,"
                "net_profit_min,net_profit_max",
            ),
            (
                "express",
                "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,"
                "total_assets",
            ),
        )
        for api_name, fields in requests:
            try:
                response = await self.provider.query(
                    api_name,
                    {
                        "ts_code": arguments.code,
                        "start_date": start.strftime("%Y%m%d"),
                        "end_date": end.strftime("%Y%m%d"),
                        "limit": 20,
                    },
                    fields,
                )
            except ProviderError as exc:
                target = denied_sources if exc.kind == "permission_denied" else failed_sources
                target.append(api_name)
                continue
            for row in response.rows:
                ann_date = _compact_date(row.get("ann_date"))
                if (
                    str(row.get("ts_code") or "") == str(arguments.code)
                    and ann_date is not None
                    and start <= ann_date <= end
                ):
                    rows.append(_normalized_earnings_row(row, api_name))

        rows = _dedupe_recent_items(rows, ("event_type", "ts_code", "ann_date", "period_end"))
        rows.sort(key=lambda row: str(row.get("ann_date") or ""), reverse=True)
        selected = rows[: min(arguments.limit, _RECENT_INFORMATION_LIMIT)]
        return _recent_provider_result(
            call,
            selected,
            result_as_of=arguments.as_of or self.as_of,
            start=start,
            end=end,
            requested_sources=("forecast", "express"),
            failed_sources=tuple(failed_sources),
            denied_sources=tuple(denied_sources),
            evidence_factory=_earnings_evidence,
            empty_reason="no_earnings_events_in_seven_day_window",
            incomplete_reason="one_or_more_earnings_event_sources_unavailable",
            all_attempts=len(requests),
        )


def _indicator_window_gap(complete, price_bars, expected_days=30):
    if len(complete) != expected_days:
        return "fewer_than_30_complete_indicator_dates"
    latest_price_date = price_bars[-1].trade_date if price_bars else None
    if complete[-1][0] != latest_price_date:
        return "indicator_price_date_mismatch"
    return None


def _select_fundamental_window(domain, records, as_of_date=None):
    as_of_date = as_of_date or datetime.now().date()
    profitability_plan = profitability_period_plan(as_of_date)
    analysis_plan = fundamental_analysis_period_plan(as_of_date)
    if domain == "company_profile":
        selected = []
        missing = []
        for record_type in _COMPANY_SINGLE_RECORD_TYPES:
            matches = _records_of_type(records, record_type)
            if matches:
                selected.append(matches[0])
            else:
                missing.append(record_type)
        for record_type in _COMPANY_LATEST_PERIOD_TYPES:
            matches = _records_of_type(records, record_type)
            if not matches:
                missing.append(record_type)
                continue
            latest = _fundamental_period(matches[0])
            selected.extend(item for item in matches if _fundamental_period(item) == latest)
        metric_records = _records_of_type(records, "company_key_metrics")
        key_metrics = _records_for_periods(
            metric_records,
            analysis_plan.company_metric_periods,
        )
        if key_metrics:
            latest_period = max(item.period_end for item in key_metrics)
            key_metrics = [
                next(
                    (
                        original
                        for original in metric_records
                        if original.period_end == item.period_end and original.origin == "direct"
                    ),
                    item,
                )
                if item.period_end != latest_period
                else item
                for item in key_metrics
            ]
        selected.extend(key_metrics)
        if len(key_metrics) < len(analysis_plan.company_metric_periods):
            missing.append("company_key_metrics")
        return _sort_fundamental_records(selected), tuple(missing)

    expected = {
        "profitability": {
            "income_statement_annual": profitability_plan.annual_periods,
            "financial_indicator_annual": profitability_plan.annual_periods,
            "income_statement_single_quarter": profitability_plan.quarter_periods,
            "financial_indicator_single_quarter": profitability_plan.quarter_periods,
        },
        "balance_sheet": {
            "balance_sheet_point_in_time": analysis_plan.balance_sheet_periods,
        },
        "cash_flow": {
            "cash_flow_statement_single_quarter": analysis_plan.cash_flow_periods,
        },
    }[domain]
    selected = []
    missing = []
    for record_type, periods in expected.items():
        matches = _records_for_periods(_records_of_type(records, record_type), periods)
        selected.extend(matches)
        if len(matches) < len(periods):
            missing.append(record_type)
    return _sort_fundamental_records(selected), tuple(missing)


def _records_of_type(records, record_type):
    return sorted(
        (item for item in records if item.record_type == record_type),
        key=lambda item: (
            _fundamental_period(item),
            item.data_date,
            item.provenance.timing.available_at,
        ),
        reverse=True,
    )


def _records_for_periods(records, periods):
    expected = set(periods)
    selected = []
    seen = set()
    for record in records:
        period = _fundamental_period(record)
        if period not in expected or period in seen:
            continue
        selected.append(record)
        seen.add(period)
        if seen == expected:
            break
    return selected


def _fundamental_period(record):
    return record.period_end or record.data_date


def _sort_fundamental_records(records):
    return sorted(
        records,
        key=lambda item: (_fundamental_period(item), item.record_type, item.source_key),
        reverse=True,
    )


def _day_boundary(value: date, *, start: bool) -> str:
    boundary = time.min if start else time(23, 59, 59)
    return datetime.combine(value, boundary).strftime("%Y-%m-%d %H:%M:%S")


def _recent_window(as_of: datetime):
    end = as_of.date()
    return end - timedelta(days=_RECENT_INFORMATION_DAYS - 1), end


def _compact_date(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if "-" not in text[:10]:
            return datetime.strptime(text[:8], "%Y%m%d").date()
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _normalized_news_row(row, *, source_api, source):
    content = str(row.get("content") or "")
    return {
        "title": str(row.get("title") or "")[:500],
        "content": content[:_NEWS_CONTENT_LIMIT],
        "content_truncated": len(content) > _NEWS_CONTENT_LIMIT,
        "published_at": str(row.get("pub_time") or row.get("datetime") or ""),
        "source": source,
        "source_api": source_api,
        "channels": str(row.get("channels") or "")[:500] or None,
    }


def _normalized_announcement_row(row):
    ann_date = _compact_date(row.get("ann_date"))
    return {
        "ts_code": str(row.get("ts_code") or ""),
        "name": str(row.get("name") or "")[:100],
        "title": str(row.get("title") or "")[:500],
        "url": str(row.get("url") or "")[:2_000],
        "ann_date": None if ann_date is None else ann_date.isoformat(),
        "published_at": str(row.get("rec_time") or row.get("ann_date") or ""),
        "source_api": "anns_d",
    }


def _normalized_earnings_row(row, api_name):
    ann_date = _compact_date(row.get("ann_date"))
    period_end = _compact_date(row.get("end_date"))
    item = {key: value for key, value in row.items() if key not in {"ann_date", "end_date"}}
    item.update(
        {
            "event_type": "earnings_forecast" if api_name == "forecast" else "earnings_express",
            "ann_date": None if ann_date is None else ann_date.isoformat(),
            "period_end": None if period_end is None else period_end.isoformat(),
            "source_api": api_name,
        }
    )
    return item


def _dedupe_recent_items(rows, fields):
    unique = {}
    for row in rows:
        key = tuple(str(row.get(field) or "").strip().casefold() for field in fields)
        unique.setdefault(key, row)
    return list(unique.values())


def _recent_provider_result(
    call,
    items,
    *,
    result_as_of,
    start,
    end,
    requested_sources,
    failed_sources,
    denied_sources,
    evidence_factory,
    empty_reason,
    incomplete_reason,
    always_incomplete=False,
    all_attempts=None,
):
    attempt_count = all_attempts if all_attempts is not None else len(requested_sources)
    unavailable_count = len(failed_sources) + len(denied_sources)
    gaps = []
    if always_incomplete:
        gaps.append(
            BusinessToolGap(
                domain=call.name,
                status="coverage_incomplete",
                reason=incomplete_reason,
            )
        )
    elif unavailable_count:
        gaps.append(
            BusinessToolGap(
                domain=call.name,
                status="coverage_incomplete",
                reason=incomplete_reason,
            )
        )
    if failed_sources:
        gaps.append(
            BusinessToolGap(
                domain=f"{call.name}_sources",
                status="source_unavailable",
                reason="one_or_more_sources_unavailable",
            )
        )
    if denied_sources:
        gaps.append(
            BusinessToolGap(
                domain=f"{call.name}_sources",
                status="permission_denied",
                reason="one_or_more_sources_permission_denied",
            )
        )

    if items:
        status = BusinessToolStatus.PARTIAL if gaps else BusinessToolStatus.OK
    elif denied_sources and len(denied_sources) == attempt_count:
        status = BusinessToolStatus.PERMISSION_DENIED
    elif unavailable_count == attempt_count:
        status = BusinessToolStatus.SOURCE_UNAVAILABLE
    elif gaps:
        status = BusinessToolStatus.COVERAGE_INCOMPLETE
    else:
        status = BusinessToolStatus.NO_DATA

    if not items and status == BusinessToolStatus.COVERAGE_INCOMPLETE:
        gaps.append(
            BusinessToolGap(
                domain=call.name,
                status="coverage_incomplete",
                reason=empty_reason,
            )
        )
    complete = not always_incomplete and unavailable_count == 0
    return BusinessToolResult(
        request_id=call.request_id,
        tool=call.name,
        status=status,
        as_of=result_as_of,
        items=tuple(items),
        evidence=tuple(evidence_factory(item) for item in items),
        coverage=ToolCoverage(
            complete=complete,
            sources=requested_sources,
            start=start,
            end=end,
            note=(
                "seven_calendar_days_ending_at_as_of"
                if complete
                else "bounded_or_incomplete_seven_day_coverage"
            ),
        ),
        gaps=tuple(gaps),
    )


def _hashed_evidence(row, *, label, source, observed_on=None, quality="unverified"):
    canonical = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return BusinessToolEvidence(
        evidence_id=f"ev_{hashlib.sha256(canonical).hexdigest()}",
        label=(str(label)[:160] or source),
        observed_on=observed_on,
        source=source,
        quality=quality,
    )


def _recent_news_evidence(row):
    observed_on = _compact_date(row.get("published_at"))
    return _hashed_evidence(
        row,
        label=row.get("title"),
        source=f"{row.get('source_api')}:{row.get('source')}",
        observed_on=observed_on,
    )


def _announcement_evidence(row):
    return _hashed_evidence(
        row,
        label=row.get("title"),
        source="anns_d",
        observed_on=_compact_date(row.get("ann_date")),
    )


def _earnings_evidence(row):
    return _hashed_evidence(
        row,
        label=f"{row.get('event_type')}:{row.get('period_end') or 'unknown'}",
        source=str(row.get("source_api") or "earnings_event"),
        observed_on=_compact_date(row.get("ann_date")),
    )


def _news_matches(row, security, query):
    # The upstream endpoint has no keyword selector; query stays soft to avoid false negatives.
    _ = query
    text = f"{row.get('title') or ''}\n{row.get('content') or ''}".casefold()
    identifiers = {
        str(security.name).strip().casefold(),
        str(security.code).strip().casefold(),
        str(security.code).split(".", 1)[0].strip().casefold(),
    }
    return any(identifier and identifier in text for identifier in identifiers)
