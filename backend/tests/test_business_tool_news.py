from datetime import datetime
from types import SimpleNamespace

import anyio

from app.contracts.business_tools import AgentRole, BusinessToolStatus
from app.contracts.common import SHANGHAI
from app.providers.tushare import ProviderError, ProviderResult
from app.services.business_tool_dataops import DataOpsBusinessToolFactory

REQUEST = "7" * 32
AS_OF = datetime(2026, 11, 12, 12, tzinfo=SHANGHAI)


def _registry(provider):
    return DataOpsBusinessToolFactory(
        SimpleNamespace(code="000001.SZ", name="平安银行", exchange="SZSE"),
        as_of=AS_OF,
        provider=provider,
    ).registry()


def _call(name, limit=10):
    return {
        "name": name,
        "request_id": REQUEST,
        "arguments": {
            "code": "000001.SZ",
            "limit": limit,
            "as_of": AS_OF.isoformat(),
        },
    }


def test_stock_news_combines_two_feeds_with_fixed_seven_day_window_and_ten_item_cap():
    class Provider:
        def __init__(self):
            self.calls = []

        async def query(self, name, params, fields):
            self.calls.append((name, params, fields))
            if name == "major_news":
                return ProviderResult(
                    ("title", "content", "pub_time", "src"),
                    (
                        (
                            "平安银行发布经营信息",
                            "平安银行披露近期经营情况。" + "甲" * 2_000,
                            "2026-11-11 09:30:00",
                            params["src"],
                        ),
                        ("无关公司新闻", "与目标证券无关", "2026-11-11 09:00:00", params["src"]),
                    ),
                )
            return ProviderResult(
                ("title", "content", "datetime", "channels"),
                (
                    (
                        f"平安银行快讯-{params['src']}",
                        "平安银行相关新闻",
                        "2026-11-12 10:00:00",
                        "公司",
                    ),
                ),
            )

    async def scenario():
        provider = Provider()
        result = await _registry(provider).execute(_call("get_stock_news"), AgentRole.NEWS)
        return provider, result

    provider, result = anyio.run(scenario)
    calls = provider.calls
    assert len(calls) == 7
    assert {call[0] for call in calls} == {"major_news", "news"}
    assert all(call[1]["start_date"] == "2026-11-06 00:00:00" for call in calls)
    assert all(call[1]["end_date"] == "2026-11-12 23:59:59" for call in calls)
    assert result.status == BusinessToolStatus.PARTIAL
    assert result.coverage.complete is False
    assert result.coverage.start.isoformat() == "2026-11-06"
    assert len(result.items) == len(result.evidence) == 5
    assert len(result.items) <= 10
    assert result.items[0]["published_at"] == "2026-11-12 10:00:00"
    assert any(item["content_truncated"] is True for item in result.items)
    assert all(item.quality == "unverified" for item in result.evidence)


def test_announcements_query_each_of_seven_dates_and_return_at_most_ten():
    class Provider:
        def __init__(self):
            self.calls = []

        async def query(self, name, params, fields):
            self.calls.append((name, params, fields))
            day = params["ann_date"]
            return ProviderResult(
                ("ann_date", "ts_code", "name", "title", "url", "rec_time"),
                tuple(
                    (
                        day,
                        "000001.SZ",
                        "平安银行",
                        f"公告-{day}-{index}",
                        f"https://example.test/{day}/{index}.pdf",
                        f"{day[:4]}-{day[4:6]}-{day[6:]} 18:00:0{index}",
                    )
                    for index in range(2)
                ),
            )

    async def scenario():
        provider = Provider()
        result = await _registry(provider).execute(_call("get_announcements"), AgentRole.NEWS)
        return provider, result

    provider, result = anyio.run(scenario)
    assert len(provider.calls) == 7
    assert {call[0] for call in provider.calls} == {"anns_d"}
    assert [call[1]["ann_date"] for call in provider.calls] == [
        "20261106",
        "20261107",
        "20261108",
        "20261109",
        "20261110",
        "20261111",
        "20261112",
    ]
    assert all(call[1]["ts_code"] == "000001.SZ" for call in provider.calls)
    assert result.status == BusinessToolStatus.OK
    assert result.coverage.complete is True
    assert len(result.items) == len(result.evidence) == 10
    assert result.items[0]["ann_date"] == "2026-11-12"


def test_earnings_events_merge_forecast_and_express_with_one_combined_cap():
    class Provider:
        def __init__(self):
            self.calls = []

        async def query(self, name, params, fields):
            self.calls.append((name, params, fields))
            if name == "forecast":
                return ProviderResult(
                    ("ts_code", "ann_date", "end_date", "type", "net_profit_min"),
                    tuple(
                        ("000001.SZ", f"202611{day:02d}", "20260930", "预增", day)
                        for day in range(6, 13)
                    ),
                )
            return ProviderResult(
                ("ts_code", "ann_date", "end_date", "revenue", "n_income"),
                tuple(
                    ("000001.SZ", f"202611{day:02d}", "20260930", day * 100, day * 10)
                    for day in range(6, 13)
                ),
            )

    async def scenario():
        provider = Provider()
        result = await _registry(provider).execute(_call("get_earnings_events"), AgentRole.NEWS)
        return provider, result

    provider, result = anyio.run(scenario)
    assert [call[0] for call in provider.calls] == ["forecast", "express"]
    assert all(call[1]["start_date"] == "20261106" for call in provider.calls)
    assert all(call[1]["end_date"] == "20261112" for call in provider.calls)
    assert all(call[1]["ts_code"] == "000001.SZ" for call in provider.calls)
    assert result.status == BusinessToolStatus.OK
    assert result.coverage.complete is True
    assert len(result.items) == len(result.evidence) == 10
    assert {item["event_type"] for item in result.items} == {
        "earnings_forecast",
        "earnings_express",
    }


def test_empty_stock_news_remains_coverage_incomplete():
    class Provider:
        async def query(self, name, params, fields):
            del name, params, fields
            return ProviderResult((), ())

    async def scenario():
        return await _registry(Provider()).execute(_call("get_stock_news"), AgentRole.NEWS)

    result = anyio.run(scenario)
    assert result.status == BusinessToolStatus.COVERAGE_INCOMPLETE
    assert result.items == result.evidence == ()
    assert "no_company_matched_news_in_bounded_coverage" in {gap.reason for gap in result.gaps}


def test_stock_news_permission_denial_is_explicit():
    class Provider:
        async def query(self, name, params, fields):
            del name, params, fields
            raise ProviderError("permission_denied", http_status=403)

    async def scenario():
        return await _registry(Provider()).execute(_call("get_stock_news"), AgentRole.NEWS)

    result = anyio.run(scenario)
    assert result.status == BusinessToolStatus.PERMISSION_DENIED
    assert result.items == result.evidence == ()
    assert "one_or_more_sources_permission_denied" in {gap.reason for gap in result.gaps}
