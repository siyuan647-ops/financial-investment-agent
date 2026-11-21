from datetime import datetime

import anyio
import pytest
from pydantic import ValidationError

from app.contracts.business_tools import (
    BUSINESS_TOOL_CALL,
    AgentRole,
    BusinessToolName,
    BusinessToolResult,
    BusinessToolStatus,
    ToolCoverage,
)
from app.contracts.common import SHANGHAI
from app.services.business_tools import BUSINESS_TOOL_DEFINITIONS, BusinessToolRegistry

REQUEST = "1" * 32
AS_OF = datetime(2026, 11, 12, 12, tzinfo=SHANGHAI)


def domain_call(name="get_profitability", **overrides):
    arguments = {"code": "000001.SZ", "as_of": AS_OF.isoformat()}
    arguments.update(overrides)
    return {"name": name, "request_id": REQUEST, "arguments": arguments}


def test_nine_role_tools_are_registered_with_bounded_metadata_and_exact_role_sets():
    registry = BusinessToolRegistry()
    assert set(registry.names) == set(BusinessToolName)
    assert len(BUSINESS_TOOL_DEFINITIONS) == 9
    assert registry.definitions_for(AgentRole.INVESTMENT) == ()

    fundamental = {item.name for item in registry.definitions_for(AgentRole.FUNDAMENTAL)}
    assert fundamental == {
        BusinessToolName.GET_COMPANY_PROFILE,
        BusinessToolName.GET_PROFITABILITY,
        BusinessToolName.GET_BALANCE_SHEET,
        BusinessToolName.GET_CASH_FLOW,
    }
    news = {item.name for item in registry.definitions_for(AgentRole.NEWS)}
    assert news == {
        BusinessToolName.GET_STOCK_NEWS,
        BusinessToolName.GET_ANNOUNCEMENTS,
        BusinessToolName.GET_EARNINGS_EVENTS,
    }
    assert registry.definitions_for(AgentRole.RISK) == ()
    market = {item.name for item in registry.definitions_for(AgentRole.MARKET)}
    assert market == {
        BusinessToolName.GET_STOCK_DATA,
        BusinessToolName.GET_INDICATORS,
    }
    metadata = registry.metadata_for(AgentRole.NEWS)
    versions = {item["name"]: item["version"] for item in metadata}
    assert versions["get_stock_news"] == "1.0.0"
    assert versions["get_announcements"] == "1.0.0"
    assert versions["get_earnings_events"] == "1.0.0"
    assert set(versions.values()) == {"1.0.0"}
    assert all(item["input_schema"]["additionalProperties"] is False for item in metadata)
    fundamental_metadata = registry.metadata_for(AgentRole.FUNDAMENTAL)
    assert {item["name"] for item in fundamental_metadata} == {
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
    }

    indicator_call = BUSINESS_TOOL_CALL.validate_python(
        {
            "name": "get_indicators",
            "request_id": REQUEST,
            "arguments": {
                "code": "000001.SZ",
                "as_of": AS_OF.isoformat(),
                "as_of_window": "2026-11-12",
                "indicators": ["rsi", "macd", "macds", "macdh"],
            },
        }
    )
    assert indicator_call.arguments.output_days == 30


def test_contracts_reject_extra_arguments_and_unknown_tools():
    with pytest.raises(ValidationError):
        BUSINESS_TOOL_CALL.validate_python(
            {
                "name": "get_profitability",
                "request_id": REQUEST,
                "arguments": {
                    "code": "000001.SZ",
                    "expression": "run arbitrary Python",
                },
            }
        )
    with pytest.raises(ValidationError):
        BUSINESS_TOOL_CALL.validate_python(
            domain_call(start="2024-01-01", end="2024-12-31", limit=100)
        )
    with pytest.raises(ValidationError):
        BUSINESS_TOOL_CALL.validate_python(
            {"name": "tushare.call", "request_id": REQUEST, "arguments": {}}
        )
    with pytest.raises(ValidationError):
        BUSINESS_TOOL_CALL.validate_python(
            {
                "name": "get_stock_news",
                "request_id": REQUEST,
                "arguments": {
                    "code": "000001.SZ",
                    "as_of": AS_OF.isoformat(),
                    "limit": 11,
                },
            }
        )
    with pytest.raises(ValidationError):
        BUSINESS_TOOL_CALL.validate_python(
            {
                "name": "get_announcements",
                "request_id": REQUEST,
                "arguments": {
                    "code": "000001.SZ",
                    "as_of": AS_OF.isoformat(),
                    "start": "2020-01-01",
                },
            }
        )


def test_registry_denies_investment_before_handler_and_reports_missing_backend_explicitly():
    called = False

    async def handler(tool_call):
        nonlocal called
        called = True
        raise AssertionError(tool_call)

    async def run():
        registry = BusinessToolRegistry({BusinessToolName.GET_PROFITABILITY: handler})
        denied = await registry.execute(domain_call(), AgentRole.INVESTMENT)
        assert denied.status == BusinessToolStatus.PERMISSION_DENIED
        assert denied.items == denied.evidence == ()
        assert denied.gaps[0].reason == "agent_role_not_allowed_for_tool"
        assert not called

        missing = await BusinessToolRegistry().execute(domain_call(), AgentRole.FUNDAMENTAL)
        assert missing.status == BusinessToolStatus.SOURCE_UNAVAILABLE
        assert missing.gaps[0].reason == "tool_backend_not_configured"

    anyio.run(run)


def test_registered_handler_returns_compact_data_and_identity_is_verified():
    async def handler(tool_call):
        return BusinessToolResult(
            request_id=tool_call.request_id,
            tool=tool_call.name,
            status="ok",
            as_of=AS_OF,
            items=({"field": "revenue", "value": "123", "unit": "CNY"},),
            coverage=ToolCoverage(
                complete=True,
                sources=("main_business_database",),
                start="2024-01-01",
                end="2024-12-31",
            ),
        )

    async def run():
        registry = BusinessToolRegistry({BusinessToolName.GET_PROFITABILITY: handler})
        result = await registry.execute(domain_call(), AgentRole.FUNDAMENTAL)
        assert result.status == BusinessToolStatus.OK
        assert result.items[0]["field"] == "revenue"

    anyio.run(run)


def test_business_tool_items_accept_non_json_native_values_until_serialization():
    result = BusinessToolResult(
        request_id=REQUEST,
        tool="get_profitability",
        status="ok",
        as_of=AS_OF,
        items=({"input_versions": ("version-a", "version-b")},),
        coverage=ToolCoverage(complete=True, sources=("main_business_database",)),
    )

    assert result.items[0]["input_versions"] == ("version-a", "version-b")
    assert result.model_dump(mode="json")["items"][0]["input_versions"] == [
        "version-a",
        "version-b",
    ]


def test_incomplete_text_coverage_cannot_be_encoded_as_clean_no_data():
    with pytest.raises(ValidationError, match="unavailable_result_requires_gap"):
        BusinessToolResult(
            request_id=REQUEST,
            tool="get_stock_news",
            status="coverage_incomplete",
            as_of=AS_OF,
            coverage=ToolCoverage(complete=False, note="licensed_sources_disabled"),
        )
