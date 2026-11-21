"""Bounded business-tool contracts for specialist research agents."""

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter, model_validator

from app.contracts.common import CalendarDate, Contract, RequestId, Text, Timestamp, TsCode


class BusinessToolName(StrEnum):
    GET_STOCK_DATA = "get_stock_data"
    GET_INDICATORS = "get_indicators"
    GET_STOCK_NEWS = "get_stock_news"
    GET_ANNOUNCEMENTS = "get_announcements"
    GET_EARNINGS_EVENTS = "get_earnings_events"
    GET_COMPANY_PROFILE = "get_company_profile"
    GET_PROFITABILITY = "get_profitability"
    GET_BALANCE_SHEET = "get_balance_sheet"
    GET_CASH_FLOW = "get_cash_flow"


class AgentRole(StrEnum):
    ORCHESTRATOR = "orchestrator"
    FUNDAMENTAL = "fundamental"
    NEWS = "news"
    MARKET = "market"
    INVESTMENT = "investment"
    RISK = "risk"


class ToolCapability(StrEnum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    COVERAGE_DEPENDENT = "coverage_dependent"


class BusinessToolStatus(StrEnum):
    OK = "ok"
    PARTIAL = "partial"
    NO_DATA = "no_data"
    COVERAGE_INCOMPLETE = "coverage_incomplete"
    SOURCE_UNAVAILABLE = "source_unavailable"
    PERMISSION_DENIED = "permission_denied"
    REJECTED = "rejected"


class _CodeArguments(Contract):
    code: TsCode
    as_of: Timestamp | None = None


TechnicalIndicatorName = Literal[
    "close_50_sma",
    "close_10_ema",
    "macd",
    "macds",
    "macdh",
    "rsi",
    "boll",
    "boll_ub",
    "boll_lb",
    "atr",
    "vwma",
]


class GetStockDataArguments(_CodeArguments):
    as_of_window: CalendarDate
    window: Literal[60] = 60


class GetIndicatorsArguments(_CodeArguments):
    as_of_window: CalendarDate
    indicators: Annotated[tuple[TechnicalIndicatorName, ...], Field(min_length=1, max_length=8)]
    output_days: Literal[30] = 30

    @model_validator(mode="after")
    def unique_indicators(self):
        if len(set(self.indicators)) != len(self.indicators):
            raise ValueError("duplicate_technical_indicator")
        return self


class RecentCompanyInformationArguments(_CodeArguments):
    """Program-bounded seven-day company information request."""

    limit: Annotated[int, Field(ge=1, le=10)] = 10


class FundamentalDomainArguments(_CodeArguments):
    """Program-bounded read from one published fundamental business domain."""


def _call_model(name: str, arguments_type):
    return type(
        name,
        (Contract,),
        {
            "__annotations__": {
                "name": Literal[arguments_type[0]],
                "request_id": RequestId,
                "arguments": arguments_type[1],
            },
            "name": arguments_type[0],
        },
    )


GetStockDataCall = _call_model("GetStockDataCall", ("get_stock_data", GetStockDataArguments))
GetIndicatorsCall = _call_model("GetIndicatorsCall", ("get_indicators", GetIndicatorsArguments))
GetStockNewsCall = _call_model(
    "GetStockNewsCall", ("get_stock_news", RecentCompanyInformationArguments)
)
GetAnnouncementsCall = _call_model(
    "GetAnnouncementsCall", ("get_announcements", RecentCompanyInformationArguments)
)
GetEarningsEventsCall = _call_model(
    "GetEarningsEventsCall", ("get_earnings_events", RecentCompanyInformationArguments)
)
GetCompanyProfileCall = _call_model(
    "GetCompanyProfileCall", ("get_company_profile", FundamentalDomainArguments)
)
GetProfitabilityCall = _call_model(
    "GetProfitabilityCall", ("get_profitability", FundamentalDomainArguments)
)
GetBalanceSheetCall = _call_model(
    "GetBalanceSheetCall", ("get_balance_sheet", FundamentalDomainArguments)
)
GetCashFlowCall = _call_model("GetCashFlowCall", ("get_cash_flow", FundamentalDomainArguments))

BUSINESS_TOOL_CALL_MODELS = {
    BusinessToolName.GET_STOCK_DATA: GetStockDataCall,
    BusinessToolName.GET_INDICATORS: GetIndicatorsCall,
    BusinessToolName.GET_STOCK_NEWS: GetStockNewsCall,
    BusinessToolName.GET_ANNOUNCEMENTS: GetAnnouncementsCall,
    BusinessToolName.GET_EARNINGS_EVENTS: GetEarningsEventsCall,
    BusinessToolName.GET_COMPANY_PROFILE: GetCompanyProfileCall,
    BusinessToolName.GET_PROFITABILITY: GetProfitabilityCall,
    BusinessToolName.GET_BALANCE_SHEET: GetBalanceSheetCall,
    BusinessToolName.GET_CASH_FLOW: GetCashFlowCall,
}

BusinessToolCall = Annotated[
    GetStockDataCall
    | GetIndicatorsCall
    | GetStockNewsCall
    | GetAnnouncementsCall
    | GetEarningsEventsCall
    | GetCompanyProfileCall
    | GetProfitabilityCall
    | GetBalanceSheetCall
    | GetCashFlowCall,
    Field(discriminator="name"),
]
BUSINESS_TOOL_CALL = TypeAdapter(BusinessToolCall)


class BusinessToolGap(Contract):
    domain: Text
    status: BusinessToolStatus
    reason: Text


class BusinessToolEvidence(Contract):
    evidence_id: Annotated[str, Field(pattern=r"^(ev_[0-9a-f]{64}|doc_[0-9a-f]{32})$")]
    label: Annotated[str, Field(min_length=1, max_length=160)]
    observed_on: CalendarDate | None = None
    source: Annotated[str, Field(min_length=1, max_length=160)]
    quality: Literal["unknown", "unverified", "valid", "invalid"] = "unknown"


class ToolCoverage(Contract):
    complete: bool
    sources: Annotated[tuple[str, ...], Field(max_length=20)] = ()
    start: CalendarDate | None = None
    end: CalendarDate | None = None
    last_synced_at: Timestamp | None = None
    note: Annotated[str | None, Field(max_length=500)] = None


class BusinessToolResult(Contract):
    request_id: RequestId
    tool: BusinessToolName
    status: BusinessToolStatus
    as_of: Timestamp
    items: Annotated[tuple[dict[str, Any], ...], Field(max_length=200)] = ()
    evidence: Annotated[tuple[BusinessToolEvidence, ...], Field(max_length=200)] = ()
    coverage: ToolCoverage
    gaps: Annotated[tuple[BusinessToolGap, ...], Field(max_length=20)] = ()

    @model_validator(mode="after")
    def consistent(self):
        if self.status == BusinessToolStatus.OK and (not self.items or self.gaps):
            raise ValueError("ok_result_requires_items_without_gaps")
        if self.status in {
            BusinessToolStatus.NO_DATA,
            BusinessToolStatus.SOURCE_UNAVAILABLE,
            BusinessToolStatus.PERMISSION_DENIED,
            BusinessToolStatus.REJECTED,
        } and (self.items or self.evidence):
            raise ValueError("unavailable_result_cannot_claim_data")
        if (
            self.status
            in {
                BusinessToolStatus.COVERAGE_INCOMPLETE,
                BusinessToolStatus.SOURCE_UNAVAILABLE,
                BusinessToolStatus.PERMISSION_DENIED,
                BusinessToolStatus.REJECTED,
            }
            and not self.gaps
        ):
            raise ValueError("unavailable_result_requires_gap")
        return self
