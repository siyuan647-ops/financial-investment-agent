"""A-share domain schema v1; unknown upstream semantics remain explicit."""

from datetime import date
from typing import Annotated, Literal

from pydantic import Field, model_validator

from app.contracts.common import (
    CalendarDate,
    Contract,
    Digest,
    Measure,
    Provenance,
    RequestId,
    Text,
    TsCode,
)

Exchange = Literal["SSE", "SZSE"]
PeriodBasis = Literal["instant", "quarter", "ytd", "annual", "ttm", "unknown"]
Scope = Literal["consolidated", "parent", "unknown"]


class SecurityKey(Contract):
    market: Literal["CN_A"] = "CN_A"
    exchange: Exchange
    ts_code: TsCode

    @model_validator(mode="after")
    def qualified_code(self):
        if self.ts_code[-2:] != {"SSE": "SH", "SZSE": "SZ"}[self.exchange]:
            raise ValueError("exchange_suffix_mismatch")
        return self


class Record(Contract):
    provenance: Provenance


class Security(Record):
    security: SecurityKey
    name: Text
    list_date: CalendarDate | None = None
    delist_date: CalendarDate | None = None
    listing_status: Literal["listed", "delisted", "paused", "unknown"] = "unknown"

    @model_validator(mode="after")
    def dates(self):
        if self.list_date and self.delist_date and self.delist_date < self.list_date:
            raise ValueError("delisting_before_listing")
        return self


class TradingCalendar(Record):
    exchange: Exchange
    calendar_date: CalendarDate
    is_open: Annotated[bool, Field(strict=True)]
    previous_open_date: CalendarDate | None = None

    @model_validator(mode="after")
    def previous_date(self):
        if self.previous_open_date and self.previous_open_date >= self.calendar_date:
            raise ValueError("previous_open_date_not_previous")
        return self


class SecurityAlias(Record):
    security: SecurityKey
    name: Text
    effective_from: CalendarDate
    effective_to: CalendarDate | None = None

    @model_validator(mode="after")
    def valid_interval(self):
        if self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("alias_end_before_start")
        return self


def require_units(pairs):
    for measure, unit in pairs:
        if measure.unit != unit:
            raise ValueError("domain_unit_mismatch")


class PriceBar(Record):
    security: SecurityKey
    trade_date: CalendarDate
    frequency: Literal["1d"] = "1d"
    adjustment: Literal["none"] = "none"
    open: Measure
    high: Measure
    low: Measure
    close: Measure
    volume: Measure
    amount: Measure

    @model_validator(mode="after")
    def units(self):
        require_units(
            [(getattr(self, key), "CNY/share") for key in ("open", "high", "low", "close")]
        )
        require_units([(self.volume, "share"), (self.amount, "CNY")])
        return self


class AdjustmentFactor(Record):
    security: SecurityKey
    trade_date: CalendarDate
    factor: Measure

    @model_validator(mode="after")
    def units(self):
        require_units([(self.factor, "factor")])
        return self


class Valuation(Record):
    security: SecurityKey
    trade_date: CalendarDate
    methodology: Literal["provider_reported"] = "provider_reported"
    pe: Measure
    pe_ttm: Measure
    pb: Measure
    total_market_cap: Measure
    float_market_cap: Measure
    dividend_yield: Measure
    dividend_yield_ttm: Measure

    @model_validator(mode="after")
    def units(self):
        require_units(
            [
                (self.pe, "multiple"),
                (self.pe_ttm, "multiple"),
                (self.pb, "multiple"),
                (self.total_market_cap, "CNY"),
                (self.float_market_cap, "CNY"),
                (self.dividend_yield, "ratio"),
                (self.dividend_yield_ttm, "ratio"),
            ]
        )
        return self


class FinancialPeriod(Contract):
    end: CalendarDate
    start: CalendarDate | None
    basis: PeriodBasis

    @model_validator(mode="after")
    def interval(self):
        if self.basis in {"instant", "unknown"}:
            if self.start is not None:
                raise ValueError("instant_or_unknown_has_no_start")
            return self
        if self.start is None or self.start > self.end:
            raise ValueError("period_start_required")
        if (self.end.month, self.end.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
            raise ValueError("quarter_end_required")
        if self.basis == "quarter":
            expected = date(self.end.year, self.end.month - 2, 1)
        elif self.basis in {"ytd", "annual"}:
            expected = date(self.end.year, 1, 1)
            if self.basis == "annual" and self.end.month != 12:
                raise ValueError("annual_requires_december")
        else:
            month = self.end.month % 12 + 1
            expected = date(self.end.year if month == 1 else self.end.year - 1, month, 1)
        if self.start != expected:
            raise ValueError("period_basis_mismatch")
        return self


FinancialMetric = Literal[
    "revenue",
    "total_revenue",
    "net_profit_parent",
    "net_profit_total",
    "net_profit_ex_nonrecurring",
    "total_assets",
    "total_liabilities",
    "operating_cash_flow",
    "investing_cash_flow",
    "financing_cash_flow",
]
STATEMENTS = {
    "revenue": "income",
    "total_revenue": "income",
    "net_profit_parent": "income",
    "net_profit_total": "income",
    "net_profit_ex_nonrecurring": "fina_indicator",
    "total_assets": "balancesheet",
    "total_liabilities": "balancesheet",
    "operating_cash_flow": "cashflow",
    "investing_cash_flow": "cashflow",
    "financing_cash_flow": "cashflow",
}

# Type 11 has ambiguous upstream wording; it is deliberately not inferred.
REPORT_TYPES = {
    "1": ("consolidated", "cumulative", "reported"),
    "2": ("consolidated", "quarter", "reported"),
    "3": ("consolidated", "quarter", "adjusted"),
    "4": ("consolidated", "cumulative", "adjusted"),
    "5": ("consolidated", "cumulative", "pre_adjustment"),
    "6": ("parent", "cumulative", "reported"),
    "7": ("parent", "quarter", "reported"),
    "8": ("parent", "quarter", "adjusted"),
    "9": ("parent", "cumulative", "adjusted"),
    "10": ("parent", "cumulative", "pre_adjustment"),
    "12": ("parent", "cumulative", "pre_adjustment"),
}


class Calculation(Contract):
    formula: Text
    input_versions: Annotated[tuple[Digest, ...], Field(min_length=1)]


class FinancialFact(Record):
    security: SecurityKey
    metric: FinancialMetric
    statement: Literal["income", "balancesheet", "cashflow", "fina_indicator"]
    period: FinancialPeriod
    scope: Scope
    company_type: Literal[
        "industrial", "bank", "insurance", "securities", "other_finance", "unknown"
    ]
    report_type: Text | None
    update_flag: Text | None
    announced_on: CalendarDate | None = None
    actual_announced_on: CalendarDate | None = None
    revision: Literal["reported", "adjusted", "pre_adjustment", "unknown"]
    supersedes_version: Digest | None = None
    amount: Measure
    calculation: Calculation | None = None

    @model_validator(mode="after")
    def semantics(self):
        require_units([(self.amount, "CNY")])
        if STATEMENTS[self.metric] != self.statement:
            raise ValueError("metric_statement_mismatch")
        if self.statement == "balancesheet" and self.period.basis != "instant":
            raise ValueError("balance_sheet_is_instant")
        if self.statement != "balancesheet" and self.period.basis == "instant":
            raise ValueError("flow_is_not_instant")
        if self.calculation is not None:
            if self.report_type is not None or self.scope == "unknown":
                raise ValueError("calculation_requires_known_scope_and_no_report_type")
            if self.provenance.data_version in self.calculation.input_versions:
                raise ValueError("calculation_cannot_reference_itself")
        elif self.statement != "fina_indicator":
            semantics = REPORT_TYPES.get(self.report_type)
            if semantics:
                scope, basis, revision = semantics
                if self.scope != scope or self.revision != revision:
                    raise ValueError("report_type_semantics_mismatch")
                if self.statement != "balancesheet":
                    expected = (
                        basis
                        if basis == "quarter"
                        else ("annual" if self.period.end.month == 12 else "ytd")
                    )
                    if self.period.basis != expected:
                        raise ValueError("report_type_period_mismatch")
            elif (
                self.scope != "unknown"
                or self.revision != "unknown"
                or (self.statement != "balancesheet" and self.period.basis != "unknown")
            ):
                raise ValueError("unknown_report_type_requires_unknown_semantics")
        incomplete = (
            self.scope == "unknown" or self.period.basis == "unknown" or self.revision == "unknown"
        )
        if incomplete and self.provenance.quality == "valid":
            raise ValueError("unknown_semantics_cannot_be_valid")
        if self.supersedes_version == self.provenance.data_version:
            raise ValueError("revision_cannot_replace_itself")
        return self


class CorporateAction(Record):
    security: SecurityKey
    event_id: Text
    action: Literal["dividend"] = "dividend"
    report_end: CalendarDate
    announced_on: CalendarDate | None
    stage: Literal["proposed", "approved", "implemented", "cancelled", "unknown"]
    raw_stage: Text | None
    cash_after_tax: Measure
    cash_before_tax: Measure
    stock_distribution: Measure
    record_date: CalendarDate | None
    ex_date: CalendarDate | None
    pay_date: CalendarDate | None
    # A scheduled pay date or 'implemented' never proves that payment happened.
    payment_status: Literal["unverified"] = "unverified"

    @model_validator(mode="after")
    def units(self):
        require_units(
            [
                (self.cash_after_tax, "CNY/share"),
                (self.cash_before_tax, "CNY/share"),
                (self.stock_distribution, "ratio"),
            ]
        )
        return self


class IndustryMembership(Record):
    security: SecurityKey
    classification: Literal["provider_basic", "SW"]
    classification_version: Text | None
    level: Literal[1, 2, 3] | None
    industry_code: Text | None
    industry_name: Text
    effective_from: CalendarDate | None
    effective_to: CalendarDate | None
    history: Literal["current_snapshot", "interval_unverified", "verified_interval"]

    @model_validator(mode="after")
    def interval(self):
        if self.effective_from and self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("industry_interval_reversed")
        if self.history == "current_snapshot" and (self.effective_from or self.effective_to):
            raise ValueError("snapshot_cannot_claim_interval")
        if self.history == "verified_interval" and (
            self.effective_from is None or self.classification_version is None
        ):
            raise ValueError("verified_history_requires_date_and_version")
        if self.classification == "SW" and (self.level is None or self.industry_code is None):
            raise ValueError("sw_requires_level_and_code")
        return self


class Evidence(Record):
    evidence_id: Annotated[str, Field(pattern=r"^ev_[0-9a-f]{64}$")]
    request_id: RequestId
    kind: Literal["structured", "document", "derived"]
    record_versions: Annotated[tuple[Digest, ...], Field(min_length=1)]
    locator: Text
    input_evidence_ids: tuple[Annotated[str, Field(pattern=r"^ev_[0-9a-f]{64}$")], ...] = ()
    formula: Text | None = None

    @model_validator(mode="after")
    def derivation(self):
        if self.kind == "derived" and (not self.input_evidence_ids or self.formula is None):
            raise ValueError("derived_evidence_requires_inputs_and_formula")
        if self.kind != "derived" and (self.input_evidence_ids or self.formula is not None):
            raise ValueError("source_evidence_cannot_claim_derivation")
        if self.evidence_id in self.input_evidence_ids:
            raise ValueError("self_referencing_evidence")
        return self
