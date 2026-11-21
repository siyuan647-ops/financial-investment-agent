"""Step17 contracts for bounded standard tools and evidence-backed facts."""

from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, TypeAdapter, model_validator

from app.contracts.common import (
    CalendarDate,
    Contract,
    DataStatus,
    Digest,
    Number,
    Provenance,
    RequestId,
    Text,
    Timestamp,
    TsCode,
    Unit,
)
from app.contracts.domain import Evidence


class ToolName(StrEnum):
    QUOTE = "quote"
    BARS = "bars"
    VALUATION = "valuation"
    FINANCIALS = "financials"
    DIVIDENDS = "dividends"
    INDUSTRY = "industry"
    CALCULATION = "calculation"


class _AsOfArguments(Contract):
    code: TsCode
    as_of: Timestamp | None = None


class QuoteArguments(_AsOfArguments):
    on: CalendarDate


class BarsArguments(_AsOfArguments):
    start: CalendarDate
    end: CalendarDate
    limit: Annotated[int, Field(ge=1, le=200)] = 100

    @model_validator(mode="after")
    def bounded_window(self):
        if self.start > self.end or (self.end - self.start).days > 1096:
            raise ValueError("bars_range_up_to_three_years")
        return self


class ValuationArguments(_AsOfArguments):
    start: CalendarDate
    end: CalendarDate
    limit: Annotated[int, Field(ge=1, le=100)] = 100

    @model_validator(mode="after")
    def bounded_window(self):
        if self.start > self.end or (self.end - self.start).days > 1096:
            raise ValueError("valuation_range_up_to_three_years")
        return self


class FinancialArguments(_AsOfArguments):
    start: CalendarDate
    end: CalendarDate
    report_type: Annotated[str, Field(min_length=1, max_length=10, pattern=r"^[0-9]+$")] = "1"
    limit: Annotated[int, Field(ge=1, le=120)] = 120

    @model_validator(mode="after")
    def bounded_window(self):
        if self.start > self.end or (self.end - self.start).days > 1096:
            raise ValueError("financial_range_up_to_three_years")
        return self


class DividendArguments(_AsOfArguments):
    start: CalendarDate | None = None
    end: CalendarDate | None = None
    limit: Annotated[int, Field(ge=1, le=100)] = 100

    @model_validator(mode="after")
    def paired_window(self):
        if (self.start is None) != (self.end is None):
            raise ValueError("dividend_start_and_end_must_be_paired")
        if self.start and (self.start > self.end or (self.end - self.start).days > 1096):
            raise ValueError("dividend_range_up_to_three_years")
        return self


class IndustryArguments(_AsOfArguments):
    on: CalendarDate | None = None
    classification: Literal["provider_basic", "SW"] | None = None
    limit: Annotated[int, Field(ge=1, le=20)] = 20


class CalculationArguments(_AsOfArguments):
    period_end: CalendarDate
    metric: Literal[
        "net_profit_margin",
        "liability_ratio",
        "operating_cash_flow_margin",
        "operating_cash_flow_to_profit",
    ]


class QuoteCall(Contract):
    name: Literal["quote"] = "quote"
    request_id: RequestId
    arguments: QuoteArguments


class BarsCall(Contract):
    name: Literal["bars"] = "bars"
    request_id: RequestId
    arguments: BarsArguments


class ValuationCall(Contract):
    name: Literal["valuation"] = "valuation"
    request_id: RequestId
    arguments: ValuationArguments


class FinancialsCall(Contract):
    name: Literal["financials"] = "financials"
    request_id: RequestId
    arguments: FinancialArguments


class DividendsCall(Contract):
    name: Literal["dividends"] = "dividends"
    request_id: RequestId
    arguments: DividendArguments


class IndustryCall(Contract):
    name: Literal["industry"] = "industry"
    request_id: RequestId
    arguments: IndustryArguments


class CalculationCall(Contract):
    name: Literal["calculation"] = "calculation"
    request_id: RequestId
    arguments: CalculationArguments


StandardToolCall = Annotated[
    QuoteCall
    | BarsCall
    | ValuationCall
    | FinancialsCall
    | DividendsCall
    | IndustryCall
    | CalculationCall,
    Field(discriminator="name"),
]
STANDARD_TOOL_CALL = TypeAdapter(StandardToolCall)


class DataApiRequest(Contract):
    path: Annotated[str, Field(pattern=r"^/api/v1/data/")]
    parameters: dict[str, str | int | bool]


class ToolBudget(Contract):
    max_calls: Annotated[int, Field(ge=1, le=100)] = 8
    max_records: Annotated[int, Field(ge=1, le=10_000)] = 500
    max_evidence: Annotated[int, Field(ge=1, le=50_000)] = 2_000
    max_context_facts: Annotated[int, Field(ge=1, le=10_000)] = 300


class EvidenceEnvelope(Contract):
    evidence: Evidence
    sources: Annotated[tuple[Provenance, ...], Field(min_length=1)]
    transformation: Text
    expression: Text | None = None

    @model_validator(mode="after")
    def complete_lineage(self):
        versions = tuple(sorted({item.data_version for item in self.sources}))
        if versions != tuple(sorted(set(self.evidence.record_versions))):
            raise ValueError("evidence_sources_do_not_match_record_versions")
        if self.evidence.kind == "derived" and self.expression is None:
            raise ValueError("derived_evidence_requires_expression")
        return self


class NumericFact(Contract):
    kind: Literal["numeric"] = "numeric"
    field: Text
    value: Number
    unit: Unit
    raw_value: Number
    raw_unit: Text
    evidence_id: Annotated[str, Field(pattern=r"^ev_[0-9a-f]{64}$")]


class TextFact(Contract):
    kind: Literal["text"] = "text"
    field: Text
    value: Annotated[str, Field(min_length=1, max_length=500)]
    evidence_id: Annotated[str, Field(pattern=r"^ev_[0-9a-f]{64}$")]
    untrusted_source_text: Literal[True] = True


Fact = Annotated[NumericFact | TextFact, Field(discriminator="kind")]


class DataGap(Contract):
    field: Text
    status: DataStatus
    reason: Text
    locator: Text | None = None


class DataReference(Contract):
    tool: ToolName
    api_path: Annotated[str, Field(pattern=r"^/api/v1/data/")]
    row_count: Annotated[int, Field(ge=1)]
    as_of: Timestamp
    version_token: Digest
    data_versions: tuple[Digest, ...]
    freshness: Literal["unknown", "fresh", "stale"]
    quality: Literal["unknown", "unverified", "valid", "invalid"]
    note: Literal["query_reference_only; values_are_not_in_llm_context"] = (
        "query_reference_only; values_are_not_in_llm_context"
    )


class FactAnswer(Contract):
    request_id: RequestId
    tool: ToolName
    status: Literal["verified", "partial", "reference", "no_data", "rejected"]
    facts: tuple[Fact, ...] = ()
    evidence: tuple[EvidenceEnvelope, ...] = ()
    gaps: tuple[DataGap, ...] = ()
    references: tuple[DataReference, ...] = ()

    @model_validator(mode="after")
    def citations_are_closed(self):
        identifiers = [item.evidence.evidence_id for item in self.evidence]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("duplicate_evidence_id")
        if any(item.evidence_id not in identifiers for item in self.facts):
            raise ValueError("fact_requires_local_evidence")
        if self.status in {"no_data", "rejected"} and (self.facts or self.evidence):
            raise ValueError("unavailable_answer_cannot_claim_evidence")
        if self.status == "reference" and (self.facts or self.evidence or not self.references):
            raise ValueError("reference_answer_must_only_contain_query_reference")
        if self.status == "verified" and (not self.facts or self.gaps or self.references):
            raise ValueError("verified_answer_requires_complete_local_facts")
        return self


def serialize_parameter(value: date | datetime | StrEnum | str | int | bool | None):
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    return value
