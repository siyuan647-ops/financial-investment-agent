"""Security identity and metric facts shared by five-role research."""

from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from app.contracts.common import Contract, Text, TsCode
from app.contracts.evidence import DataGap, EvidenceEnvelope, NumericFact


class FastMetric(StrEnum):
    CLOSE = "close"
    PE = "pe"
    PE_TTM = "pe_ttm"
    PB = "pb"
    REVENUE = "revenue"
    TOTAL_REVENUE = "total_revenue"
    NET_PROFIT_PARENT = "net_profit_parent"
    NET_PROFIT_MARGIN = "net_profit_margin"
    LIABILITY_RATIO = "liability_ratio"
    OPERATING_CASH_FLOW_MARGIN = "operating_cash_flow_margin"
    OPERATING_CASH_FLOW_TO_PROFIT = "operating_cash_flow_to_profit"


class SecurityCandidate(Contract):
    code: TsCode
    name: Text
    exchange: Literal["SSE", "SZSE"]


class MetricResult(Contract):
    metric: FastMetric
    label: Text
    status: Literal["verified", "partial", "no_data", "rejected"]
    fact: NumericFact | None = None
    evidence: EvidenceEnvelope | None = None
    gaps: tuple[DataGap, ...] = ()

    @model_validator(mode="after")
    def closed_result(self):
        if (self.fact is None) != (self.evidence is None):
            raise ValueError("metric_fact_and_evidence_must_be_paired")
        if self.fact and self.fact.evidence_id != self.evidence.evidence.evidence_id:
            raise ValueError("metric_fact_requires_matching_evidence")
        if self.status == "verified" and (self.fact is None or self.gaps):
            raise ValueError("verified_metric_requires_complete_fact")
        if self.status in {"no_data", "rejected"} and self.fact is not None:
            raise ValueError("unavailable_metric_cannot_claim_fact")
        return self


class VerificationSummary(Contract):
    citations_closed: bool
    numeric_values_valid: bool
    locators_match_plan: bool
    verified_metrics: int = Field(ge=0, le=4)
    requested_metrics: int = Field(ge=0, le=4)
