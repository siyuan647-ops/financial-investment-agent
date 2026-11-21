"""Versioned records for the five directly obtainable fundamental domains."""

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from app.contracts.common import CalendarDate, Contract, Provenance, Text
from app.contracts.domain import SecurityKey

FundamentalDomain = Literal[
    "company_profile",
    "profitability",
    "balance_sheet",
    "cash_flow",
    "auxiliary",
]
DirectValue = Decimal | int | str | bool


class FundamentalRecord(Contract):
    provenance: Provenance
    security: SecurityKey
    domain: FundamentalDomain
    record_type: Text = Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")
    source_key: Text
    data_date: CalendarDate
    period_end: CalendarDate | None = None
    announced_on: CalendarDate | None = None
    effective_on: CalendarDate | None = None
    values: dict[str, DirectValue] = Field(default_factory=dict)
    origin: Literal["direct", "calculated"] = "direct"
    formula: Text | None = None
    input_versions: tuple[str, ...] = ()

    @model_validator(mode="after")
    def dates_and_origin(self):
        fetched = self.provenance.timing.fetched_at.date()
        if self.data_date > fetched:
            raise ValueError("data_date_after_fetch")
        if self.announced_on:
            if self.announced_on > fetched:
                raise ValueError("announcement_after_fetch")
        if not self.values:
            raise ValueError("fundamental_record_requires_obtained_values")
        if any(value is None or isinstance(value, float) for value in self.values.values()):
            raise ValueError("null_or_binary_float_not_obtained_value")
        if self.origin == "direct" and (self.formula is not None or self.input_versions):
            raise ValueError("direct_record_cannot_claim_calculation")
        if self.origin == "calculated" and (not self.formula or not self.input_versions):
            raise ValueError("calculation_requires_formula_and_inputs")
        return self


DOMAIN_TABLES = {
    "company_profile": "company_profile_record",
    "profitability": "profitability_record",
    "balance_sheet": "balance_sheet_record",
    "cash_flow": "cash_flow_record",
    "auxiliary": "fundamental_aux_record",
}


def canonical_data_date(
    *,
    fetched_on: date,
    period_end: date | None,
    announced_on: date | None,
    effective_on: date | None,
) -> date:
    """Business date is explicit; disclosure wins, then effective/period, then observation."""
    if announced_on:
        return announced_on
    if period_end:
        return period_end
    if effective_on and effective_on <= fetched_on:
        return effective_on
    return fetched_on
