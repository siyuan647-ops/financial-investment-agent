"""Versioned deterministic technical-indicator records."""

from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from app.contracts.common import CalendarDate, Contract, Digest, Provenance
from app.contracts.domain import SecurityKey

IndicatorName = Literal[
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


class TechnicalIndicatorRecord(Contract):
    security: SecurityKey
    trade_date: CalendarDate
    indicator: IndicatorName
    value: Decimal
    parameters: dict[str, int | Decimal] = Field(default_factory=dict)
    calculation_version: Literal["technical-indicator-v1"] = "technical-indicator-v1"
    input_versions: tuple[Digest, ...]
    provenance: Provenance

    @model_validator(mode="after")
    def valid_inputs(self):
        if not self.input_versions:
            raise ValueError("technical_indicator_requires_inputs")
        if len(set(self.input_versions)) != len(self.input_versions):
            raise ValueError("duplicate_technical_indicator_input")
        if self.trade_date > self.provenance.timing.fetched_at.date():
            raise ValueError("technical_indicator_after_fetch")
        return self


def indicator_periods() -> dict[str, int]:
    return {
        "close_50_sma": 50,
        "close_10_ema": 10,
        "macd": 26,
        "macds": 9,
        "macdh": 9,
        "rsi": 14,
        "boll": 20,
        "boll_ub": 20,
        "boll_lb": 20,
        "atr": 14,
        "vwma": 20,
    }
