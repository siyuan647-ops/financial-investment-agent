"""Shared wire contracts. No persistence, publication or provider I/O."""

import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

SHANGHAI = timezone(timedelta(hours=8), name="Asia/Shanghai")
Text = Annotated[str, Field(min_length=1, pattern=r"\S")]
RequestId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
TsCode = Annotated[str, Field(pattern=r"^[0-9]{6}\.(SH|SZ)$")]


def _decimal_input(value):
    if isinstance(value, (bool, float)):
        raise ValueError("decimal_requires_string_integer_or_Decimal")
    return value


Number = Annotated[
    Decimal, BeforeValidator(_decimal_input), Field(allow_inf_nan=False, max_digits=38)
]


def _date_input(value):
    if type(value) is date or (
        isinstance(value, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value)
    ):
        return value
    raise ValueError("expected_ISO_calendar_date")


CalendarDate = Annotated[date, BeforeValidator(_date_input)]


def _timestamp_input(value):
    if isinstance(value, datetime) or (isinstance(value, str) and "T" in value):
        return value
    raise ValueError("expected_ISO_aware_timestamp")


Timestamp = Annotated[
    AwareDatetime,
    BeforeValidator(_timestamp_input),
    AfterValidator(lambda value: value.astimezone(SHANGHAI)),
]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class DataStatus(StrEnum):
    OK = "ok"
    PERMISSION_DENIED = "permission_denied"
    SOURCE_FAILURE = "source_failure"
    NOT_DISCLOSED = "not_disclosed"
    NULL_VALUE = "null_value"
    NOT_REQUESTED = "not_requested"
    NO_DATA = "no_data"
    NOT_APPLICABLE = "not_applicable"
    QUALITY_ERROR = "quality_error"
    STALE = "stale"
    UNKNOWN = "unknown"


Unit = Literal["CNY", "CNY/share", "share", "ratio", "multiple", "factor"]
RawUnit = Literal[
    "CNY",
    "CNY/share",
    "share",
    "ratio",
    "multiple",
    "factor",
    "lot",
    "thousand_CNY",
    "ten_thousand_CNY",
    "ten_thousand_share",
    "percent",
]
UNIT_RULES = {
    "CNY": ("CNY", Decimal(1)),
    "CNY/share": ("CNY/share", Decimal(1)),
    "share": ("share", Decimal(1)),
    "ratio": ("ratio", Decimal(1)),
    "multiple": ("multiple", Decimal(1)),
    "factor": ("factor", Decimal(1)),
    "lot": ("share", Decimal(100)),
    "thousand_CNY": ("CNY", Decimal(1000)),
    "ten_thousand_CNY": ("CNY", Decimal(10000)),
    "ten_thousand_share": ("share", Decimal(10000)),
    "percent": ("ratio", Decimal("0.01")),
}


def scale(value: Decimal, multiplier: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return value * multiplier


class Measure(Contract):
    value: Number | None
    raw_value: Number | None
    unit: Unit
    raw_unit: RawUnit
    status: DataStatus
    reason: Text | None = None

    @model_validator(mode="after")
    def consistent(self):
        unit, multiplier = UNIT_RULES[self.raw_unit]
        if self.unit != unit:
            raise ValueError("unit_mismatch")
        if self.value is not None:
            if self.raw_value is None or self.value != scale(self.raw_value, multiplier):
                raise ValueError("conversion_mismatch")
            if self.status not in {DataStatus.OK, DataStatus.STALE, DataStatus.QUALITY_ERROR}:
                raise ValueError("status_cannot_have_value")
        elif self.status in {DataStatus.OK, DataStatus.STALE}:
            raise ValueError("status_requires_value")
        elif self.raw_value is not None and self.status != DataStatus.QUALITY_ERROR:
            raise ValueError("missing_status_cannot_have_raw_value")
        if self.status != DataStatus.OK and self.reason is None:
            raise ValueError("non_ok_requires_reason")
        return self


class Availability(Contract):
    """Exact disclosure must refer to this revision, never to an older filing."""

    fetched_at: Timestamp
    disclosed_on: CalendarDate | None = None
    disclosed_at: Timestamp | None = None
    basis: Literal["exact_disclosure", "date_only", "observed"]
    available_at: Timestamp

    @model_validator(mode="after")
    def conservative(self):
        if self.basis == "exact_disclosure":
            if self.disclosed_at is None:
                raise ValueError("exact_disclosure_required")
            expected = self.disclosed_at
            if self.disclosed_on and self.disclosed_at.date() != self.disclosed_on:
                raise ValueError("disclosure_date_mismatch")
        elif self.basis == "date_only":
            if self.disclosed_on is None or self.disclosed_at is not None:
                raise ValueError("date_only_requires_date")
            expected = datetime.combine(self.disclosed_on + timedelta(days=1), time(), SHANGHAI)
        else:
            if self.disclosed_at is not None:
                raise ValueError("observed_cannot_claim_exact_disclosure")
            expected = self.fetched_at
        if self.available_at != expected:
            raise ValueError("unsafe_availability")
        if self.disclosed_at and self.disclosed_at > self.fetched_at:
            raise ValueError("disclosure_after_fetch")
        if self.disclosed_on and self.disclosed_on > self.fetched_at.date():
            raise ValueError("disclosure_date_after_fetch")
        return self


class Provenance(Contract):
    provider: Text
    source_api: Text
    source_ref: Text
    raw_sha256: Digest
    request_id: RequestId
    data_version: Digest
    schema_version: Literal["1.0.0"] = "1.0.0"
    mapping_version: Literal[
        "step05-v1",
        "step07-v1",
        "step08-v1",
        "step09-v1",
        "step10-v1",
        "fundamental-v1",
        "fundamental-profitability-window-v1",
        "fundamental-analysis-window-v1",
        "technical-indicator-v1",
    ] = "step05-v1"
    timing: Availability
    quality: Literal["unverified", "valid", "invalid"] = "unverified"
    freshness: Literal["unknown", "fresh", "stale"] = "unknown"


class AsOfQuery(Contract):
    request_id: RequestId
    as_of: Timestamp
    knowledge: Literal["public", "system"] = "public"

    def includes(self, provenance: Provenance) -> bool:
        timing = provenance.timing
        return timing.available_at <= self.as_of and (
            self.knowledge == "public" or timing.fetched_at <= self.as_of
        )


class DataError(Contract):
    request_id: RequestId
    status: DataStatus
    reason_code: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")]
    source_api: Text | None = None
    retryable: bool = False

    @model_validator(mode="after")
    def failure_only(self):
        if self.status == DataStatus.OK:
            raise ValueError("error_cannot_be_ok")
        if self.retryable and self.status != DataStatus.SOURCE_FAILURE:
            raise ValueError("only_source_failure_may_retry")
        return self
