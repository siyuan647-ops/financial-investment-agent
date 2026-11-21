"""Step14 public API contracts and bounded query/task inputs."""

from datetime import date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from app.contracts.common import CalendarDate, Contract, Timestamp, TsCode


class Page(Contract):
    number: int
    size: int
    total: int
    has_more: bool


class DataMeta(Contract):
    as_of: Timestamp
    unit: Literal["standard", "raw"]
    adjustment: Literal["none", "front", "back"] | None = None
    freshness: Literal["unknown", "fresh", "stale"]
    quality: Literal["unknown", "unverified", "valid", "invalid"]
    data_versions: tuple[str, ...]
    version_token: str
    cache: Literal["hit", "miss", "bypass"]


class DataPage(Contract):
    request_id: str
    data: tuple[dict, ...]
    page: Page
    meta: DataMeta


class BackfillRequest(Contract):
    codes: Annotated[tuple[TsCode, ...], Field(min_length=1, max_length=30)]
    start: CalendarDate
    end: CalendarDate
    window_days: Annotated[int, Field(ge=1, le=46)] = 31

    @model_validator(mode="after")
    def bounded(self):
        if len(set(self.codes)) != len(self.codes):
            raise ValueError("unique_codes_required")
        if self.start > self.end or (self.end - self.start).days > 1096:
            raise ValueError("backfill_range_up_to_three_years")
        return self


class GapFillRequest(Contract):
    codes: Annotated[tuple[TsCode, ...], Field(min_length=1, max_length=30)]
    start: CalendarDate
    end: CalendarDate

    @model_validator(mode="after")
    def valid(self):
        if len(set(self.codes)) != len(self.codes):
            raise ValueError("unique_codes_required")
        if self.start > self.end:
            raise ValueError("invalid_gap_fill_range")
        return self


class RetryRequest(Contract):
    batch_id: UUID


class TaskReceipt(Contract):
    task_id: UUID
    status: Literal["submitting", "submitted", "submission_failed"]
    duplicate: bool


class TaskView(Contract):
    task_id: UUID
    owner_id: str
    kind: Literal["backfill", "gap_fill", "retry"]
    status: str
    deployment: str
    flow_run_id: UUID | None
    parameters: dict
    result: dict
    created_at: datetime
    updated_at: datetime


class LineageQuery(Contract):
    node_type: Literal["raw", "standard", "derived", "evidence", "report", "alert"]
    reference_key: str


def quarter_ends(start: date, end: date) -> list[date]:
    if start > end:
        raise ValueError("invalid_financial_range")
    values = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            value = date(year, month, day)
            if start <= value <= end:
                values.append(value)
    if not 1 <= len(values) <= 12:
        raise ValueError("financial_range_requires_one_to_twelve_quarters")
    return values
