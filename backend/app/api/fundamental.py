"""Read-only as-of API for the five fundamental business domains."""

from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import AwareDatetime

from app.api.auth import current_identity
from app.api.dependencies import Database
from app.contracts.common import SHANGHAI, AsOfQuery
from app.repositories.fundamental import FundamentalRepository

router = APIRouter(
    prefix="/api/v1/fundamental",
    tags=["fundamental"],
    dependencies=[Depends(current_identity)],
)
Domain = Literal["company_profile", "profitability", "balance_sheet", "cash_flow", "auxiliary"]
Code = Annotated[str, Path(pattern=r"^[0-9]{6}\.(SH|SZ)$")]


@router.get("/{domain}/{code}")
async def fundamental_records(
    request: Request,
    database: Database,
    domain: Domain,
    code: Code,
    record_type: Annotated[str | None, Query(pattern=r"^[a-z][a-z0-9_]{0,79}$")] = None,
    start: date | None = None,
    end: date | None = None,
    as_of: AwareDatetime | None = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
):
    records = await FundamentalRepository(database).records(
        domain,
        code,
        AsOfQuery(
            request_id=request.state.request_id,
            as_of=as_of or datetime.now(SHANGHAI),
            knowledge="public",
        ),
        record_type=record_type,
        start=start,
        end=end,
        limit=limit,
    )
    return {
        "request_id": request.state.request_id,
        "domain": domain,
        "ts_code": code,
        "as_of": (as_of or datetime.now(SHANGHAI)).isoformat(),
        "records": [record.model_dump(mode="json") for record in records],
    }
