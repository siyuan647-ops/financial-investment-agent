"""Target-only entry contract for five-role research."""

from typing import Annotated
from uuid import UUID

from pydantic import Field

from app.contracts.analysis import SecurityCandidate
from app.contracts.common import Contract, RequestId, Text, Timestamp
from app.contracts.research import ResearchStatus


class UnifiedAnalysisRequest(Contract):
    target: Annotated[str, Field(min_length=1, max_length=100, pattern=r"\S")]
    as_of: Timestamp | None = None


class UnifiedAnalysisResponse(Contract):
    request_id: RequestId
    target: Text
    as_of: Timestamp
    security: SecurityCandidate
    task_id: UUID
    task_status: ResearchStatus
    deduplicated: bool
