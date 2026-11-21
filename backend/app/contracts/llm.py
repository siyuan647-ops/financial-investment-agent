"""Contracts for bounded, observable language-model calls."""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ModelPurpose(StrEnum):
    PLAN = "plan"
    GENERATE = "generate"
    VERIFY = "verify"


class LLMMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, max_length=200_000)


class TokenUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_tokens: int = Field(ge=0, le=1_000_000_000)
    output_tokens: int = Field(ge=0, le=1_000_000_000)
    total_tokens: int = Field(ge=0, le=1_000_000_000)

    @model_validator(mode="after")
    def validate_total(self):
        if self.total_tokens < self.input_tokens + self.output_tokens:
            raise ValueError("total_tokens cannot be smaller than input plus output")
        return self


class LLMCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invocation_id: UUID
    purpose: ModelPurpose
    model: str
    content: str
    finish_reason: str | None = None
    usage: TokenUsage | None = None
    estimated_cost: Decimal | None = Field(default=None, ge=0)
    cost_currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")

    @model_validator(mode="after")
    def validate_cost_pair(self):
        if (self.estimated_cost is None) != (self.cost_currency is None):
            raise ValueError("estimated_cost and cost_currency must be known together")
        return self


class LLMToolCall(BaseModel):
    """One validated OpenAI-compatible function call returned by a model."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:-]+$")
    name: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:-]+$")
    arguments: dict[str, Any]


class LLMStreamEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["delta", "completed"]
    delta: str | None = None
    completion: LLMCompletion | None = None

    @model_validator(mode="after")
    def validate_shape(self):
        if self.kind == "delta" and (self.delta is None or self.completion is not None):
            raise ValueError("delta event requires only delta")
        if self.kind == "completed" and (self.completion is None or self.delta is not None):
            raise ValueError("completed event requires only completion")
        return self


@dataclass(frozen=True)
class StructuredCompletion[StructuredValue: BaseModel]:
    completion: LLMCompletion
    parsed: StructuredValue


@dataclass(frozen=True)
class ToolCompletion:
    completion: LLMCompletion
    tool_calls: tuple[LLMToolCall, ...]
