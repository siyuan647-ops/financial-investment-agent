"""Contracts for durable, bounded specialist research tasks."""

from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, JsonValue, model_validator

from app.contracts.analysis import FastMetric, MetricResult, SecurityCandidate
from app.contracts.business_tools import BusinessToolResult
from app.contracts.common import CalendarDate, Contract, Text, Timestamp
from app.contracts.evidence import DataGap, FactAnswer


class ResearchStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ResearchPhase(StrEnum):
    QUEUED = "queued"
    PLAN = "plan"
    EXECUTE = "execute"
    REFLECT = "reflect"
    GENERATE = "generate"
    VERIFY = "verify"
    COMPLETED = "completed"


class ResearchRequest(Contract):
    question: Annotated[str, Field(min_length=1, max_length=4_000, pattern=r"\S")]
    as_of: Timestamp | None = None


class ResearchPlan(Contract):
    target: Annotated[str, Field(min_length=1, max_length=100)]
    metrics: Annotated[tuple[FastMetric, ...], Field(min_length=1, max_length=8)]
    supplemental_metrics: Annotated[tuple[FastMetric, ...], Field(max_length=4)] = ()
    price_date: CalendarDate | None = None
    period_end: CalendarDate | None = None
    thesis: Annotated[str, Field(min_length=1, max_length=1_000, pattern=r"\S")]
    reason_code: Annotated[
        str, Field(pattern=r"^[a-z][a-z0-9_]{0,79}$", min_length=1, max_length=80)
    ]
    document_query: Annotated[str | None, Field(min_length=1, max_length=1_000)] = None

    @model_validator(mode="after")
    def valid_plan(self):
        requested = (*self.metrics, *self.supplemental_metrics)
        if len(set(requested)) != len(requested):
            raise ValueError("research_plan_metrics_must_be_unique")
        return self


class ResearchFinding(Contract):
    statement: Annotated[str, Field(min_length=1, max_length=2_000, pattern=r"\S")]
    evidence_ids: Annotated[tuple[str, ...], Field(min_length=1, max_length=20)]


class ResearchReport(Contract):
    title: Annotated[str, Field(min_length=1, max_length=200, pattern=r"\S")]
    summary: Annotated[str, Field(min_length=1, max_length=4_000, pattern=r"\S")]
    markdown: Annotated[str, Field(max_length=50_000)] = ""
    findings: Annotated[tuple[ResearchFinding, ...], Field(max_length=20)] = ()
    limitations: Annotated[tuple[str, ...], Field(max_length=20)] = ()


class RoleStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    SKIPPED = "skipped"


class AssessmentClaim(Contract):
    statement: Annotated[str, Field(min_length=1, max_length=2_000, pattern=r"\S")]
    evidence_ids: Annotated[tuple[str, ...], Field(min_length=1, max_length=20)]


class SpecialistRole(StrEnum):
    FUNDAMENTAL = "fundamental"
    NEWS = "news"
    MARKET = "market"
    INVESTMENT = "investment"
    RISK = "risk"


class SpecialistAssessment(Contract):
    role: Literal["fundamental", "news", "market"]
    summary: Annotated[str, Field(min_length=1, max_length=4_000, pattern=r"\S")]
    report_markdown: Annotated[str, Field(min_length=1, max_length=30_000, pattern=r"\S")]
    coverage_status: Literal["complete", "partial", "coverage_incomplete", "source_unavailable"]
    claims: Annotated[tuple[AssessmentClaim, ...], Field(max_length=20)] = ()
    limitations: Annotated[tuple[str, ...], Field(max_length=20)] = ()


class SpecialistDecision(Contract):
    action: Literal["tool", "finish"]
    tool_call: dict | None = None
    assessment: SpecialistAssessment | None = None

    @model_validator(mode="after")
    def valid_action(self):
        if self.action == "tool" and (self.tool_call is None or self.assessment is not None):
            raise ValueError("tool_action_requires_only_tool_call")
        if self.action == "finish" and (self.assessment is None or self.tool_call is not None):
            raise ValueError("finish_action_requires_only_assessment")
        return self


class SpecialistToolBatchDecision(Contract):
    tool_calls: Annotated[tuple[dict, ...], Field(max_length=4)] = ()


class RiskSynthesis(Contract):
    upstream_complete: bool
    risk_level: Literal["low", "medium", "high", "unknown"]
    summary: Annotated[str, Field(min_length=1, max_length=4_000, pattern=r"\S")]
    risks: Annotated[tuple[AssessmentClaim, ...], Field(max_length=20)] = ()
    data_gaps: Annotated[tuple[str, ...], Field(max_length=20)] = ()

    @model_validator(mode="after")
    def conservative(self):
        if (not self.upstream_complete or self.data_gaps) and self.risk_level == "low":
            raise ValueError("incomplete_risk_synthesis_cannot_be_low")
        if not _meaningful_text(self.summary, minimum=4):
            raise ValueError("risk_summary_not_meaningful")
        return self


InvestmentPlanRecommendation = Literal["Buy", "Overweight", "Hold", "Underweight", "Sell"]


class InvestmentAnalysisTransport(Contract):
    """Minimal model-facing structure; business quality is validated separately."""

    recommendation: InvestmentPlanRecommendation = Field(
        description=(
            "投资建议，恰好五选一 Buy / Overweight / Hold / Underweight / Sell；"
            "比较多空证据并选择更强的一方。"
        )
    )
    rationale: str = Field(description="综合三个上游报告形成的详细投资理由。")
    limitations: tuple[str, ...] = Field(
        description="分析限制列表；即使只有一项也必须使用字符串数组。"
    )
    strategic_actions: str = Field(description="给交易员的具体执行步骤及相对标准仓位 sizing 指引。")


class InvestmentAnalysisDraft(Contract):
    """Internal structured result rendered to Markdown before leaving the agent."""

    recommendation: InvestmentPlanRecommendation = Field(
        description=(
            "投资建议，恰好五选一 Buy / Overweight / Hold / Underweight / Sell。"
            "冲突本身不是 Hold 的理由；比较双方证据并选择更强一方，只有权衡后仍然平衡"
            "或证据太少时才使用 Hold。"
        )
    )
    rationale: Annotated[str, Field(min_length=20, max_length=8_000, pattern=r"\S")] = Field(
        description=(
            "详细理由：综合基本面、新闻面与技术面，权衡多空证据、证据质量、"
            "主要驱动、关键不确定性及评级依据。"
        )
    )
    limitations: Annotated[
        tuple[Annotated[str, Field(min_length=4, max_length=2_000, pattern=r"\S")], ...],
        Field(min_length=1, max_length=20),
    ] = Field(
        description=(
            "分析限制，包括上游缺失、source_unavailable、coverage_incomplete、"
            "数据冲突和未经验证的推断；缺失信息不得支持任一方向。"
        )
    )
    strategic_actions: Annotated[str, Field(min_length=20, max_length=4_000, pattern=r"\S")] = (
        Field(
            description=(
                "给交易员执行建议的具体步骤，包含相对标准仓位的 sizing 指引。"
                "研究团队看不到调用者的持仓，因此不得假设实际仓位或生成个性化配置。"
            )
        )
    )

    @model_validator(mode="after")
    def meaningful_investment_plan(self):
        if not _meaningful_text(self.rationale, minimum=20):
            raise ValueError("investment_rationale_not_meaningful")
        if not _meaningful_text(self.strategic_actions, minimum=20):
            raise ValueError("investment_strategic_actions_not_meaningful")
        if any(not _meaningful_text(item, minimum=4) for item in self.limitations):
            raise ValueError("investment_limitation_not_meaningful")
        return self


class ToolAttemptRecord(Contract):
    sequence: Annotated[int, Field(ge=1, le=20)]
    invocation_id: UUID | None = None
    tool_name: Literal[
        "get_stock_data",
        "get_indicators",
        "get_stock_news",
        "get_announcements",
        "get_earnings_events",
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
        "unrecognized",
    ]
    argument_keys: Annotated[tuple[str, ...], Field(max_length=20)] = ()
    sanitized_arguments: dict[str, JsonValue] = Field(default_factory=dict)
    stage: Literal["selection", "security_injection", "schema_validation", "execution", "completed"]
    status: Literal["succeeded", "failed"]
    error_code: Annotated[str | None, Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")] = None

    @model_validator(mode="after")
    def valid_outcome(self):
        if self.status == "succeeded":
            if self.stage != "completed" or self.error_code is not None:
                raise ValueError("successful_tool_attempt_requires_completed_stage")
        elif self.stage == "completed" or self.error_code is None:
            raise ValueError("failed_tool_attempt_requires_stage_and_error")
        return self


class SpecialistInvocationTrace(Contract):
    role: SpecialistRole
    status: RoleStatus
    prompt_version: Annotated[str, Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")]
    tokens: Annotated[int, Field(ge=0, le=1_000_000_000)] = 0
    invocation_ids: Annotated[tuple[UUID, ...], Field(max_length=20)] = ()
    tool_calls: Annotated[int, Field(ge=0, le=20)] = 0
    tool_attempts: Annotated[tuple[ToolAttemptRecord, ...], Field(max_length=20)] = ()
    elapsed_ms: Annotated[int, Field(ge=0)] = 0
    error_code: Annotated[str | None, Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")] = None

    @model_validator(mode="after")
    def valid_trace(self):
        if self.status == RoleStatus.SUCCEEDED and self.error_code is not None:
            raise ValueError("successful_specialist_cannot_have_error")
        if self.status != RoleStatus.SUCCEEDED and self.error_code is None:
            raise ValueError("incomplete_specialist_requires_error")
        return self


class SpecialistCollaborationResult(Contract):
    fundamental: SpecialistAssessment | None = None
    news: SpecialistAssessment | None = None
    market: SpecialistAssessment | None = None
    investment: Annotated[str, Field(min_length=1, max_length=40_000, pattern=r"\S")] | None = None
    risk: Annotated[str, Field(min_length=1, max_length=40_000, pattern=r"\S")] | None = None
    tool_results: Annotated[tuple[BusinessToolResult, ...], Field(max_length=20)] = ()
    traces: Annotated[tuple[SpecialistInvocationTrace, ...], Field(min_length=5, max_length=5)]

    @model_validator(mode="after")
    def valid_roles(self):
        by_role = {item.role: item for item in self.traces}
        if set(by_role) != set(SpecialistRole):
            raise ValueError("specialist_collaboration_requires_three_roles")
        values = {
            SpecialistRole.FUNDAMENTAL: self.fundamental,
            SpecialistRole.NEWS: self.news,
            SpecialistRole.MARKET: self.market,
            SpecialistRole.INVESTMENT: self.investment,
            SpecialistRole.RISK: self.risk,
        }
        for role in (
            SpecialistRole.FUNDAMENTAL,
            SpecialistRole.NEWS,
            SpecialistRole.MARKET,
        ):
            if values[role] is None:
                raise ValueError("specialist_assessment_required_even_when_degraded")
        for role in (SpecialistRole.INVESTMENT, SpecialistRole.RISK):
            value = values[role]
            if (by_role[role].status == RoleStatus.SUCCEEDED) != (value is not None):
                raise ValueError("specialist_trace_and_result_mismatch")
        if self.risk is not None and self.investment is None:
            raise ValueError("risk_review_requires_investment_analysis")
        return self


class AgentAuditRecord(Contract):
    role: Literal["fundamental", "news", "market", "investment", "risk", "final"]
    prompt_version: Annotated[str, Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")]
    status: RoleStatus
    input_payload: dict[str, JsonValue]
    output_payload: dict[str, JsonValue] | None = None
    tools: Annotated[tuple[str, ...], Field(max_length=20)] = ()
    tool_attempts: Annotated[tuple[ToolAttemptRecord, ...], Field(max_length=20)] = ()
    tokens: Annotated[int, Field(ge=0, le=1_000_000_000)] = 0


class SemanticVerification(Contract):
    passed: bool
    unsupported_statements: Annotated[tuple[str, ...], Field(max_length=20)] = ()
    reason_codes: Annotated[tuple[str, ...], Field(max_length=20)] = ()

    @model_validator(mode="after")
    def valid_verification(self):
        if self.passed and (self.unsupported_statements or self.reason_codes):
            raise ValueError("passed_verification_cannot_have_failures")
        if not self.passed and not self.reason_codes:
            raise ValueError("failed_verification_requires_reason")
        return self


class ResearchVerification(Contract):
    citations_closed: bool
    numeric_values_valid: bool
    locators_match_plan: bool
    semantic: SemanticVerification
    verified_metrics: Annotated[int, Field(ge=0, le=12)]
    requested_metrics: Annotated[int, Field(ge=0, le=12)]
    correction_attempts: Annotated[int, Field(ge=0, le=1)] = 0

    @property
    def passed(self):
        return (
            self.citations_closed
            and self.numeric_values_valid
            and self.locators_match_plan
            and self.semantic.passed
            and self.verified_metrics == self.requested_metrics
        )


class ResearchExecution(Contract):
    fact_answers: tuple[FactAnswer, ...] = ()
    results: tuple[MetricResult, ...] = ()
    gaps: tuple[DataGap, ...] = ()
    collaboration: SpecialistCollaborationResult | None = None
    agent_records: Annotated[tuple[AgentAuditRecord, ...], Field(max_length=5)] = ()


class ResearchTaskAccepted(Contract):
    task_id: UUID
    status: ResearchStatus
    deduplicated: bool


class ResearchEvent(Contract):
    sequence: Annotated[int, Field(ge=1)]
    phase: ResearchPhase
    status: ResearchStatus
    code: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")]
    occurred_at: Timestamp


class ResearchTask(Contract):
    task_id: UUID
    request_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    owner_id: Annotated[str, Field(min_length=1, max_length=64)]
    question: Annotated[str, Field(min_length=1, max_length=4_000)]
    as_of: Timestamp
    status: ResearchStatus
    phase: ResearchPhase
    cancel_requested: bool = False
    plan: ResearchPlan | None = None
    security: SecurityCandidate | None = None
    execution: ResearchExecution | None = None
    report: ResearchReport | None = None
    verification: ResearchVerification | None = None
    failure_code: Text | None = None
    events: tuple[ResearchEvent, ...] = ()
    created_at: Timestamp
    updated_at: Timestamp

    @model_validator(mode="after")
    def valid_terminal_task(self):
        if self.status == ResearchStatus.SUCCEEDED:
            if self.report is None or self.verification is None or not self.verification.passed:
                raise ValueError("succeeded_task_requires_verified_report")
        if self.status == ResearchStatus.PARTIAL and self.report is None:
            raise ValueError("partial_task_requires_report")
        if self.status == ResearchStatus.FAILED and self.failure_code is None:
            raise ValueError("failed_task_requires_code")
        if self.status == ResearchStatus.CANCELLED and not self.cancel_requested:
            raise ValueError("cancelled_task_requires_request")
        return self


def _meaningful_text(value: str, *, minimum: int) -> bool:
    compact = "".join(value.split())
    return len(compact) >= minimum and not compact.isdigit()
