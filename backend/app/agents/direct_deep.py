"""Target-only research: fixed specialists -> risk -> gated unverified report."""

import asyncio
from dataclasses import dataclass

from app.contracts.analysis import SecurityCandidate
from app.contracts.research import (
    AgentAuditRecord,
    ResearchExecution,
    ResearchPlan,
    ResearchReport,
    ResearchStatus,
    ResearchVerification,
    SemanticVerification,
)


@dataclass(frozen=True)
class DirectDeepBudget:
    max_seconds: float = 300
    max_tokens: int = 24_000


class ResearchCancelled(RuntimeError):
    pass


class ResearchBudgetExceeded(RuntimeError):
    pass


class DirectDeepResearchAgent:
    def __init__(self, collaboration, *, budget=None):
        self.collaboration = collaboration
        self.budget = budget or DirectDeepBudget()

    async def run(
        self,
        target,
        *,
        security,
        request_id,
        task_id,
        identity,
        as_of,
        progress=None,
        cancelled=None,
    ):
        del identity
        tokens = 0

        async def checkpoint(phase):
            if cancelled and await cancelled():
                raise ResearchCancelled("research_cancelled")
            if progress:
                await progress(phase, ResearchStatus.RUNNING)

        def add_tokens(value):
            nonlocal tokens
            tokens += value
            if tokens > self.budget.max_tokens:
                raise ResearchBudgetExceeded("research_token_budget_exceeded")

        async with asyncio.timeout(self.budget.max_seconds):
            await checkpoint("plan")
            security = SecurityCandidate.model_validate(security)
            context = ResearchPlan(
                target=security.code,
                metrics=("close",),
                thesis="固定职责基本面、新闻、技术面与风险研究。",
                reason_code="target_only_multi_agent",
            )

            await checkpoint("execute")
            objective = "对程序已解析的目标证券执行固定职责研究，不解释或替换目标。"
            collaboration, role_tokens = await self.collaboration.run(
                objective,
                context,
                security,
                ResearchExecution(),
                request_id=request_id,
                task_id=task_id,
                as_of=as_of,
            )
            add_tokens(role_tokens)
            records = _specialist_records(collaboration, security, as_of)
            execution = ResearchExecution(collaboration=collaboration, agent_records=records)

            await checkpoint("generate")
            report = _collaboration_report(security, collaboration)
            verification = ResearchVerification(
                citations_closed=False,
                numeric_values_valid=False,
                locators_match_plan=False,
                semantic=SemanticVerification(
                    passed=False,
                    reason_codes=(
                        "evidence_validation_deferred",
                        "semantic_verification_skipped",
                        "content_gate_skipped",
                    ),
                ),
                verified_metrics=0,
                requested_metrics=0,
                correction_attempts=0,
            )
            return {
                "status": ResearchStatus.PARTIAL,
                "plan": context,
                "security": security,
                "execution": execution,
                "report": report,
                "verification": verification,
                "tokens": tokens,
            }


def _specialist_records(collaboration, security, as_of):
    fundamental_trace, news_trace, market_trace, investment_trace, risk_trace = collaboration.traces
    split = fundamental_trace.tool_calls
    fundamental_tools = collaboration.tool_results[:split]
    news_tools = collaboration.tool_results[split : split + news_trace.tool_calls]
    market_start = split + news_trace.tool_calls
    market_tools = collaboration.tool_results[market_start : market_start + market_trace.tool_calls]
    shared = {
        "security": security.model_dump(mode="json"),
        "as_of": as_of.isoformat(),
    }
    return (
        AgentAuditRecord(
            role="fundamental",
            prompt_version=fundamental_trace.prompt_version,
            status=fundamental_trace.status,
            input_payload={**shared, "responsibility": "fixed_fundamental"},
            output_payload=None
            if collaboration.fundamental is None
            else collaboration.fundamental.model_dump(mode="json"),
            tools=tuple(item.tool.value for item in fundamental_tools),
            tool_attempts=fundamental_trace.tool_attempts,
            tokens=fundamental_trace.tokens,
        ),
        AgentAuditRecord(
            role="news",
            prompt_version=news_trace.prompt_version,
            status=news_trace.status,
            input_payload={**shared, "responsibility": "fixed_news"},
            output_payload=None
            if collaboration.news is None
            else collaboration.news.model_dump(mode="json"),
            tools=tuple(item.tool.value for item in news_tools),
            tool_attempts=news_trace.tool_attempts,
            tokens=news_trace.tokens,
        ),
        AgentAuditRecord(
            role="market",
            prompt_version=market_trace.prompt_version,
            status=market_trace.status,
            input_payload={**shared, "responsibility": "fixed_market"},
            output_payload=None
            if collaboration.market is None
            else collaboration.market.model_dump(mode="json"),
            tools=tuple(item.tool.value for item in market_tools),
            tool_attempts=market_trace.tool_attempts,
            tokens=market_trace.tokens,
        ),
        AgentAuditRecord(
            role="investment",
            prompt_version=investment_trace.prompt_version,
            status=investment_trace.status,
            input_payload={
                "fundamental": None
                if collaboration.fundamental is None
                else collaboration.fundamental.model_dump(mode="json"),
                "news": None
                if collaboration.news is None
                else collaboration.news.model_dump(mode="json"),
                "market": None
                if collaboration.market is None
                else collaboration.market.model_dump(mode="json"),
            },
            output_payload=None
            if collaboration.investment is None
            else {"report_markdown": collaboration.investment},
            tokens=investment_trace.tokens,
        ),
        AgentAuditRecord(
            role="risk",
            prompt_version=risk_trace.prompt_version,
            status=risk_trace.status,
            input_payload={
                "investment": None
                if collaboration.investment is None
                else collaboration.investment,
            },
            output_payload=None
            if collaboration.risk is None
            else {"report_markdown": collaboration.risk},
            tokens=risk_trace.tokens,
        ),
    )


def _collaboration_report(security, collaboration):
    investment = collaboration.investment
    risk = collaboration.risk
    if investment is None or risk is None:
        raise ValueError("research_investment_or_risk_missing")
    markdown = f"{investment}\n\n{risk}"
    return ResearchReport(
        title=f"{security.code} 多Agent投资分析",
        summary="Investment分析与Risk终审Markdown已生成；最终结论详见报告正文。",
        markdown=markdown,
        limitations=("evidence_validation_deferred",),
    )
