from datetime import datetime

import anyio

from app.agents.direct_deep import DirectDeepResearchAgent
from app.contracts.common import SHANGHAI
from app.contracts.research import (
    RoleStatus,
    SpecialistAssessment,
    SpecialistCollaborationResult,
    SpecialistInvocationTrace,
    ToolAttemptRecord,
)

REQUEST = "1" * 32
AS_OF = datetime(2026, 11, 13, 12, tzinfo=SHANGHAI)


def test_target_only_deep_runs_specialists_without_router_planner_or_verify():
    events = []

    class Collaboration:
        async def run(self, question, context, security, execution, **kwargs):
            del question, context, security, execution, kwargs
            events.append(("specialists",))
            return (
                SpecialistCollaborationResult(
                    fundamental=SpecialistAssessment(
                        role="fundamental",
                        summary="无可用财务证据",
                        report_markdown="# 基本面\n\n现有数据覆盖不足，无法形成完整判断。",
                        coverage_status="coverage_incomplete",
                        limitations=("no_data",),
                    ),
                    news=SpecialistAssessment(
                        role="news",
                        summary="新闻覆盖不完整",
                        report_markdown="# 新闻\n\n当前仅有有限来源，不能证明不存在负面新闻。",
                        coverage_status="coverage_incomplete",
                        limitations=("coverage",),
                    ),
                    market=SpecialistAssessment(
                        role="market",
                        summary="技术面覆盖不完整",
                        report_markdown="# 技术面\n\n当前行情不足，无法形成完整技术判断。",
                        coverage_status="coverage_incomplete",
                        limitations=("coverage",),
                    ),
                    investment=(
                        "**Recommendation**: Hold\n\n"
                        "**Rationale**: 数据不足，无法形成方向性占优判断。\n\n"
                        "**Limitations**:\n- 基本面、新闻和技术面均存在缺口。\n\n"
                        "**Strategic Actions**: 维持相对标准仓位，等待覆盖恢复后重新评估。"
                    ),
                    risk=(
                        "# 风险终审\n\n## 最终评级\n\n**最终评级：中性**\n\n"
                        "## 风险综合评估\n\n基本面、新闻面和技术面均存在数据缺口，"
                        "方向判断置信度有限。\n\n"
                        "## 终审主线逻辑\n\n投资分析初评已为中性，现有风险不足以支持改变评级。\n\n"
                        "## 最终交易动作\n\n维持相对标准仓位，"
                        "等待上游关键数据覆盖恢复后再入场。\n\n"
                        "## 评级调整\n\n维持中性，不作调整。"
                    ),
                    traces=(
                        SpecialistInvocationTrace(
                            role="fundamental",
                            status=RoleStatus.SUCCEEDED,
                            prompt_version="f",
                            tool_attempts=(
                                ToolAttemptRecord(
                                    sequence=1,
                                    tool_name="get_profitability",
                                    argument_keys=("code", "start", "end"),
                                    sanitized_arguments={
                                        "code": "000001.SZ",
                                        "start": "2026-01-01",
                                        "end": "2026-11-13",
                                    },
                                    stage="schema_validation",
                                    status="failed",
                                    error_code="tool_arguments_invalid",
                                ),
                            ),
                        ),
                        SpecialistInvocationTrace(
                            role="news", status=RoleStatus.SUCCEEDED, prompt_version="n"
                        ),
                        SpecialistInvocationTrace(
                            role="market", status=RoleStatus.SUCCEEDED, prompt_version="m"
                        ),
                        SpecialistInvocationTrace(
                            role="investment", status=RoleStatus.SUCCEEDED, prompt_version="i"
                        ),
                        SpecialistInvocationTrace(
                            role="risk", status=RoleStatus.SUCCEEDED, prompt_version="r"
                        ),
                    ),
                ),
                30,
            )

    async def scenario():
        phases = []

        async def progress(phase, status):
            phases.append((phase, status.value))

        state = await DirectDeepResearchAgent(Collaboration()).run(
            "000001.SZ",
            security={"code": "000001.SZ", "name": "平安银行", "exchange": "SZSE"},
            request_id=REQUEST,
            task_id="2" * 32,
            identity=object(),
            as_of=AS_OF,
            progress=progress,
        )
        return state, phases

    state, phases = anyio.run(scenario)
    assert [item[0] for item in events] == ["specialists"]
    assert [item[0] for item in phases] == ["plan", "execute", "generate"]
    assert state["plan"].reason_code == "target_only_multi_agent"
    assert state["execution"].fact_answers == ()
    assert state["status"] == "partial"
    assert state["tokens"] == 30
    assert "**最终评级：中性**" in state["report"].markdown
    assert [item.role for item in state["execution"].agent_records] == [
        "fundamental",
        "news",
        "market",
        "investment",
        "risk",
    ]
    fundamental_record = state["execution"].agent_records[0]
    assert fundamental_record.tool_attempts[0].tool_name == "get_profitability"
    assert fundamental_record.tool_attempts[0].error_code == "tool_arguments_invalid"
    assert state["verification"].semantic.reason_codes == (
        "evidence_validation_deferred",
        "semantic_verification_skipped",
        "content_gate_skipped",
    )
