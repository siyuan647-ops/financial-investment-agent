import json
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import anyio
import pytest

from app.agents.specialists import (
    INVESTMENT_ANALYSIS_PROMPT_VERSION,
    RISK_REVIEW_PROMPT_VERSION,
    DynamicMultiAgentResearchGraph,
    FundamentalTurn,
    MarketTurn,
    QwenSpecialistModels,
    RoleModelOutput,
    SpecialistBudget,
    _compact_market_tool_result,
    _fundamental_model_argument_schema,
    _fundamental_model_tool_metadata,
    _investment_markdown_system_message,
    _investment_structured_system_message,
    _market_model_argument_schema,
    _market_model_tool_metadata,
    _market_system_message,
    _render_investment_analysis,
    _risk_review_system_message,
)
from app.contracts.business_tools import (
    AgentRole,
    BusinessToolEvidence,
    BusinessToolName,
    BusinessToolResult,
    ToolCoverage,
)
from app.contracts.common import SHANGHAI
from app.contracts.llm import LLMCompletion, LLMToolCall, ModelPurpose
from app.contracts.research import (
    InvestmentAnalysisDraft,
    InvestmentAnalysisTransport,
    SpecialistAssessment,
    SpecialistDecision,
    SpecialistRole,
)
from app.services.business_tools import BusinessToolRegistry

REQUEST = "1" * 32
AS_OF = datetime(2026, 11, 13, 12, tzinfo=SHANGHAI)
EVIDENCE = "ev_" + "a" * 64


def result(call):
    return BusinessToolResult(
        request_id=call.request_id,
        tool=call.name,
        status="ok",
        as_of=AS_OF,
        items=({"field": "sample", "value": "1"},),
        evidence=(
            BusinessToolEvidence(
                evidence_id=EVIDENCE,
                label="sample:000001.SZ",
                source="main_business_database",
                quality="valid",
            ),
        ),
        coverage=ToolCoverage(complete=True, sources=("main_business_database",)),
    )


def dynamic_arguments(name):
    if name in {
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
    }:
        return {}
    if name == "company.snapshot":
        return {}
    if name == "finance.query":
        return {"start": "2023-11-13", "end": "2026-11-13"}
    if name == "market.query":
        return {"start": "2026-10-13", "end": "2026-11-13"}
    if name == "events.query":
        return {"start": "2023-11-13", "end": "2026-11-13"}
    raise AssertionError(name)


def test_three_specialists_select_tools_in_parallel_before_risk_synthesis():
    seen = []
    investment_inputs = []
    risk_inputs = []
    market_tool_views = []
    market_calls = []

    async def handler(call):
        assert call.request_id == REQUEST
        assert call.arguments.code == "000001.SZ"
        assert call.arguments.as_of == AS_OF
        if hasattr(call.arguments, "end"):
            assert call.arguments.end <= AS_OF.date()
        if call.name == "get_stock_data":
            assert call.arguments.as_of_window == AS_OF.date()
            assert call.arguments.window == 60
            market_calls.append(call)
        elif call.name == "get_indicators":
            assert call.arguments.as_of_window == AS_OF.date()
            assert call.arguments.output_days == 30
            market_calls.append(call)
        seen.append(call.name)
        return result(call)

    class Models:
        async def fundamental_turn(self, *args, **kwargs):
            results = args[3]
            del kwargs
            if not results:
                calls = tuple(
                    LLMToolCall(id=f"call_{index}", name=name, arguments=dynamic_arguments(name))
                    for index, name in enumerate(
                        (
                            "get_company_profile",
                            "get_profitability",
                            "get_balance_sheet",
                            "get_cash_flow",
                        )
                    )
                )
                return RoleModelOutput(FundamentalTurn("", calls), 10, uuid4())
            return RoleModelOutput(
                FundamentalTurn(
                    markdown=(
                        "# 基本面\n## 公司基础档案\n完成。\n## 盈利\n完成。\n"
                        "## 资产负债\n完成。\n## 现金流\n完成。\n## 数据质量与限制\n"
                        "所有必要工具均已调用，结论只引用已返回证据。"
                    )
                ),
                10,
                uuid4(),
            )

        async def decide(self, role, question, security, tools, results, **kwargs):
            del question, security, kwargs
            called = {item.tool.value for item in results}
            if role == "fundamental":
                order = (
                    "get_company_profile",
                    "get_profitability",
                    "get_balance_sheet",
                    "get_cash_flow",
                )
            elif role == "news":
                order = (
                    "get_stock_news",
                    "get_announcements",
                    "get_earnings_events",
                )
            else:
                market_tool_views.append(tuple(item["name"] for item in tools))
                order = ("get_stock_data", "get_indicators")
            name = next((item for item in order if item not in called), None)
            if name is not None:
                arguments = {
                    "code": "000001.SZ",
                    "start": "2027-01-01",
                    "end": "2027-12-31",
                    "as_of": AS_OF.isoformat(),
                }
                if name == "company.snapshot":
                    arguments = {"code": "600000.SH", "as_of": "2027-01-01T00:00:00+08:00"}
                elif name in {
                    "get_stock_news",
                    "get_announcements",
                    "get_earnings_events",
                }:
                    arguments = {"limit": 10}
                elif name == "get_stock_data":
                    arguments = {"as_of_window": "2027-12-31"}
                elif name == "get_indicators":
                    arguments = {
                        "as_of_window": "2027-12-31",
                        "indicators": [
                            "rsi",
                            "macd",
                            "macds",
                            "macdh",
                            "boll",
                            "boll_ub",
                            "boll_lb",
                            "atr",
                        ],
                    }
                return RoleModelOutput(
                    SpecialistDecision(
                        action="tool",
                        tool_call={
                            "name": name,
                            "request_id": "0" * 32,
                            "arguments": arguments,
                        },
                    ),
                    10,
                    uuid4(),
                )
            return RoleModelOutput(
                SpecialistDecision(
                    action="finish",
                    assessment=SpecialistAssessment(
                        role=role.value,
                        summary=f"{role.value} complete",
                        report_markdown=(
                            "# 基本面\n## 公司基础档案\n完成。\n## 盈利\n完成。\n"
                            "## 资产负债\n完成。\n## 现金流\n完成。\n## 数据质量与限制\n"
                            "所有必要工具均已调用，结论只引用已返回证据。"
                            if role == "fundamental"
                            else "# 新闻\n## 公司新闻\n已检查授权来源。\n## 公告\n待授权。\n"
                            "## 公司事件\n已检查现有事件。\n## 行业与宏观影响\n"
                            "只依据返回新闻判断。\n## 信号\n当前信号中性。\n"
                            "## 来源覆盖\n有限来源，未把行情数据作为新闻。"
                            if role == "news"
                            else "# 技术面\n## 价格趋势\n已分析。\n## 移动均线\n已分析。\n"
                            "## MACD与动量\n已分析。\n## 波动率\n已分析。\n"
                            "## 量价关系\n已分析。\n## 指标选择依据\n已说明。\n"
                            "## 数据质量与限制\n仅使用窗口内数据。\n"
                            "| 观察日期 | 主要信号 | 证据 | 条件 | 风险与限制 |\n"
                            "| --- | --- | --- | --- | --- |\n"
                            "| 2026-11-13 | 示例 | ev | 条件性 | 无 |"
                        ),
                        coverage_status="complete",
                        claims=({"statement": "有证据结论", "evidence_ids": [EVIDENCE]},),
                    ),
                ),
                10,
                uuid4(),
            )

        async def market_turn(self, question, security, tools, results, **kwargs):
            del question, security, kwargs
            market_tool_views.append(tuple(item["name"] for item in tools))
            if tools:
                name = tools[0]["name"]
                arguments = (
                    {"symbol": "LLM_FIELD_MUST_BE_IGNORED"}
                    if name == "get_stock_data"
                    else {
                        "symbol": "LLM_FIELD_MUST_BE_IGNORED",
                        "output_days": 99,
                        "indicators": [
                            "rsi",
                            "macd",
                            "macds",
                            "macdh",
                            "boll",
                            "boll_ub",
                            "boll_lb",
                            "atr",
                        ],
                    }
                )
                return RoleModelOutput(
                    MarketTurn(
                        "",
                        (LLMToolCall(id=f"call_{name}", name=name, arguments=arguments),),
                    ),
                    10,
                    uuid4(),
                )
            assert {item.tool.value for item in results} == {
                "get_stock_data",
                "get_indicators",
            }
            return RoleModelOutput(
                MarketTurn(
                    "# 技术面\n## 价格趋势\n已分析。\n## 移动均线\n已分析。\n"
                    "## MACD与动量\n已分析。\n## 波动率\n已分析。\n"
                    "## 量价关系\n已分析。\n## 指标选择依据\n已说明。\n"
                    "## 数据质量与限制\n仅使用窗口内数据。\n"
                    "| 观察日期 | 主要信号 | 条件 | 风险与限制 |\n"
                    "| --- | --- | --- | --- |\n"
                    "| 2026-11-13 | 示例 | 条件性 | 无 |"
                ),
                10,
                uuid4(),
            )

        async def synthesize_investment(self, question, fundamental, news, market, **kwargs):
            del question, kwargs
            investment_inputs.append(
                (
                    fundamental.coverage_status,
                    fundamental.limitations,
                    news.coverage_status,
                    news.limitations,
                    market.coverage_status,
                    market.limitations,
                )
            )
            return RoleModelOutput(
                _render_investment_analysis(
                    InvestmentAnalysisDraft(
                        recommendation="Overweight",
                        rationale=(
                            "盈利改善对看多方向形成支持，但新闻覆盖有限，"
                            "因此只适合采用建设性的看多评级而不是更强结论。"
                        ),
                        limitations=("新闻来源覆盖有限，近期事件风险未被完整验证。",),
                        strategic_actions=(
                            "相对标准仓位小幅增配并分步执行，待新闻覆盖恢复后再决定是否继续提高敞口。"
                        ),
                    )
                ),
                10,
                uuid4(),
            )

        async def review_risk(self, question, investment, fundamental, news, market, **kwargs):
            del question, fundamental, news, market, kwargs
            risk_inputs.append(investment)
            return RoleModelOutput(
                (
                    "# 风险终审\n\n## 最终评级\n\n**最终评级：中性**\n\n"
                    "## 风险综合评估\n\n新闻覆盖不足且技术行情存在缺口，"
                    "原始看多计划的证据完整性不足。\n\n"
                    "## 终审主线逻辑\n\n由于核心事件风险和近期量价关系无法核验，"
                    "终审将看多初评下调为中性。\n\n"
                    "## 最终交易动作\n\n维持相对标准仓位，"
                    "等待新闻和近期行情覆盖恢复后再评估入场条件。\n\n"
                    "## 评级调整\n\n已将投资分析Agent的Overweight评级调整为中性。"
                ),
                10,
                uuid4(),
            )

    async def scenario():
        registry = BusinessToolRegistry(
            {
                BusinessToolName.GET_COMPANY_PROFILE: handler,
                BusinessToolName.GET_PROFITABILITY: handler,
                BusinessToolName.GET_BALANCE_SHEET: handler,
                BusinessToolName.GET_CASH_FLOW: handler,
                BusinessToolName.GET_STOCK_NEWS: handler,
                BusinessToolName.GET_ANNOUNCEMENTS: handler,
                BusinessToolName.GET_EARNINGS_EVENTS: handler,
                BusinessToolName.GET_STOCK_DATA: handler,
                BusinessToolName.GET_INDICATORS: handler,
            }
        )
        graph = DynamicMultiAgentResearchGraph(
            Models(),
            registry,
            budget=SpecialistBudget(max_tool_calls=6, max_role_tokens=16_000),
        )
        return await graph.run(
            "分析平安银行",
            object(),
            {"code": "000001.SZ", "name": "平安银行", "exchange": "SZSE"},
            object(),
            request_id=REQUEST,
            task_id=str(uuid4()),
            as_of=AS_OF,
        )

    collaboration, tokens = anyio.run(scenario)
    assert set(seen) == {
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
        "get_stock_news",
        "get_announcements",
        "get_earnings_events",
        "get_stock_data",
        "get_indicators",
    }
    assert collaboration.fundamental.role == "fundamental"
    assert collaboration.news.role == "news"
    assert collaboration.market.role == "market"
    assert investment_inputs == [("complete", (), "complete", (), "complete", ())]
    assert len(risk_inputs) == 1
    assert "**Recommendation**: Overweight" in risk_inputs[0]
    assert "**Strategic Actions**" in collaboration.investment
    assert "**最终评级：中性**" in collaboration.risk
    assert [trace.role for trace in collaboration.traces] == [
        "fundamental",
        "news",
        "market",
        "investment",
        "risk",
    ]
    assert len(collaboration.traces[0].tool_attempts) == 4
    assert len(collaboration.traces[1].tool_attempts) == 3
    assert len(collaboration.traces[2].tool_attempts) == 2
    assert market_tool_views == [
        ("get_stock_data",),
        ("get_indicators",),
        (),
    ]
    assert all(
        attempt.status == "succeeded"
        for trace in collaboration.traces[:3]
        for attempt in trace.tool_attempts
    )
    assert collaboration.traces[0].tool_attempts[0].sanitized_arguments["code"] == "000001.SZ"
    assert [item.arguments.model_dump(mode="json") for item in market_calls] == [
        {
            "code": "000001.SZ",
            "as_of": AS_OF.isoformat(),
            "as_of_window": AS_OF.date().isoformat(),
            "window": 60,
        },
        {
            "code": "000001.SZ",
            "as_of": AS_OF.isoformat(),
            "as_of_window": AS_OF.date().isoformat(),
            "indicators": [
                "rsi",
                "macd",
                "macds",
                "macdh",
                "boll",
                "boll_ub",
                "boll_lb",
                "atr",
            ],
            "output_days": 30,
        },
    ]
    assert tokens == 110


def test_market_prompt_and_native_schemas_limit_model_supplied_arguments():
    prompt = _market_system_message()
    assert "get_stock_data；调用该工具时不要填写任何参数，只传空对象{}" in prompt
    assert "get_indicators；调用该工具时只填写indicators一个参数" in prompt

    stock_schema = _market_model_argument_schema("get_stock_data")
    assert stock_schema == {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }
    indicator_schema = _market_model_argument_schema("get_indicators")
    assert indicator_schema["required"] == ["indicators"]
    assert set(indicator_schema["properties"]) == {"indicators"}
    assert indicator_schema["properties"]["indicators"]["minItems"] == 1
    assert indicator_schema["properties"]["indicators"]["maxItems"] == 8
    assert indicator_schema["properties"]["indicators"]["uniqueItems"] is True
    assert indicator_schema["additionalProperties"] is False

    model_metadata = _market_model_tool_metadata(
        (
            {
                "name": "get_stock_data",
                "description": "stock",
                "argument_schema": {"properties": {"code": {"type": "string"}}},
                "input_schema": {"properties": {"request_id": {"type": "string"}}},
            },
            {
                "name": "get_indicators",
                "description": "indicators",
                "argument_schema": {"properties": {"code": {"type": "string"}}},
                "input_schema": {"properties": {"request_id": {"type": "string"}}},
            },
        )
    )
    assert model_metadata[0]["argument_schema"] == stock_schema
    assert model_metadata[1]["argument_schema"] == indicator_schema
    assert all(set(item) == {"name", "description", "argument_schema"} for item in model_metadata)


def test_fundamental_native_tool_path_uses_empty_model_arguments_and_executes_all_domains():
    calls = []
    definitions_seen = []
    report = (
        "# 基本面分析\n\n## 公司基础档案\n完成。\n\n## 盈利\n完成。\n\n"
        "## 资产负债\n完成。\n\n## 现金流\n完成。\n\n"
        "## 数据质量与限制\n四个必要工具均已完成。"
    )

    class Client:
        async def complete_with_tools(self, messages, definitions, **kwargs):
            definitions_seen.append(definitions)
            assert "调用上述四个工具时只传空对象{}" in messages[0].content
            if len(definitions_seen) == 1:
                tool_calls = tuple(
                    LLMToolCall(id=f"call_{name}", name=name, arguments={})
                    for name in (
                        "get_company_profile",
                        "get_profitability",
                        "get_balance_sheet",
                        "get_cash_flow",
                    )
                )
                content = ""
            else:
                tool_calls = ()
                content = report
            return SimpleNamespace(
                completion=LLMCompletion(
                    invocation_id=uuid4(),
                    purpose=ModelPurpose.GENERATE,
                    model="test",
                    content=content,
                ),
                tool_calls=tool_calls,
            )

        async def complete(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

    async def handler(call):
        calls.append(call)
        return result(call)

    async def scenario():
        registry = BusinessToolRegistry(
            {
                BusinessToolName.GET_COMPANY_PROFILE: handler,
                BusinessToolName.GET_PROFITABILITY: handler,
                BusinessToolName.GET_BALANCE_SHEET: handler,
                BusinessToolName.GET_CASH_FLOW: handler,
            }
        )
        graph = DynamicMultiAgentResearchGraph(
            QwenSpecialistModels(Client()),
            registry,
            budget=SpecialistBudget(
                max_role_tokens=16_000,
                report_reserve_tokens=10_000,
            ),
        )
        return await graph._run_fundamental(
            {
                "question": "分析平安银行",
                "request_id": REQUEST,
                "task_id": str(uuid4()),
                "security": {"code": "000001.SZ", "name": "平安银行"},
                "as_of": AS_OF,
                "tools": registry,
            }
        )

    outcome = anyio.run(scenario)
    assert outcome.trace.status == "succeeded"
    assert outcome.trace.error_code is None
    assert outcome.assessment.report_markdown == report
    assert {call.name for call in calls} == {
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
    }
    assert all(call.arguments.code == "000001.SZ" for call in calls)
    assert all(call.arguments.as_of == AS_OF for call in calls)
    assert len(definitions_seen) == 2
    assert all(
        definition["function"]["parameters"]
        == {"type": "object", "properties": {}, "additionalProperties": False}
        for definitions in definitions_seen
        for definition in definitions
    )


def test_fundamental_model_metadata_rejects_market_tools():
    empty_schema = {"type": "object", "properties": {}, "additionalProperties": False}
    assert _fundamental_model_argument_schema("get_company_profile") == empty_schema
    metadata = _fundamental_model_tool_metadata(
        ({"name": "get_profitability", "description": "profit"},)
    )
    assert metadata == [
        {
            "name": "get_profitability",
            "description": "profit",
            "argument_schema": empty_schema,
        }
    ]
    with pytest.raises(ValueError, match="fundamental_tool_name_invalid"):
        _fundamental_model_argument_schema("get_stock_data")


def test_investment_and_risk_prompts_define_model_owned_decision_work():
    structured = _investment_structured_system_message()
    markdown = _investment_markdown_system_message()
    for investment in (structured, markdown):
        assert "冲突本身不是Hold的理由" in investment
        assert "commit到证据更强的一方" in investment
        assert "Buy表示强烈看多" in investment
        assert "研究团队看不到调用者的真实持仓" in investment
    assert "recommendation、rationale、limitations、strategic_actions四个字段" in structured
    assert "limitations必须是字符串数组" in structured
    assert "不要输出Markdown" in structured
    assert "完整Markdown正文" not in structured
    assert "完整Markdown正文" in markdown
    assert "不要输出JSON" in markdown
    assert "只生成结构化结果" not in markdown

    risk = _risk_review_system_message()
    assert "终审风险审查Agent" in risk
    assert "低置信证据不可以作为核心定价依据" in risk
    assert "直接否决开仓并强制中性" in risk
    assert "修正Strategic Actions" in risk
    assert "Buy=强烈看多、Overweight=看多、Hold=中性" in risk
    assert "恰好五选一" in risk
    assert "不得输出包含斜杠的候选列表" in risk
    assert "直接输出完整Markdown正文" in risk
    assert "# 风险终审" in risk
    assert "## 风险综合评估" in risk
    assert "## 最终交易动作" in risk
    assert "## 评级调整" in risk
    assert "不能调用任何外部工具" in risk


def test_investment_structured_result_is_rendered_to_markdown():
    class Client:
        async def complete_structured(self, messages, response_model, **kwargs):
            assert response_model is InvestmentAnalysisTransport
            assert "不要输出Markdown" in messages[0].content
            assert "完整Markdown正文" not in messages[0].content
            assert kwargs["prompt_version"] == INVESTMENT_ANALYSIS_PROMPT_VERSION
            assert kwargs["use_profile_max_output_tokens"] is False
            assert kwargs["temperature"] == 0
            return SimpleNamespace(
                parsed=InvestmentAnalysisTransport(
                    recommendation="Overweight",
                    rationale=(
                        "盈利改善是当前主要正面驱动，但新闻覆盖有限且技术数据不完整，"
                        "建设性看多强于中性，但不足以支持最高档评级。"
                    ),
                    limitations=("新闻coverage不完整，近期事件风险无法充分核验。",),
                    strategic_actions=(
                        "先按标准仓位的较低比例分步增配，待新闻覆盖和技术行情恢复完整后再提高敞口。"
                    ),
                ),
                completion=LLMCompletion(
                    invocation_id=uuid4(),
                    purpose=ModelPurpose.GENERATE,
                    model="test",
                    content="",
                ),
            )

        async def complete(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

    async def scenario():
        return await QwenSpecialistModels(Client()).synthesize_investment(
            "分析目标",
            None,
            None,
            None,
            request_id=REQUEST,
            task_id=str(uuid4()),
        )

    output = anyio.run(scenario)
    assert output.value.startswith("**Recommendation**: Overweight")
    assert "**Rationale**: 盈利改善" in output.value
    assert "**Limitations**:\n- 新闻coverage不完整" in output.value
    assert output.value.endswith("再提高敞口。")


def test_investment_structured_result_rejects_semantically_empty_text():
    with pytest.raises(ValueError, match="investment_rationale_not_meaningful"):
        InvestmentAnalysisDraft(
            recommendation="Hold",
            rationale="1" * 20,
            limitations=("新闻来源不可用。",),
            strategic_actions="维持相对标准仓位，等待缺失信息恢复后再重新评估投资方向。",
        )


def test_investment_transport_schema_only_enforces_shape():
    schema = InvestmentAnalysisTransport.model_json_schema()
    properties = schema["properties"]
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "recommendation",
        "rationale",
        "limitations",
        "strategic_actions",
    }
    assert properties["recommendation"]["enum"] == [
        "Buy",
        "Overweight",
        "Hold",
        "Underweight",
        "Sell",
    ]
    assert properties["rationale"]["type"] == "string"
    assert not ({"minLength", "maxLength", "pattern"} & properties["rationale"].keys())
    assert properties["limitations"]["type"] == "array"
    assert properties["limitations"]["items"]["type"] == "string"
    assert not ({"minItems", "maxItems"} & properties["limitations"].keys())
    assert properties["strategic_actions"]["type"] == "string"


def test_investment_structured_failure_retries_as_markdown():
    fallback_markdown = (
        "**Recommendation**: Hold\n\n"
        "**Rationale**: 当前信息不足，暂时不能建立有充分证据支持的方向性判断。\n\n"
        "**Limitations**:\n- 新闻来源不可用。\n\n"
        "**Strategic Actions**: 维持相对标准仓位，等待数据恢复后重新评估。"
    )

    class Client:
        async def complete_structured(self, *args, **kwargs):
            raise ValueError("invalid structured output")

        async def complete(self, messages, **kwargs):
            assert "完整Markdown正文" in messages[0].content
            assert "只生成结构化结果" not in messages[0].content
            assert kwargs["prompt_version"] == INVESTMENT_ANALYSIS_PROMPT_VERSION
            return LLMCompletion(
                invocation_id=uuid4(),
                purpose=ModelPurpose.GENERATE,
                model="test",
                content=fallback_markdown,
            )

    async def scenario():
        return await QwenSpecialistModels(Client()).synthesize_investment(
            "分析目标",
            None,
            None,
            None,
            request_id=REQUEST,
            task_id=str(uuid4()),
        )

    output = anyio.run(scenario)
    assert output.value == fallback_markdown


def test_investment_business_validation_failure_retries_as_markdown():
    fallback_markdown = (
        "**Recommendation**: Hold\n\n"
        "**Rationale**: 结构正确但内容不足，改用Markdown重新形成完整分析。\n\n"
        "**Limitations**:\n- 上游信息仍需进一步核验。\n\n"
        "**Strategic Actions**: 维持相对标准仓位，等待补充验证后重新评估。"
    )

    class Client:
        async def complete_structured(self, messages, response_model, **kwargs):
            del messages, response_model, kwargs
            return SimpleNamespace(
                parsed=InvestmentAnalysisTransport(
                    recommendation="Hold",
                    rationale="过短",
                    limitations=(),
                    strategic_actions="过短",
                ),
                completion=LLMCompletion(
                    invocation_id=uuid4(),
                    purpose=ModelPurpose.GENERATE,
                    model="test",
                    content="",
                ),
            )

        async def complete(self, messages, **kwargs):
            assert "完整Markdown正文" in messages[0].content
            assert kwargs["prompt_version"] == INVESTMENT_ANALYSIS_PROMPT_VERSION
            return LLMCompletion(
                invocation_id=uuid4(),
                purpose=ModelPurpose.GENERATE,
                model="test",
                content=fallback_markdown,
            )

    async def scenario():
        return await QwenSpecialistModels(Client()).synthesize_investment(
            "分析目标",
            None,
            None,
            None,
            request_id=REQUEST,
            task_id=str(uuid4()),
        )

    output = anyio.run(scenario)
    assert output.value == fallback_markdown


def test_risk_input_is_trimmed_and_output_uses_final_review_contract():
    captured = {}
    risk_markdown = (
        "# 风险终审\n\n## 最终评级\n\n**最终评级：看多**\n\n"
        "## 风险综合评估\n\n基本面盈利改善具有支持，但新闻为空且技术行情截断降低整体置信度。\n\n"
        "## 终审主线逻辑\n\n高置信基本面证据仍占优，通过收紧交易动作控制数据缺口风险。\n\n"
        "## 最终交易动作\n\n采用低于上游建议的相对标准仓位，等待完整行情和新闻覆盖恢复。\n\n"
        "## 评级调整\n\n维持投资分析Agent的看多方向，不作评级调整。"
    )

    class Client:
        async def complete(self, messages, **kwargs):
            captured.update(json.loads(messages[1].content))
            assert kwargs["prompt_version"] == RISK_REVIEW_PROMPT_VERSION
            return LLMCompletion(
                invocation_id=uuid4(),
                purpose=ModelPurpose.GENERATE,
                model="test",
                content=risk_markdown,
            )

    assessment = SpecialistAssessment(
        role="fundamental",
        summary="基本面摘要",
        report_markdown="# 基本面\n完整报告",
        coverage_status="coverage_incomplete",
        limitations=("不会进入Risk输入",),
    )

    async def scenario():
        return await QwenSpecialistModels(Client()).review_risk(
            "分析目标",
            "**Recommendation**: Overweight",
            assessment,
            None,
            None,
            request_id=REQUEST,
            task_id=str(uuid4()),
        )

    output = anyio.run(scenario)
    assert output.value == risk_markdown
    assert captured["fundamental"] == {
        "role": "fundamental",
        "summary": "基本面摘要",
        "report_markdown": "# 基本面\n完整报告",
    }
    assert captured["news"] is None
    assert captured["market"] is None


def test_market_markdown_path_has_no_role_token_or_timeout_gate():
    class Models:
        async def market_turn(self, question, security, tools, results, **kwargs):
            del question, security, kwargs
            if tools:
                name = tools[0]["name"]
                arguments = (
                    {"as_of_window": AS_OF.date().isoformat()}
                    if name == "get_stock_data"
                    else {
                        "as_of_window": AS_OF.date().isoformat(),
                        "indicators": ["rsi", "atr", "vwma"],
                    }
                )
                return RoleModelOutput(
                    MarketTurn(
                        "",
                        (LLMToolCall(id=f"call_{name}", name=name, arguments=arguments),),
                    ),
                    10_000,
                    uuid4(),
                )
            assert len(results) == 2
            return RoleModelOutput(
                MarketTurn(
                    "# 技术面\n## 价格趋势\n已分析。\n## 移动均线\n已分析。\n"
                    "## MACD与动量\n已分析。\n## 波动率\n已分析。\n"
                    "## 量价关系\n已分析。\n## 指标选择依据\n已说明。\n"
                    "## 数据质量与限制\n仅使用工具数据。\n"
                    "| 观察日期 | 主要信号 | 条件 | 风险与限制 |\n"
                    "| --- | --- | --- | --- |\n"
                    "| 2026-11-13 | 示例 | 条件性 | 无 |"
                ),
                10_000,
                uuid4(),
            )

    async def handler(call):
        return result(call)

    async def scenario():
        registry = BusinessToolRegistry(
            {
                BusinessToolName.GET_STOCK_DATA: handler,
                BusinessToolName.GET_INDICATORS: handler,
            }
        )
        graph = DynamicMultiAgentResearchGraph(
            Models(),
            registry,
            budget=SpecialistBudget(
                role_timeout_seconds=0.000001,
                max_role_tokens=1,
            ),
        )
        return await graph._run_market(
            {
                "question": "test",
                "request_id": REQUEST,
                "task_id": str(uuid4()),
                "security": {"code": "000001.SZ"},
                "as_of": AS_OF,
                "tools": registry,
            }
        )

    outcome = anyio.run(scenario)
    assert outcome.assessment.report_markdown.startswith("# 技术面")
    assert outcome.assessment.claims == ()
    assert outcome.trace.tokens == 30_000
    assert len(outcome.trace.tool_attempts) == 2


def test_market_model_context_preserves_full_windows_and_omits_evidence_ids():
    stock_items = tuple(
        {
            "trade_date": (date(2026, 8, 14) + timedelta(days=index)).isoformat(),
            "close": str(10 + index / 100),
            "evidence_id": EVIDENCE,
        }
        for index in range(60)
    )
    stock = BusinessToolResult(
        request_id=REQUEST,
        tool="get_stock_data",
        status="ok",
        as_of=AS_OF,
        items=stock_items,
        evidence=(
            BusinessToolEvidence(
                evidence_id=EVIDENCE,
                label="ohlcv:000001.SZ:2026-11-13",
                source="storage.price_bar",
            ),
        ),
        coverage=ToolCoverage(complete=True, sources=("storage.price_bar",)),
    )
    indicator_items = tuple(
        {
            "trade_date": (date(2026, 9, 28) + timedelta(days=index)).isoformat(),
            "values": {"rsi": str(40 + index)},
            "evidence_id": EVIDENCE,
        }
        for index in range(30)
    )
    indicators = BusinessToolResult(
        request_id=REQUEST,
        tool="get_indicators",
        status="ok",
        as_of=AS_OF,
        items=indicator_items,
        evidence=(
            BusinessToolEvidence(
                evidence_id=EVIDENCE,
                label="indicators:000001.SZ",
                source="storage.technical_indicator",
            ),
        ),
        coverage=ToolCoverage(
            complete=True,
            sources=("storage.technical_indicator",),
        ),
    )

    compact_stock = _compact_market_tool_result(stock)
    compact_indicators = _compact_market_tool_result(indicators)

    assert len(compact_stock["items"]) == 60
    assert compact_stock["items"][0]["trade_date"] == stock_items[0]["trade_date"]
    assert compact_stock["items"][-1]["trade_date"] == stock_items[-1]["trade_date"]
    assert compact_stock["evidence"] == []
    assert all("evidence_id" not in item for item in compact_stock["items"])
    assert len(compact_indicators["items"]) == 30
    assert compact_indicators["items"][0]["trade_date"] == indicator_items[0]["trade_date"]
    assert compact_indicators["items"][-1]["trade_date"] == indicator_items[-1]["trade_date"]
    assert compact_indicators["evidence"] == []
    assert all("evidence_id" not in item for item in compact_indicators["items"])


def test_failed_tool_attempts_are_redacted_and_classified():
    scenarios = (
        (
            {"name": "shell.run", "arguments": {"code": "secret", "password": "hidden"}},
            None,
            "selection",
            "tool_name_invalid",
        ),
        (
            {
                "name": "get_company_profile",
                "arguments": {"start": "tomorrow"},
            },
            None,
            "security_injection",
            "tool_date_invalid",
        ),
        (
            {
                "name": "get_indicators",
                "arguments": {"indicators": ["invented"]},
            },
            None,
            "schema_validation",
            "tool_arguments_invalid",
        ),
        (
            {"name": "get_company_profile", "arguments": {}},
            RuntimeError("upstream secret must not be persisted"),
            "execution",
            "tool_execution_failed",
        ),
    )

    for proposed_call, handler_error, expected_stage, expected_error in scenarios:

        class Models:
            async def decide(self, *args, proposed=proposed_call, **kwargs):
                del args, kwargs
                return RoleModelOutput(
                    SpecialistDecision(action="tool", tool_call=proposed),
                    10,
                    uuid4(),
                )

            async def synthesize_risk(self, *args, **kwargs):
                raise AssertionError((args, kwargs))

        async def handler(call, error=handler_error):
            if error is not None:
                raise error
            return result(call)

        async def scenario():
            registry = BusinessToolRegistry(
                {
                    BusinessToolName.GET_COMPANY_PROFILE: handler,
                    BusinessToolName.GET_STOCK_DATA: handler,
                    BusinessToolName.GET_INDICATORS: handler,
                }
            )
            graph = DynamicMultiAgentResearchGraph(Models(), registry)
            return await graph._run_specialist(
                SpecialistRole.MARKET,
                AgentRole.ORCHESTRATOR,
                {
                    "question": "test",
                    "request_id": REQUEST,
                    "task_id": str(uuid4()),
                    "security": {"code": "000001.SZ"},
                    "as_of": AS_OF,
                    "tools": registry,
                },
            )

        outcome = anyio.run(scenario)
        attempt = outcome.trace.tool_attempts[0]
        assert attempt.stage == expected_stage
        assert attempt.status == "failed"
        assert attempt.error_code == expected_error
        serialized = attempt.model_dump_json()
        assert "password" not in serialized
        assert "hidden" not in serialized
        assert "upstream secret" not in serialized


def test_fundamental_dynamically_batches_tools_deduplicates_and_returns_raw_markdown():
    active = 0
    max_active = 0
    turn_calls = 0

    async def handler(call):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await anyio.sleep(0)
        active -= 1
        return result(call)

    class Models:
        async def fundamental_turn(self, *args, **kwargs):
            nonlocal turn_calls
            del args, kwargs
            turn_calls += 1
            if turn_calls == 1:
                names = (
                    "get_company_profile",
                    "get_profitability",
                    "get_balance_sheet",
                    "get_cash_flow",
                )
                calls = tuple(
                    LLMToolCall(id=f"call_{i}", name=name, arguments=dynamic_arguments(name))
                    for i, name in enumerate(names)
                )
                return RoleModelOutput(FundamentalTurn("", calls), 100, uuid4())
            if turn_calls == 2:
                calls = (
                    LLMToolCall(
                        id="call_dup",
                        name="get_profitability",
                        arguments={},
                    ),
                )
                return RoleModelOutput(FundamentalTurn("", calls), 100, uuid4())
            return RoleModelOutput(FundamentalTurn("# 原始报告\n保留模型格式。"), 100, uuid4())

        async def decide(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

        async def synthesize_risk(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

    async def scenario():
        registry = BusinessToolRegistry(
            {
                BusinessToolName.GET_COMPANY_PROFILE: handler,
                BusinessToolName.GET_PROFITABILITY: handler,
                BusinessToolName.GET_BALANCE_SHEET: handler,
                BusinessToolName.GET_CASH_FLOW: handler,
            }
        )
        graph = DynamicMultiAgentResearchGraph(
            Models(),
            registry,
            budget=SpecialistBudget(
                max_role_tokens=16_000,
                report_reserve_tokens=10_000,
                max_fundamental_tool_rounds=20,
            ),
        )
        return await graph._run_fundamental(
            {
                "question": "test",
                "request_id": REQUEST,
                "task_id": str(uuid4()),
                "security": {"code": "000001.SZ"},
                "as_of": AS_OF,
                "tools": registry,
            }
        )

    outcome = anyio.run(scenario)
    assert max_active == 4
    assert turn_calls == 3
    assert outcome.assessment.report_markdown == "# 原始报告\n保留模型格式。"
    assert outcome.assessment.claims == ()
    assert outcome.trace.tokens == 300
    assert len(outcome.results) == 4
    assert [item.tool_name for item in outcome.trace.tool_attempts[:4]] == [
        "get_company_profile",
        "get_profitability",
        "get_balance_sheet",
        "get_cash_flow",
    ]
    assert outcome.trace.tool_attempts[4].error_code == "tool_duplicate_call"


def test_fundamental_tool_round_ceiling_degrades_when_required_domains_are_missing():
    allow_tools_history = []

    class Models:
        async def fundamental_turn(self, *args, **kwargs):
            del args
            allow_tools_history.append(kwargs["allow_tools"])
            if kwargs["allow_tools"]:
                call = LLMToolCall(id="call_company", name="get_company_profile", arguments={})
                return RoleModelOutput(FundamentalTurn("", (call,)), 10, uuid4())
            return RoleModelOutput(FundamentalTurn("# 强制收尾\n使用已有结果。"), 10, uuid4())

    async def handler(call):
        return result(call)

    async def scenario():
        registry = BusinessToolRegistry({BusinessToolName.GET_COMPANY_PROFILE: handler})
        graph = DynamicMultiAgentResearchGraph(
            Models(),
            registry,
            budget=SpecialistBudget(
                max_fundamental_tool_rounds=2,
                max_role_tokens=100_000,
                report_reserve_tokens=0,
                supplemental_call_reserve_tokens=0,
            ),
        )
        return await graph._run_fundamental(
            {
                "question": "test",
                "request_id": REQUEST,
                "task_id": str(uuid4()),
                "security": {"code": "000001.SZ"},
                "as_of": AS_OF,
                "tools": registry,
            }
        )

    outcome = anyio.run(scenario)
    assert allow_tools_history == [True, True]
    assert "fundamental_required_tools_missing" in outcome.assessment.limitations
    assert outcome.trace.status == "failed"
    assert outcome.trace.error_code == "fundamental_required_tools_missing"
    assert outcome.trace.tool_attempts[1].error_code == "tool_duplicate_call"


def test_tool_response_contract_failure_has_distinct_audit_code():
    class Models:
        async def decide(self, *args, **kwargs):
            del args, kwargs
            return RoleModelOutput(
                SpecialistDecision(
                    action="tool",
                    tool_call={"name": "get_company_profile", "arguments": {}},
                ),
                10,
                uuid4(),
            )

        async def synthesize_risk(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

    async def invalid_handler(call):
        value = result(call).model_copy(update={"request_id": "2" * 32})
        return value

    async def scenario():
        registry = BusinessToolRegistry({BusinessToolName.GET_COMPANY_PROFILE: invalid_handler})
        graph = DynamicMultiAgentResearchGraph(Models(), registry)
        return await graph._run_specialist(
            SpecialistRole.FUNDAMENTAL,
            AgentRole.FUNDAMENTAL,
            {
                "question": "test",
                "request_id": REQUEST,
                "task_id": str(uuid4()),
                "security": {"code": "000001.SZ"},
                "as_of": AS_OF,
                "tools": registry,
            },
        )

    outcome = anyio.run(scenario)
    attempt = outcome.trace.tool_attempts[0]
    assert attempt.stage == "execution"
    assert attempt.error_code == "tool_response_invalid"


def test_specialist_cannot_finish_with_fabricated_citation_and_decision_degrades():
    class Models:
        async def decide(self, role, *args, **kwargs):
            del args, kwargs
            return RoleModelOutput(
                SpecialistDecision(
                    action="finish",
                    assessment=SpecialistAssessment(
                        role=role.value,
                        summary="unsupported",
                        report_markdown="# 不完整报告\n此内容不会越过必要工具覆盖门禁。",
                        coverage_status="complete",
                        claims=(
                            {
                                "statement": "伪造引用",
                                "evidence_ids": ["ev_" + "0" * 64],
                            },
                        ),
                    ),
                ),
                1,
                uuid4(),
            )

        async def synthesize_risk(self, *args, **kwargs):
            raise AssertionError((args, kwargs))

    async def scenario():
        return await DynamicMultiAgentResearchGraph(Models(), BusinessToolRegistry()).run(
            "test",
            object(),
            {"code": "000001.SZ"},
            object(),
            request_id=REQUEST,
            task_id=str(uuid4()),
            as_of=AS_OF,
        )

    collaboration, _ = anyio.run(scenario)
    assert collaboration.fundamental is not None
    assert collaboration.news is not None
    assert collaboration.investment is not None
    assert collaboration.risk is not None
    assert "role_model_failed" in collaboration.fundamental.limitations
    assert collaboration.traces[0].status == "failed"
    assert collaboration.traces[0].error_code == "role_model_failed"
    assert "**Recommendation**: Hold" in collaboration.investment
    assert "**最终评级：无法形成**" in collaboration.risk


def test_fundamental_markdown_is_not_normalized_or_section_filled():
    raw = "```markdown\n# 平安银行报告\n\n只有模型原文。\n```"
    assessment = SpecialistAssessment(
        role="fundamental",
        summary="Fundamental Agent已完成Markdown报告。",
        report_markdown=raw,
        coverage_status="partial",
    )
    assert assessment.report_markdown == raw
