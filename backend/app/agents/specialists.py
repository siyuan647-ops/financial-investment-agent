"""Parallel fundamental/news/market agents followed by risk synthesis."""

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from time import perf_counter
from types import CodeType
from typing import Protocol, TypedDict
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from app.agents.role_registry import ROLE_DEFINITIONS, RoleRegistry
from app.contracts.business_tools import BUSINESS_TOOL_CALL, AgentRole, BusinessToolResult
from app.contracts.llm import LLMMessage, LLMToolCall
from app.contracts.research import (
    InvestmentAnalysisDraft,
    InvestmentAnalysisTransport,
    RiskSynthesis,
    RoleStatus,
    SpecialistAssessment,
    SpecialistCollaborationResult,
    SpecialistDecision,
    SpecialistInvocationTrace,
    SpecialistRole,
    ToolAttemptRecord,
)
from app.services.business_tools import BusinessToolRegistry

FUNDAMENTAL_PROMPT_VERSION = ROLE_DEFINITIONS[SpecialistRole.FUNDAMENTAL].prompt_version
NEWS_PROMPT_VERSION = ROLE_DEFINITIONS[SpecialistRole.NEWS].prompt_version
MARKET_PROMPT_VERSION = ROLE_DEFINITIONS[SpecialistRole.MARKET].prompt_version
INVESTMENT_ANALYSIS_PROMPT_VERSION = ROLE_DEFINITIONS[SpecialistRole.INVESTMENT].prompt_version
RISK_REVIEW_PROMPT_VERSION = ROLE_DEFINITIONS[SpecialistRole.RISK].prompt_version


@dataclass(frozen=True)
class RoleModelOutput[T]:
    value: T
    tokens: int
    invocation_id: UUID


_FUNDAMENTAL_REQUIRED = {
    "get_company_profile",
    "get_profitability",
    "get_balance_sheet",
    "get_cash_flow",
}
_NEWS_REQUIRED = {
    "get_stock_news",
    "get_announcements",
    "get_earnings_events",
}
_MARKET_REQUIRED = {"get_stock_data", "get_indicators"}
_AUDITED_TOOL_NAMES = {
    "get_stock_data",
    "get_indicators",
    "get_stock_news",
    "get_announcements",
    "get_earnings_events",
    "get_company_profile",
    "get_profitability",
    "get_balance_sheet",
    "get_cash_flow",
}
_AUDITED_ARGUMENT_KEYS = {
    "code",
    "as_of",
    "as_of_window",
    "limit",
    "indicators",
    "output_days",
    "window",
}
_AUDITED_ENUM_VALUES = {
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
}


def _parallel_agent_system_prompt(
    tools,
    *,
    trade_date,
    instrument_context,
    system_message,
):
    tool_names = "、".join(f"`{item['name']}`" for item in tools) or "无"
    context = json.dumps(instrument_context, ensure_ascii=False, default=str)
    return (
        "你是一名 AI 助手，与其他助手协同工作。使用提供的工具推进问题解答。"
        "如果你无法完整作答也没关系，其他配备不同工具的助手会接续处理剩余部分。"
        "尽你所能推进工作；说明你的工具能够支持哪些能力，取舍权衡交由其他 Agent 决定。 "
        f"你可调用工具：{tool_names}。 今日日期为 `{trade_date}`；"
        "所有分析与工具调用的日期范围，均以此日期作为当前时点。"
        f" {context}\n\n{system_message}"
    )


def _market_system_message():
    return (
        "你是一名交易助手，负责对目标证券进行技术面分析。只能依据get_stock_data和"
        "get_indicators返回的数据分析价格趋势、动量、波动率及量价关系，不得推断公司基本面、"
        "新闻、公告、估值、股息率或宏观事件。必须先调用get_stock_data；调用该工具时不要填写"
        "任何参数，只传空对象{}，证券代码、分析日期和60日窗口全部由程序固定注入。取得OHLCV"
        "结果后，再调用get_indicators；调用该工具时只填写indicators一个参数，不得填写code、"
        "as_of、as_of_window、output_days或其他字段，证券代码、分析日期和30日窗口全部由程序"
        "固定注入。从以下指标中选择"
        "最多8个信息互补、不重复的指标，读取最近30个完整交易日的预计算指标序列。"
        "可选指标仅限：移动均线close_50_sma、close_10_ema；MACD相关macd、macds、macdh；"
        "动量rsi；波动率boll、boll_ub、boll_lb、atr；量能vwma。调用get_indicators时必须严格"
        "使用上述原名，至少选择1个、最多8个且不得重复，并优先覆盖趋势、动量、波动率和量能。"
        "MACD线、信号线和柱状图应结合解释，布林中轨、上轨和下轨应结合解释；最终报告必须单独"
        "说明指标选择理由。所有日期、OHLCV、指标值、涨跌幅、支撑和压力判断只能依据工具返回的"
        "带日期记录。数据缺失、日期不一致或相互矛盾时，必须在数据质量与限制中明确列出，不得补值、"
        "修改或抹平差异。不得宣称历史回测有效，不得把技术信号描述成确定性预测；支撑、压力、突破、"
        "反转、超买和超卖只能表述为技术观察或条件性情景。ATR仅用于描述波动尺度和候选风险距离，"
        "不得生成个性化仓位方案或保证止损有效。报告必须包含：价格趋势、移动均线、MACD与动量、"
        "波动率、量价关系、指标选择依据、数据质量与限制；末尾必须附加Markdown表格，汇总观察日期、"
        "主要信号、条件、风险与限制。最终只输出Markdown正文，不要JSON、结构化claims或Evidence ID。"
    )


def _investment_common_system_message():
    return (
        "你是A股投资分析Agent。你的职责是综合基本面、新闻面与技术面三个上游Agent的完整报告，"
        "形成给交易员使用的投资计划。你只能使用输入中明确提供的信息，不能调用工具，不得使用模型"
        "记忆或外部知识补充输入中不存在的事实。\n\n"
        "recommendation恰好五选一：Buy表示强烈看多并建议建立"
        "或显著增加标准仓位；Overweight表示建设性看多并建议逐步提高相对标准仓位；Hold表示多空证据"
        "权衡后仍然平衡或证据太少；Underweight表示谨慎看空并建议降低相对标准仓位；Sell表示强烈看空"
        "并建议退出或回避。上游报告存在冲突是正常现象，冲突本身不是Hold的理由；必须比较双方论据，"
        "commit到证据更强的一方，并用Buy/Overweight或Underweight/Sell表达胜出幅度。\n\n"
        "rationale必须给出详细理由：综合基本面、新闻面与技术面中的具体事实，权衡看多和看空论据，"
        "评价来源、时效性、覆盖范围、可信度和相互印证情况，并说明哪些论据决定recommendation。不得"
        "只输出等级、数字、标签或无解释结论。limitations必须列出上游缺失、source_unavailable、"
        "coverage_incomplete、数据日期冲突、未经验证的推断及可能使结论失效的条件；缺失信息只能作为"
        "限制，不得支持任一方向。只要任一上游存在不完整状态或limitations，本字段不得省略相关影响。\n\n"
        "strategic_actions必须给交易员可执行的具体步骤，说明相对标准仓位的sizing、分步执行条件、"
        "需要等待或跟踪的验证指标以及导致增持、减持或退出的条件。研究团队看不到调用者的真实持仓，"
        "因此只能提供相对标准仓位的非个性化指引，不得假设实际仓位。本阶段暂不要求Evidence ID。\n\n"
    )


def _investment_structured_system_message():
    return _investment_common_system_message() + (
        "本次调用只生成结构化结果。完整填写且只填写recommendation、rationale、limitations、"
        "strategic_actions四个字段。rationale和strategic_actions必须是字符串；limitations必须是字符串"
        "数组，即使只有一项也必须使用数组。不得增加bull_pool、bear_pool、confidence或其他字段。"
        "recommendation不得输出中文、解释、斜杠候选列表或多个评级。不要输出Markdown、代码围栏、"
        "解释性前言或结尾。"
    )


def _investment_markdown_system_message():
    return _investment_common_system_message() + (
        "本次调用直接输出完整Markdown正文，不要输出JSON或代码围栏。严格依次使用"
        "Recommendation、Rationale、Limitations、Strategic Actions四段；Limitations使用"
        "项目符号列表。"
    )


def _markdown_bullets(items, empty: str) -> str:
    return "\n".join(f"- {item}" for item in items) if items else f"- {empty}"


def _render_investment_analysis(value: InvestmentAnalysisDraft) -> str:
    limitations = _markdown_bullets(value.limitations, "未形成可交付的分析限制。")
    return (
        f"**Recommendation**: {value.recommendation}\n\n"
        f"**Rationale**: {value.rationale}\n\n"
        "**Limitations**:\n"
        f"{limitations}\n\n"
        f"**Strategic Actions**: {value.strategic_actions}"
    )


def _investment_fallback_markdown(code: str) -> str:
    return (
        "**Recommendation**: Hold\n\n"
        "**Rationale**: 投资分析模型未形成可交付结论，当前不能建立有充分支撑的方向性判断。\n\n"
        "**Limitations**:\n"
        f"- {code}\n- investment_analysis_incomplete\n\n"
        "**Strategic Actions**: 暂不改变相对标准仓位，等待三个上游报告形成可验证、"
        "覆盖充分且相互一致的关键信息后再重新评估。"
    )


def _risk_review_system_message():
    return (
        "你是终审风险审查Agent，是整条投研链路的最后一环。你不能调用任何外部工具，禁止使用模型"
        "记忆补充输入中不存在的事实，所有判断只能基于输入材料：基本面Agent、技术面Agent、新闻Agent"
        "的原始输出，以及投资分析Agent的融合结论。\n\n"
        "你的工作流程严格按顺序执行：\n"
        "1. 通读上游三份并行Agent证据，复核投资分析Agent在rationale中的多空证据权衡、初步评级、"
        "strategic actions和limitations；\n"
        "2. 识别全部风险：业务风险、财报风险、数据缺陷风险、证据置信度问题、证据冲突、流动性风险、"
        "集中度风险、估值陷阱、交易计划隐患，包括止损不合理、仓位过大、价位基于低置信残缺数据；\n"
        "3. 评估上游证据置信度：高置信为审计财报、交易所公告、核验完整K线；中置信为调研纪要、"
        "一致预期；低置信为舆情传闻、截断缺失行情、未核验指标。低置信证据不可以作为核心定价依据；\n"
        "4. 做出终审判断：可以维持上游初步评级；可以下调评级，例如看多转中性，原因可以是尾部风险"
        "过高或核心证据可信度不足；只在上游明显低估高置信利好且无新增重大风险时谨慎上调；证据严重"
        "残缺或风险不可控时直接否决开仓并强制中性；\n"
        "5. 修正Strategic Actions：如果上游仓位、止损、入场条件存在风控缺陷，给出修正方案；若无问题"
        "则沿用上游方案；\n"
        "6. 所有上游报告正文标注的数据缺口和limitations必须纳入风险评估，不能忽略；缺失信息不可以"
        "作为看多或者看空的依据。\n\n"
        "投资分析英文初评固定映射为Buy=强烈看多、Overweight=看多、Hold=中性、Underweight=看空、"
        "Sell=强烈看空。最终评级必须且只能从“强烈看多”“看多”“中性”“看空”“强烈看空”中"
        "选择一个，恰好五选一；不得输出包含斜杠的候选列表、英文评级或多个评级。\n\n"
        "直接输出完整Markdown正文，不要输出JSON、代码围栏或额外前言。严格使用以下结构：\n\n"
        "# 风险终审\n\n"
        "## 最终评级\n\n"
        "**最终评级：中性**\n\n"
        "## 风险综合评估\n\n"
        "详细汇总业务风险、财报风险、数据缺陷、证据置信问题、证据冲突、流动性、集中度、估值陷阱"
        "和交易计划隐患，并说明它们对结论的影响。\n\n"
        "## 终审主线逻辑\n\n"
        "说明维持、下调、谨慎上调或否决初评的核心原因，以及风险因素如何影响最终决策。\n\n"
        "## 最终交易动作\n\n"
        "自由组织修正后的仓位、入场、止损和跟踪建议；不得使用残缺或低置信数据编造精确价位。\n\n"
        "## 评级调整\n\n"
        "明确说明是否修改Investment初评；如修改，说明原因；如维持，也说明主要剩余风险。"
    )


def _risk_agent_context(assessment):
    if assessment is None:
        return None
    return {
        "role": assessment.role,
        "summary": assessment.summary,
        "report_markdown": assessment.report_markdown,
    }


@dataclass(frozen=True)
class FundamentalTurn:
    markdown: str
    tool_calls: tuple[LLMToolCall, ...] = ()


@dataclass(frozen=True)
class MarketTurn:
    markdown: str
    tool_calls: tuple[LLMToolCall, ...] = ()


class SpecialistModels(Protocol):
    async def fundamental_turn(
        self,
        question: str,
        security: dict,
        tools: tuple[dict, ...],
        results: tuple[BusinessToolResult, ...],
        *,
        request_id: str,
        task_id: str,
        allow_tools: bool,
        trade_date: str,
    ) -> RoleModelOutput[FundamentalTurn]: ...

    async def market_turn(
        self,
        question: str,
        security: dict,
        tools: tuple[dict, ...],
        results: tuple[BusinessToolResult, ...],
        *,
        request_id: str,
        task_id: str,
        allow_tools: bool,
        trade_date: str,
    ) -> RoleModelOutput[MarketTurn]: ...

    async def decide(
        self,
        role: SpecialistRole,
        question: str,
        security: dict,
        tools: tuple[dict, ...],
        results: tuple[BusinessToolResult, ...],
        *,
        request_id: str,
        task_id: str,
        trade_date: str,
    ) -> RoleModelOutput[SpecialistDecision]: ...

    async def synthesize_investment(
        self,
        question: str,
        fundamental: SpecialistAssessment | None,
        news: SpecialistAssessment | None,
        market: SpecialistAssessment | None,
        *,
        request_id: str,
        task_id: str,
    ) -> RoleModelOutput[str]: ...

    async def review_risk(
        self,
        question: str,
        investment: str,
        fundamental: SpecialistAssessment | None,
        news: SpecialistAssessment | None,
        market: SpecialistAssessment | None,
        *,
        request_id: str,
        task_id: str,
    ) -> RoleModelOutput[str]: ...


class QwenSpecialistModels:
    def __init__(self, client, registry: RoleRegistry | None = None):
        self.client = client
        self.registry = registry or RoleRegistry()
        self.registry.validate_prompts(_current_prompt_digests())

    def _call_options(self, role):
        definition = self.registry.get(role)
        options = {
            "purpose": definition.model_purpose,
            "prompt_version": definition.prompt_version,
        }
        profile = self.registry.model_profile(role)
        if profile is not None:
            options["profile_override"] = profile
        temperature = (
            profile.temperature
            if profile is not None and profile.temperature is not None
            else definition.default_temperature
        )
        if temperature is not None:
            options["temperature"] = temperature
        return options

    async def fundamental_turn(
        self,
        question,
        security,
        tools,
        results,
        *,
        request_id,
        task_id,
        allow_tools,
        trade_date,
    ):
        system_message = (
            "你是一名研究员，负责分析一家公司**过去一周**的基本面信息。请撰写一份全面的公司基本面报告，"
            "涵盖财报、公司档案、基础财务指标、财务历史等内容，形成完整基本面视图，为交易员提供信息支撑。"
            "尽可能详实，给出有支撑依据、可落地的洞察，辅助交易员做出决策。"
            "报告末尾附加一张 Markdown "
            "表格，汇总报告要点，排版清晰便于阅读。可用工具：\n\n"
            "- `get_company_profile`：综合基本面分析\n"
            "- `get_profitability`：利润表\n"
            "- `get_balance_sheet`：资产负债表\n"
            "- `get_cash_flow`：现金流量表"
            "\n\n调用上述四个工具时只传空对象{}；证券代码、as_of和request_id"
            "全部由程序强制注入。"
        )
        if not allow_tools:
            system_message += (
                "\n\n工具循环已经结束。现在必须依据现有结果直接输出最终Markdown，不得请求工具。"
            )
        system = _parallel_agent_system_prompt(
            tools,
            trade_date=trade_date,
            instrument_context=security,
            system_message=system_message,
        )
        model_tools = _fundamental_model_tool_metadata(tools)
        payload = {
            "objective": question,
            "security": security,
            "available_tools": model_tools,
            "previous_tool_results": [_compact_tool_result(item) for item in results],
            "required_tools_remaining": sorted(
                _FUNDAMENTAL_REQUIRED - {item.tool.value for item in results}
            ),
        }
        messages = [
            LLMMessage(role="system", content=system),
            LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
        ]
        if allow_tools and model_tools:
            native_names = {item["name"].replace(".", "__"): item["name"] for item in model_tools}
            definitions = tuple(
                {
                    "type": "function",
                    "function": {
                        "name": item["name"].replace(".", "__"),
                        "description": item["description"],
                        "parameters": item["argument_schema"],
                    },
                }
                for item in model_tools
            )
            output = await self.client.complete_with_tools(
                messages,
                definitions,
                **self._call_options(SpecialistRole.FUNDAMENTAL),
                request_id=request_id,
                task_id=task_id,
            )
            completion = output.completion
            turn = FundamentalTurn(
                completion.content,
                tuple(
                    call.model_copy(update={"name": native_names.get(call.name, call.name)})
                    for call in output.tool_calls
                ),
            )
        else:
            completion = await self.client.complete(
                messages,
                **self._call_options(SpecialistRole.FUNDAMENTAL),
                request_id=request_id,
                task_id=task_id,
            )
            turn = FundamentalTurn(completion.content)
        usage = completion.usage
        return RoleModelOutput(
            turn,
            0 if usage is None else usage.total_tokens,
            completion.invocation_id,
        )

    async def market_turn(
        self,
        question,
        security,
        tools,
        results,
        *,
        request_id,
        task_id,
        allow_tools,
        trade_date,
    ):
        system_message = _market_system_message()
        if not allow_tools:
            system_message += (
                "\n\n工具循环已经结束。现在必须依据现有结果直接输出最终Markdown，不得请求工具。"
            )
        system = _parallel_agent_system_prompt(
            tools,
            trade_date=trade_date,
            instrument_context=security,
            system_message=system_message,
        )
        payload = {
            "objective": question,
            "security": security,
            "available_tools": tools,
            "previous_tool_results": [_compact_market_tool_result(item) for item in results],
            "required_tools_remaining": sorted(
                _MARKET_REQUIRED - {item.tool.value for item in results}
            ),
        }
        messages = [
            LLMMessage(role="system", content=system),
            LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
        ]
        if allow_tools and tools:
            native_names = {item["name"].replace(".", "__"): item["name"] for item in tools}
            definitions = tuple(
                {
                    "type": "function",
                    "function": {
                        "name": item["name"].replace(".", "__"),
                        "description": item["description"],
                        "parameters": _market_model_argument_schema(item["name"]),
                    },
                }
                for item in tools
            )
            output = await self.client.complete_with_tools(
                messages,
                definitions,
                **self._call_options(SpecialistRole.MARKET),
                request_id=request_id,
                task_id=task_id,
            )
            completion = output.completion
            turn = MarketTurn(
                completion.content,
                tuple(
                    call.model_copy(update={"name": native_names.get(call.name, call.name)})
                    for call in output.tool_calls
                ),
            )
        else:
            completion = await self.client.complete(
                messages,
                **self._call_options(SpecialistRole.MARKET),
                request_id=request_id,
                task_id=task_id,
            )
            turn = MarketTurn(completion.content)
        usage = completion.usage
        return RoleModelOutput(
            turn,
            0 if usage is None else usage.total_tokens,
            completion.invocation_id,
        )

    async def decide(
        self,
        role,
        question,
        security,
        tools,
        results,
        *,
        request_id,
        task_id,
        trade_date,
    ):
        called = {item.tool.value for item in results}
        available = {item["name"] for item in tools}
        if role == SpecialistRole.FUNDAMENTAL:
            required = _FUNDAMENTAL_REQUIRED & available
        elif role == SpecialistRole.NEWS:
            required = _NEWS_REQUIRED & available
        else:
            required = _MARKET_REQUIRED
        if role == SpecialistRole.FUNDAMENTAL:
            responsibilities = (
                "公司基础档案、盈利、资产负债、现金流、数据质量与缺口；只能且必须分别调用"
                "get_company_profile、get_profitability、get_balance_sheet和get_cash_flow；"
                "Markdown必须包含：公司基础档案、盈利、资产负债、现金流、数据质量与限制。"
            )
        elif role == SpecialistRole.NEWS:
            responsibilities = (
                "公司新闻、正式公告、业绩预告与业绩快报、正负中性信号和来源覆盖；"
                "只能使用且必须分别调用get_stock_news、get_announcements和"
                "get_earnings_events，并严格使用程序固定的近7个自然日窗口和每工具最多10条结果；"
                "禁止使用行情数据冒充新闻；Markdown必须包含：公司新闻、公告、公司事件、"
                "行业与宏观影响、信号、来源覆盖。新闻缺失必须写coverage_incomplete，"
                "来源不可用还必须写news_source_unavailable。"
            )
        else:
            responsibilities = (
                "你是一名交易助手，负责对目标证券进行技术面分析。只能依据get_stock_data和"
                "get_indicators返回的数据分析价格趋势、动量、波动率及量价关系，不得推断公司基本面、"
                "新闻、公告、估值、股息率或宏观事件。必须先调用get_stock_data，读取截至当前分析日期的"
                "最近60根结构化OHLCV日线；取得OHLCV结果后，再调用get_indicators，从以下指标中选择"
                "最多8个信息互补、不重复的指标，读取最近30个完整交易日的预计算指标序列。"
                "可选指标仅限：移动均线close_50_sma、close_10_ema；MACD相关macd、macds、macdh；"
                "动量rsi；波动率boll、boll_ub、boll_lb、atr；量能vwma。调用get_indicators时必须严格"
                "使用上述原名，至少选择1个、最多8个且不得重复，并优先覆盖趋势、动量、波动率和量能。"
                "MACD线、信号线和柱状图应结合解释，布林中轨、上轨和下轨应结合解释；最终报告必须单独"
                "说明指标选择理由。所有日期、OHLCV、指标值、涨跌幅、支撑和压力判断只能依据工具返回的"
                "带日期记录及evidence ID。数据缺失、日期不一致或相互矛盾时，必须在数据质量与限制中"
                "明确列出，不得补值、修改或抹平差异。不得宣称历史回测有效，不得把技术信号描述成确定性"
                "预测；支撑、压力、突破、反转、超买和超卖只能表述为技术观察或条件性情景。ATR仅用于"
                "描述波动尺度和候选风险距离，不得生成个性化仓位方案或保证止损有效。报告必须包含："
                "价格趋势、移动均线、MACD与动量、波动率、量价关系、指标选择依据、数据质量与限制；"
                "末尾必须附加Markdown表格，汇总观察日期、主要信号、证据、条件、风险与限制。"
            )
        system_message = (
            f"你是A股{role.value}分析Agent，固定职责为：{responsibilities}"
            "每轮只能选择一个已授权工具，或在必要工具全部调用后结束并输出分析。"
            "证券代码、as_of和request_id由程序强制注入；不得尝试替换。"
            "工具参数必须严格符合输入schema，不得调用Tushare、SQL、Python或未列出的工具。"
            "输出必须包含完整report_markdown、结构化claims、evidence IDs、"
            "limitations和coverage_status。"
            "结论只能引用工具结果中的evidence_id；覆盖不完整不能解释为没有风险。"
            "结束时assessment.role必须等于当前角色。只输出给定JSON Schema。"
        )
        system = _parallel_agent_system_prompt(
            tools,
            trade_date=trade_date,
            instrument_context=security,
            system_message=system_message,
        )
        payload = {
            "question": question,
            "security": security,
            "available_tools": tools,
            "previous_tool_results": [_compact_tool_result(item) for item in results],
            "program_injected_request_id": request_id,
            "required_tools_remaining": sorted(required - called),
        }
        output = await self.client.complete_structured(
            [
                LLMMessage(role="system", content=system),
                LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
            ],
            SpecialistDecision,
            **self._call_options(role),
            request_id=request_id,
            task_id=task_id,
        )
        usage = output.completion.usage
        return RoleModelOutput(
            output.parsed,
            0 if usage is None else usage.total_tokens,
            output.completion.invocation_id,
        )

    async def synthesize_investment(
        self, question, fundamental, news, market, *, request_id, task_id
    ):
        payload = {
            "question": question,
            "fundamental": None if fundamental is None else fundamental.model_dump(mode="json"),
            "news": None if news is None else news.model_dump(mode="json"),
            "market": None if market is None else market.model_dump(mode="json"),
        }
        structured_messages = [
            LLMMessage(role="system", content=_investment_structured_system_message()),
            LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
        ]
        investment_options = self._call_options(SpecialistRole.INVESTMENT)
        try:
            output = await self.client.complete_structured(
                structured_messages,
                InvestmentAnalysisTransport,
                **investment_options,
                request_id=request_id,
                task_id=task_id,
                use_profile_max_output_tokens=(
                    self.registry.model_profile(SpecialistRole.INVESTMENT) is not None
                ),
            )
            draft = InvestmentAnalysisDraft.model_validate(output.parsed.model_dump())
            markdown = _render_investment_analysis(draft)
            completion = output.completion
        except Exception:
            markdown_messages = [
                LLMMessage(role="system", content=_investment_markdown_system_message()),
                LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
            ]
            completion = await self.client.complete(
                markdown_messages,
                **self._call_options(SpecialistRole.INVESTMENT),
                request_id=request_id,
                task_id=task_id,
            )
            markdown = completion.content.strip()
            if not markdown:
                raise ValueError("investment_markdown_required") from None
        usage = completion.usage
        return RoleModelOutput(
            markdown,
            0 if usage is None else usage.total_tokens,
            completion.invocation_id,
        )

    async def review_risk(
        self,
        question,
        investment,
        fundamental,
        news,
        market,
        *,
        request_id,
        task_id,
    ):
        system = _risk_review_system_message()
        payload = {
            "question": question,
            "investment_analysis_markdown": investment,
            "fundamental": _risk_agent_context(fundamental),
            "news": _risk_agent_context(news),
            "market": _risk_agent_context(market),
        }
        completion = await self.client.complete(
            [
                LLMMessage(role="system", content=system),
                LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
            ],
            **self._call_options(SpecialistRole.RISK),
            request_id=request_id,
            task_id=task_id,
        )
        markdown = completion.content.strip()
        if not markdown:
            raise ValueError("risk_markdown_required")
        usage = completion.usage
        return RoleModelOutput(
            markdown,
            0 if usage is None else usage.total_tokens,
            completion.invocation_id,
        )


def _prompt_digest(*components):
    """Hash literal prompt material, independent of dynamic request inputs."""
    literals = []

    def collect(code):
        for value in code.co_consts:
            if isinstance(value, str):
                literals.append(value)
            elif isinstance(value, CodeType):
                collect(value)

    for component in components:
        collect(component.__code__)
    return hashlib.sha256(
        json.dumps(literals, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _current_prompt_digests():
    shared = _parallel_agent_system_prompt
    decide = QwenSpecialistModels.decide
    return {
        SpecialistRole.FUNDAMENTAL: _prompt_digest(
            shared, QwenSpecialistModels.fundamental_turn, decide
        ),
        SpecialistRole.NEWS: _prompt_digest(shared, decide),
        SpecialistRole.MARKET: _prompt_digest(
            shared, _market_system_message, QwenSpecialistModels.market_turn, decide
        ),
        SpecialistRole.INVESTMENT: _prompt_digest(
            _investment_common_system_message,
            _investment_structured_system_message,
            _investment_markdown_system_message,
        ),
        SpecialistRole.RISK: _prompt_digest(_risk_review_system_message),
    }


@dataclass(frozen=True)
class SpecialistBudget:
    role_timeout_seconds: float = 120
    max_tool_calls: int = 6
    max_role_tokens: int = 6_000
    max_fundamental_tool_rounds: int = 20
    report_reserve_tokens: int = 10_000
    supplemental_call_reserve_tokens: int = 5_000


@dataclass(frozen=True)
class SpecialistOutcome:
    assessment: SpecialistAssessment | None
    results: tuple[BusinessToolResult, ...]
    trace: SpecialistInvocationTrace


def _outcome_payload(outcome):
    return {
        "assessment": (
            None if outcome.assessment is None else outcome.assessment.model_dump(mode="json")
        ),
        "results": [item.model_dump(mode="json") for item in outcome.results],
        "trace": outcome.trace.model_dump(mode="json"),
    }


def _node_payload(role, output):
    if role in {"fundamental", "news", "market"}:
        return _outcome_payload(output[f"{role}_outcome"])
    return {
        "text": output[role],
        "trace": output[f"{role}_trace"].model_dump(mode="json"),
    }


def _node_trace(role, output):
    return (
        output[f"{role}_outcome"].trace
        if role in {"fundamental", "news", "market"}
        else output[f"{role}_trace"]
    )


def _restore_node(role, payload):
    trace = SpecialistInvocationTrace.model_validate(payload["trace"])
    if trace.role.value != role:
        raise ValueError("research_checkpoint_role_mismatch")
    if role in {"fundamental", "news", "market"}:
        assessment = payload["assessment"]
        outcome = SpecialistOutcome(
            assessment=(
                None if assessment is None else SpecialistAssessment.model_validate(assessment)
            ),
            results=tuple(BusinessToolResult.model_validate(item) for item in payload["results"]),
            trace=trace,
        )
        if outcome.assessment is not None and outcome.assessment.role != role:
            raise ValueError("research_checkpoint_assessment_mismatch")
        return {f"{role}_outcome": outcome}
    text = payload["text"]
    if not isinstance(text, str) or not text.strip() or len(text) > 40_000:
        raise ValueError("research_checkpoint_text_invalid")
    return {role: text, f"{role}_trace": trace}


class SpecialistState(TypedDict, total=False):
    question: str
    request_id: str
    task_id: str
    security: dict
    as_of: object
    tools: BusinessToolRegistry
    checkpoint_store: object
    fundamental_outcome: SpecialistOutcome
    news_outcome: SpecialistOutcome
    market_outcome: SpecialistOutcome
    investment: str | None
    investment_trace: SpecialistInvocationTrace
    risk: str | None
    risk_trace: SpecialistInvocationTrace


class DynamicMultiAgentResearchGraph:
    def __init__(
        self,
        models: SpecialistModels,
        tools: BusinessToolRegistry | Callable[[dict], BusinessToolRegistry],
        *,
        budget: SpecialistBudget | None = None,
        checkpoint_store=None,
        max_task_tokens: int | None = None,
        registry: RoleRegistry | None = None,
    ):
        self.models = models
        self.registry = registry or getattr(models, "registry", None) or RoleRegistry()
        if getattr(models, "registry", self.registry) is not self.registry:
            raise ValueError("research_role_registry_mismatch")
        self.registry.validate_prompts(_current_prompt_digests())
        self.tool_factory = tools
        self.budget = budget or SpecialistBudget()
        self.checkpoint_store = checkpoint_store
        self.max_task_tokens = max_task_tokens
        graph = StateGraph(SpecialistState)
        graph.add_node("fundamental", self._fundamental_checkpointed)
        graph.add_node("news", self._news_checkpointed)
        graph.add_node("market", self._market_checkpointed)
        graph.add_node("investment", self._investment_checkpointed)
        graph.add_node("risk", self._risk_checkpointed)
        graph.add_edge(START, "fundamental")
        graph.add_edge(START, "news")
        graph.add_edge(START, "market")
        graph.add_edge(["fundamental", "news", "market"], "investment")
        graph.add_edge("investment", "risk")
        graph.add_edge("risk", END)
        self.graph = graph.compile()

    async def run(self, question, plan, security, execution, *, request_id, task_id, as_of):
        del execution
        security_payload = (
            security.model_dump(mode="json") if hasattr(security, "model_dump") else dict(security)
        )
        security_payload["research_plan"] = (
            plan.model_dump(mode="json") if hasattr(plan, "model_dump") else plan
        )
        tools = (
            self.tool_factory(security_payload)
            if callable(self.tool_factory)
            else self.tool_factory
        )
        state = await self.graph.ainvoke(
            {
                "question": question,
                "request_id": request_id,
                "task_id": task_id,
                "security": security_payload,
                "as_of": as_of,
                "tools": tools,
                "checkpoint_store": self.checkpoint_store,
            }
        )
        fundamental = state["fundamental_outcome"]
        news = state["news_outcome"]
        market = state["market_outcome"]
        result = SpecialistCollaborationResult(
            fundamental=fundamental.assessment,
            news=news.assessment,
            market=market.assessment,
            investment=state["investment"],
            risk=state["risk"],
            tool_results=(*fundamental.results, *news.results, *market.results),
            traces=(
                fundamental.trace,
                news.trace,
                market.trace,
                state["investment_trace"],
                state["risk_trace"],
            ),
        )
        total_tokens = sum(item.tokens for item in result.traces)
        if self.max_task_tokens is not None and total_tokens > self.max_task_tokens:
            raise ValueError("research_token_budget_exceeded")
        return result, total_tokens

    async def _checkpointed(self, role, state, execute):
        store = state["checkpoint_store"]
        if store is None:
            return await execute(state)
        version = self.registry.get(role).prompt_version
        upstream = {}
        if role in {"investment", "risk"}:
            upstream = {
                name: _outcome_payload(state[f"{name}_outcome"])
                for name in ("fundamental", "news", "market")
            }
        if role == "risk":
            upstream["investment"] = state["investment"]
            upstream["investment_trace"] = state["investment_trace"].model_dump(mode="json")
        input_payload = {
            "question": state["question"],
            "security": state["security"],
            "as_of": state["as_of"].isoformat(),
            "upstream": upstream,
            "role": role,
            "prompt_version": version,
            "schema_version": 1,
        }
        digest = hashlib.sha256(
            json.dumps(
                input_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode()
        ).hexdigest()
        saved = await store.load(role, digest, version, 1)
        if saved is not None:
            try:
                output = _restore_node(role, saved)
            except (ValueError, TypeError, KeyError):
                output = None
            if output is not None:
                return output
        if self.max_task_tokens is not None:
            spent = sum(
                state[f"{name}_outcome"].trace.tokens
                for name in ("fundamental", "news", "market")
                if f"{name}_outcome" in state
            )
            if "investment_trace" in state:
                spent += state["investment_trace"].tokens
            if spent >= self.max_task_tokens:
                raise ValueError("research_token_budget_exceeded")
        await store.active()
        output = await execute(state)
        payload = _node_payload(role, output)
        trace = _node_trace(role, output)
        await store.save(role, digest, version, 1, payload, trace.tokens)
        return output

    async def _fundamental_checkpointed(self, state):
        return await self._checkpointed("fundamental", state, self._fundamental)

    async def _news_checkpointed(self, state):
        return await self._checkpointed("news", state, self._news)

    async def _market_checkpointed(self, state):
        return await self._checkpointed("market", state, self._market)

    async def _investment_checkpointed(self, state):
        return await self._checkpointed("investment", state, self._investment)

    async def _risk_checkpointed(self, state):
        return await self._checkpointed("risk", state, self._risk)

    async def _fundamental(self, state):
        return {"fundamental_outcome": await self._run_fundamental(state)}

    async def _news(self, state):
        return {
            "news_outcome": await self._run_specialist(
                SpecialistRole.NEWS, self.registry.get(SpecialistRole.NEWS).tool_role, state
            )
        }

    async def _market(self, state):
        return {"market_outcome": await self._run_market(state)}

    async def _investment(self, state):
        started = perf_counter()
        fundamental = state["fundamental_outcome"].assessment
        news = state["news_outcome"].assessment
        market = state["market_outcome"].assessment
        try:
            async with asyncio.timeout(self.budget.role_timeout_seconds):
                output = await self.models.synthesize_investment(
                    state["question"],
                    fundamental,
                    news,
                    market,
                    request_id=state["request_id"],
                    task_id=state["task_id"],
                )
                if output.tokens > self.budget.max_role_tokens:
                    raise ValueError("role_token_budget_exceeded")
                investment = str(output.value).strip()
                if not investment:
                    raise ValueError("investment_markdown_required")
                return {
                    "investment": investment,
                    "investment_trace": self._trace(
                        SpecialistRole.INVESTMENT,
                        RoleStatus.SUCCEEDED,
                        INVESTMENT_ANALYSIS_PROMPT_VERSION,
                        output.tokens,
                        (),
                        started,
                        invocation_ids=(output.invocation_id,),
                    ),
                }
        except TimeoutError:
            code = "role_timeout"
        except Exception as exc:
            code = str(exc) if str(exc) == "role_token_budget_exceeded" else "role_model_failed"
        fallback = _investment_fallback_markdown(code)
        return {
            "investment": fallback,
            "investment_trace": self._trace(
                SpecialistRole.INVESTMENT,
                RoleStatus.SUCCEEDED,
                INVESTMENT_ANALYSIS_PROMPT_VERSION,
                0,
                (),
                started,
            ),
        }

    async def _run_fundamental(self, state):
        started = perf_counter()
        results = []
        tokens = 0
        invocation_ids = []
        tool_attempts = []
        tool_role = self.registry.get(SpecialistRole.FUNDAMENTAL).tool_role
        all_metadata = state["tools"].metadata_for(tool_role)
        attempted_fingerprints = set()
        try:
            markdown = ""
            for _ in range(self.budget.max_fundamental_tool_rounds):
                selection_allowance = max(
                    0,
                    self.budget.max_role_tokens - self.budget.report_reserve_tokens,
                )
                if tokens and (
                    tokens + self.budget.supplemental_call_reserve_tokens > selection_allowance
                ):
                    break
                output = await self.models.fundamental_turn(
                    state["question"],
                    state["security"],
                    all_metadata,
                    tuple(results),
                    request_id=state["request_id"],
                    task_id=state["task_id"],
                    allow_tools=True,
                    trade_date=state["as_of"].date().isoformat(),
                )
                tokens += output.tokens
                if output.invocation_id is not None:
                    invocation_ids.append(output.invocation_id)
                turn = output.value
                if turn.tool_calls:
                    raw_calls = tuple(
                        {"name": call.name, "arguments": call.arguments} for call in turn.tool_calls
                    )
                    batch_results, batch_attempts, _ = await self._execute_tool_batch(
                        raw_calls,
                        tool_role,
                        state,
                        attempted_fingerprints,
                        first_sequence=len(tool_attempts) + 1,
                        invocation_id=output.invocation_id,
                    )
                    results.extend(batch_results)
                    tool_attempts.extend(batch_attempts)
                    continue
                missing = _FUNDAMENTAL_REQUIRED - {item.tool.value for item in results}
                if not missing:
                    markdown = turn.markdown
                    break

            missing = _FUNDAMENTAL_REQUIRED - {item.tool.value for item in results}
            if missing:
                raise ValueError("fundamental_required_tools_missing")

            if not markdown.strip():
                output = await self.models.fundamental_turn(
                    state["question"],
                    state["security"],
                    (),
                    tuple(results),
                    request_id=state["request_id"],
                    task_id=state["task_id"],
                    allow_tools=False,
                    trade_date=state["as_of"].date().isoformat(),
                )
                tokens += output.tokens
                if output.invocation_id is not None:
                    invocation_ids.append(output.invocation_id)
                markdown = output.value.markdown

            if not markdown.strip():
                raise ValueError("fundamental_markdown_empty")
            assessment = SpecialistAssessment(
                role="fundamental",
                summary="Fundamental Agent已完成Markdown报告。",
                report_markdown=markdown,
                coverage_status=_coverage_status(results),
                limitations=_limitations(tuple(results), news_available=True),
            )
            return SpecialistOutcome(
                assessment,
                tuple(results),
                self._trace(
                    SpecialistRole.FUNDAMENTAL,
                    RoleStatus.SUCCEEDED,
                    FUNDAMENTAL_PROMPT_VERSION,
                    tokens,
                    results,
                    started,
                    invocation_ids=invocation_ids,
                    tool_attempts=tool_attempts,
                ),
            )
        except Exception as exc:
            code = (
                str(exc)
                if str(exc) in {"fundamental_markdown_empty", "fundamental_required_tools_missing"}
                else "role_model_failed"
            )
        return self._fallback_outcome(
            SpecialistRole.FUNDAMENTAL,
            results,
            FUNDAMENTAL_PROMPT_VERSION,
            tokens,
            invocation_ids,
            tool_attempts,
            started,
            code,
            news_available=True,
        )

    async def _run_market(self, state):
        started = perf_counter()
        results = []
        tokens = 0
        invocation_ids = []
        tool_attempts = []
        attempted_fingerprints = set()
        metadata_by_name = {
            item["name"]: item
            for item in state["tools"].metadata_for(
                self.registry.get(SpecialistRole.MARKET).tool_role
            )
        }
        rounds = 0
        try:
            for expected_name in ("get_stock_data", "get_indicators"):
                completed = False
                while rounds < self.budget.max_tool_calls:
                    rounds += 1
                    metadata = (
                        (metadata_by_name[expected_name],)
                        if expected_name in metadata_by_name
                        else ()
                    )
                    output = await self.models.market_turn(
                        state["question"],
                        state["security"],
                        metadata,
                        tuple(results),
                        request_id=state["request_id"],
                        task_id=state["task_id"],
                        allow_tools=True,
                        trade_date=state["as_of"].date().isoformat(),
                    )
                    tokens += output.tokens
                    if output.invocation_id is not None:
                        invocation_ids.append(output.invocation_id)
                    turn = output.value
                    if not turn.tool_calls:
                        continue
                    if len(turn.tool_calls) != 1 or turn.tool_calls[0].name != expected_name:
                        raw_call = turn.tool_calls[0]
                        tool_attempts.append(
                            _tool_attempt(
                                len(tool_attempts) + 1,
                                output.invocation_id,
                                _audited_tool_name(raw_call.name),
                                raw_call.arguments,
                                "selection",
                                "failed",
                                "market_tool_order_invalid",
                            )
                        )
                        raise ValueError("market_tool_order_invalid")
                    raw_call = {
                        "name": turn.tool_calls[0].name,
                        "arguments": turn.tool_calls[0].arguments,
                    }
                    result, attempt, error = await self._execute_tool_attempt(
                        raw_call,
                        self.registry.get(SpecialistRole.MARKET).tool_role,
                        state,
                        attempted_fingerprints,
                        sequence=len(tool_attempts) + 1,
                        invocation_id=output.invocation_id,
                    )
                    tool_attempts.append(attempt)
                    if result is None:
                        raise ValueError(error or "tool_execution_failed")
                    results.append(result)
                    completed = True
                    break
                if not completed:
                    raise ValueError("market_required_tools_missing")

            output = await self.models.market_turn(
                state["question"],
                state["security"],
                (),
                tuple(results),
                request_id=state["request_id"],
                task_id=state["task_id"],
                allow_tools=False,
                trade_date=state["as_of"].date().isoformat(),
            )
            tokens += output.tokens
            if output.invocation_id is not None:
                invocation_ids.append(output.invocation_id)
            markdown = output.value.markdown
            if not markdown.strip():
                raise ValueError("market_markdown_empty")
            assessment = SpecialistAssessment(
                role="market",
                summary="Market Agent已完成Markdown技术面报告。",
                report_markdown=markdown,
                coverage_status=_coverage_status(results),
                limitations=_limitations(tuple(results), news_available=True),
            )
            _validate_specialist_content(assessment)
            return SpecialistOutcome(
                assessment,
                tuple(results),
                self._trace(
                    SpecialistRole.MARKET,
                    RoleStatus.SUCCEEDED,
                    MARKET_PROMPT_VERSION,
                    tokens,
                    results,
                    started,
                    invocation_ids=invocation_ids,
                    tool_attempts=tool_attempts,
                ),
            )
        except Exception as exc:
            code = (
                str(exc)
                if str(exc)
                in {
                    "market_markdown_empty",
                    "market_required_tools_missing",
                    "market_tool_order_invalid",
                    "tool_arguments_invalid",
                    "tool_date_invalid",
                    "tool_duplicate_call",
                    "tool_execution_failed",
                    "tool_name_invalid",
                    "tool_response_invalid",
                }
                else "role_model_failed"
            )
        return self._fallback_outcome(
            SpecialistRole.MARKET,
            results,
            MARKET_PROMPT_VERSION,
            tokens,
            invocation_ids,
            tool_attempts,
            started,
            code,
            news_available=True,
        )

    async def _execute_tool_batch(
        self,
        raw_calls,
        tool_role,
        state,
        attempted_fingerprints,
        *,
        first_sequence,
        invocation_id,
    ):
        tasks = [
            self._execute_tool_attempt(
                raw_call,
                tool_role,
                state,
                attempted_fingerprints,
                sequence=first_sequence + index,
                invocation_id=invocation_id,
            )
            for index, raw_call in enumerate(raw_calls)
        ]
        completed = await asyncio.gather(*tasks)
        results = tuple(item[0] for item in completed if item[0] is not None)
        attempts = tuple(item[1] for item in completed)
        errors = tuple(item[2] for item in completed if item[2] is not None)
        return results, attempts, errors

    async def _execute_tool_attempt(
        self,
        raw_call,
        tool_role,
        state,
        attempted_fingerprints,
        *,
        sequence,
        invocation_id,
    ):
        raw_name, raw_arguments = _tool_attempt_input(raw_call)
        audited_name = _audited_tool_name(raw_name)
        available_names = {item["name"] for item in state["tools"].metadata_for(tool_role)}
        if audited_name == "unrecognized" or raw_name not in available_names:
            return (
                None,
                _tool_attempt(
                    sequence,
                    invocation_id,
                    audited_name,
                    raw_arguments,
                    "selection",
                    "failed",
                    "tool_name_invalid",
                ),
                "tool_name_invalid",
            )
        try:
            secured = _secure_tool_call(
                raw_call,
                code=state["security"]["code"],
                as_of=state["as_of"],
                request_id=state["request_id"],
            )
        except (TypeError, ValueError):
            error_code = (
                "tool_date_invalid"
                if _contains_invalid_date(raw_arguments)
                else "tool_arguments_invalid"
            )
            return (
                None,
                _tool_attempt(
                    sequence,
                    invocation_id,
                    audited_name,
                    raw_arguments,
                    "security_injection",
                    "failed",
                    error_code,
                ),
                error_code,
            )
        try:
            tool_call = BUSINESS_TOOL_CALL.validate_python(secured)
        except ValidationError:
            return (
                None,
                _tool_attempt(
                    sequence,
                    invocation_id,
                    audited_name,
                    secured.get("arguments"),
                    "schema_validation",
                    "failed",
                    "tool_arguments_invalid",
                ),
                "tool_arguments_invalid",
            )
        fingerprint = _tool_fingerprint(secured)
        if fingerprint in attempted_fingerprints:
            return (
                None,
                _tool_attempt(
                    sequence,
                    invocation_id,
                    audited_name,
                    secured["arguments"],
                    "selection",
                    "failed",
                    "tool_duplicate_call",
                ),
                "tool_duplicate_call",
            )
        attempted_fingerprints.add(fingerprint)
        try:
            result = await state["tools"].execute(tool_call, tool_role)
        except ValidationError:
            error_code = "tool_response_invalid"
        except ValueError as exc:
            error_code = (
                "tool_response_invalid"
                if str(exc)
                in {
                    "tool_handler_response_identity_mismatch",
                    "tool_handler_exceeded_record_limit",
                }
                else "tool_execution_failed"
            )
        except Exception:
            error_code = "tool_execution_failed"
        else:
            return (
                result,
                _tool_attempt(
                    sequence,
                    invocation_id,
                    audited_name,
                    secured["arguments"],
                    "completed",
                    "succeeded",
                ),
                None,
            )
        return (
            None,
            _tool_attempt(
                sequence,
                invocation_id,
                audited_name,
                secured["arguments"],
                "execution",
                "failed",
                error_code,
            ),
            error_code,
        )

    async def _run_specialist(self, role, tool_role, state):
        started = perf_counter()
        results = []
        tokens = 0
        invocation_ids = []
        tool_attempts = []
        prompt_version = (
            FUNDAMENTAL_PROMPT_VERSION
            if role == SpecialistRole.FUNDAMENTAL
            else NEWS_PROMPT_VERSION
            if role == SpecialistRole.NEWS
            else MARKET_PROMPT_VERSION
        )
        all_metadata = state["tools"].metadata_for(tool_role)
        available_names = {item["name"] for item in all_metadata}
        news_available = role != SpecialistRole.NEWS or bool(
            {"get_stock_news", "get_announcements", "get_earnings_events"} & available_names
        )
        if role == SpecialistRole.FUNDAMENTAL:
            required = _FUNDAMENTAL_REQUIRED & available_names
        elif role == SpecialistRole.NEWS:
            required = _NEWS_REQUIRED & available_names
        else:
            required = _MARKET_REQUIRED
        try:
            async with asyncio.timeout(self.budget.role_timeout_seconds):
                for _ in range(self.budget.max_tool_calls):
                    called = {item.tool.value for item in results}
                    if role == SpecialistRole.MARKET and tool_role == AgentRole.MARKET:
                        next_market_tool = (
                            "get_stock_data"
                            if "get_stock_data" not in called
                            else "get_indicators"
                            if "get_indicators" not in called
                            else None
                        )
                        metadata = tuple(
                            item for item in all_metadata if item["name"] == next_market_tool
                        )
                    else:
                        metadata = tuple(
                            item for item in all_metadata if item["name"] not in called
                        )
                    output = await self.models.decide(
                        role,
                        state["question"],
                        state["security"],
                        metadata,
                        tuple(results),
                        request_id=state["request_id"],
                        task_id=state["task_id"],
                        trade_date=state["as_of"].date().isoformat(),
                    )
                    tokens += output.tokens
                    if output.invocation_id is not None:
                        invocation_ids.append(output.invocation_id)
                    if tokens > self.budget.max_role_tokens:
                        return self._fallback_outcome(
                            role,
                            results,
                            prompt_version,
                            tokens,
                            invocation_ids,
                            tool_attempts,
                            started,
                            "role_token_budget_exceeded",
                            news_available=news_available,
                        )
                    decision = SpecialistDecision.model_validate(output.value)
                    if decision.action == "finish":
                        if required - called:
                            continue
                        if decision.assessment.role != role.value:
                            raise ValueError("specialist_role_mismatch")
                        assessment = _normalize_assessment(
                            decision.assessment,
                            tuple(results),
                            news_available=news_available,
                        )
                        _validate_specialist_content(assessment)
                        self._validate_citations(assessment, tuple(results))
                        return SpecialistOutcome(
                            assessment,
                            tuple(results),
                            self._trace(
                                role,
                                RoleStatus.SUCCEEDED,
                                prompt_version,
                                tokens,
                                results,
                                started,
                                invocation_ids=invocation_ids,
                                tool_attempts=tool_attempts,
                            ),
                        )
                    sequence = len(tool_attempts) + 1
                    raw_name, raw_arguments = _tool_attempt_input(decision.tool_call)
                    audited_name = _audited_tool_name(raw_name)
                    invocation_id = output.invocation_id
                    if audited_name == "unrecognized" or raw_name not in available_names:
                        tool_attempts.append(
                            _tool_attempt(
                                sequence,
                                invocation_id,
                                audited_name,
                                raw_arguments,
                                "selection",
                                "failed",
                                "tool_name_invalid",
                            )
                        )
                        return self._fallback_outcome(
                            role,
                            results,
                            prompt_version,
                            tokens,
                            invocation_ids,
                            tool_attempts,
                            started,
                            "tool_name_invalid",
                            news_available=news_available,
                        )
                    try:
                        secured = _secure_tool_call(
                            decision.tool_call,
                            code=state["security"]["code"],
                            as_of=state["as_of"],
                            request_id=state["request_id"],
                        )
                    except (TypeError, ValueError):
                        error_code = (
                            "tool_date_invalid"
                            if _contains_invalid_date(raw_arguments)
                            else "tool_arguments_invalid"
                        )
                        tool_attempts.append(
                            _tool_attempt(
                                sequence,
                                invocation_id,
                                audited_name,
                                raw_arguments,
                                "security_injection",
                                "failed",
                                error_code,
                            )
                        )
                        return self._fallback_outcome(
                            role,
                            results,
                            prompt_version,
                            tokens,
                            invocation_ids,
                            tool_attempts,
                            started,
                            error_code,
                            news_available=news_available,
                        )
                    try:
                        tool_call = BUSINESS_TOOL_CALL.validate_python(secured)
                    except ValidationError:
                        tool_attempts.append(
                            _tool_attempt(
                                sequence,
                                invocation_id,
                                audited_name,
                                secured.get("arguments"),
                                "schema_validation",
                                "failed",
                                "tool_arguments_invalid",
                            )
                        )
                        return self._fallback_outcome(
                            role,
                            results,
                            prompt_version,
                            tokens,
                            invocation_ids,
                            tool_attempts,
                            started,
                            "tool_arguments_invalid",
                            news_available=news_available,
                        )
                    if tool_call.name in called:
                        tool_attempts.append(
                            _tool_attempt(
                                sequence,
                                invocation_id,
                                audited_name,
                                secured["arguments"],
                                "selection",
                                "failed",
                                "tool_duplicate_call",
                            )
                        )
                        continue
                    try:
                        result = await state["tools"].execute(tool_call, tool_role)
                    except ValidationError:
                        error_code = "tool_response_invalid"
                    except ValueError as exc:
                        error_code = (
                            "tool_response_invalid"
                            if str(exc)
                            in {
                                "tool_handler_response_identity_mismatch",
                                "tool_handler_exceeded_record_limit",
                            }
                            else "tool_execution_failed"
                        )
                    except Exception:
                        error_code = "tool_execution_failed"
                    else:
                        results.append(result)
                        tool_attempts.append(
                            _tool_attempt(
                                sequence,
                                invocation_id,
                                audited_name,
                                secured["arguments"],
                                "completed",
                                "succeeded",
                            )
                        )
                        continue
                    tool_attempts.append(
                        _tool_attempt(
                            sequence,
                            invocation_id,
                            audited_name,
                            secured["arguments"],
                            "execution",
                            "failed",
                            error_code,
                        )
                    )
                    return self._fallback_outcome(
                        role,
                        results,
                        prompt_version,
                        tokens,
                        invocation_ids,
                        tool_attempts,
                        started,
                        error_code,
                        news_available=news_available,
                    )
                return self._fallback_outcome(
                    role,
                    results,
                    prompt_version,
                    tokens,
                    invocation_ids,
                    tool_attempts,
                    started,
                    "role_tool_call_budget_exceeded",
                    news_available=news_available,
                )
        except TimeoutError:
            code = "role_timeout"
        except Exception as exc:
            code = (
                str(exc)
                if str(exc)
                in {
                    "specialist_role_mismatch",
                    "specialist_citations_not_closed",
                    "specialist_report_content_invalid",
                }
                else "role_model_or_tool_failed"
            )
        return self._fallback_outcome(
            role,
            results,
            prompt_version,
            tokens,
            invocation_ids,
            tool_attempts,
            started,
            code,
            news_available=news_available,
        )

    def _fallback_outcome(
        self,
        role,
        results,
        prompt_version,
        tokens,
        invocation_ids,
        tool_attempts,
        started,
        reason,
        *,
        news_available,
    ):
        assessment = _fallback_assessment(
            role,
            tuple(results),
            reason,
            news_available=news_available,
        )
        return SpecialistOutcome(
            assessment,
            tuple(results),
            self._trace(
                role,
                RoleStatus.FAILED,
                prompt_version,
                tokens,
                results,
                started,
                invocation_ids=invocation_ids,
                tool_attempts=tool_attempts,
                error_code=reason,
            ),
        )

    async def _risk(self, state):
        started = perf_counter()
        fundamental = state["fundamental_outcome"].assessment
        news = state["news_outcome"].assessment
        market = state["market_outcome"].assessment
        investment = state["investment"]
        try:
            async with asyncio.timeout(self.budget.role_timeout_seconds):
                output = await self.models.review_risk(
                    state["question"],
                    investment,
                    fundamental,
                    news,
                    market,
                    request_id=state["request_id"],
                    task_id=state["task_id"],
                )
                if output.tokens > self.budget.max_role_tokens:
                    raise ValueError("role_token_budget_exceeded")
                risk = str(output.value).strip()
                compact = "".join(risk.split())
                if len(compact) < 100 or compact.isdigit():
                    raise ValueError("risk_markdown_not_meaningful")
                return {
                    "risk": risk,
                    "risk_trace": self._trace(
                        SpecialistRole.RISK,
                        RoleStatus.SUCCEEDED,
                        RISK_REVIEW_PROMPT_VERSION,
                        output.tokens,
                        (),
                        started,
                        invocation_ids=(output.invocation_id,),
                    ),
                }
        except TimeoutError:
            code = "role_timeout"
        except Exception as exc:
            code = str(exc) if str(exc) == "role_token_budget_exceeded" else "role_model_failed"
        fallback = (
            "# 风险终审\n\n"
            "## 最终评级\n\n"
            "**最终评级：无法形成**\n\n"
            "## 风险综合评估\n\n"
            "风险终审模型未形成可交付结果，不能确认上游投资计划已充分覆盖关键风险。\n\n"
            "## 终审主线逻辑\n\n"
            "本次终审未完成，不维持、不调整也不否决Investment初评；该结果不代表风险审查通过。\n\n"
            "## 最终交易动作\n\n"
            "暂不扩大相对标准仓位，不依据本次未完成的终审新增入场或具体止损价位，等待风险终审恢复。\n\n"
            "## 评级调整\n\n"
            f"未形成评级调整结论。失败代码：{code}。"
        )
        return {
            "risk": fallback,
            "risk_trace": self._trace(
                SpecialistRole.RISK,
                RoleStatus.SUCCEEDED,
                RISK_REVIEW_PROMPT_VERSION,
                0,
                (),
                started,
            ),
        }

    @staticmethod
    def _validate_citations(assessment, results):
        allowed = {item.evidence_id for result in results for item in result.evidence}
        claims = assessment.risks if isinstance(assessment, RiskSynthesis) else assessment.claims
        referenced = {item for claim in claims for item in claim.evidence_ids}
        if not referenced.issubset(allowed):
            raise ValueError("specialist_citations_not_closed")

    @staticmethod
    def _trace(
        role,
        status,
        prompt_version,
        tokens,
        results,
        started,
        error_code=None,
        invocation_ids=(),
        tool_attempts=(),
    ):
        return SpecialistInvocationTrace(
            role=role,
            status=status,
            prompt_version=prompt_version,
            tokens=tokens,
            invocation_ids=tuple(invocation_ids),
            tool_calls=len(results),
            tool_attempts=tuple(tool_attempts),
            elapsed_ms=max(0, round((perf_counter() - started) * 1000)),
            error_code=error_code,
        )


def _tool_attempt_input(value):
    if not isinstance(value, dict):
        return "", None
    return str(value.get("name") or ""), value.get("arguments")


def _audited_tool_name(value):
    return value if value in _AUDITED_TOOL_NAMES else "unrecognized"


def _tool_attempt(
    sequence,
    invocation_id,
    tool_name,
    arguments,
    stage,
    status,
    error_code=None,
):
    argument_keys, sanitized = _sanitize_tool_arguments(arguments)
    return ToolAttemptRecord(
        sequence=sequence,
        invocation_id=invocation_id,
        tool_name=tool_name,
        argument_keys=argument_keys,
        sanitized_arguments=sanitized,
        stage=stage,
        status=status,
        error_code=error_code,
    )


def _sanitize_tool_arguments(value):
    if not isinstance(value, dict):
        return (), {"arguments_shape": "invalid"}
    keys = tuple(sorted(str(key) for key in value if str(key) in _AUDITED_ARGUMENT_KEYS))
    sanitized = {}
    for key in keys:
        item = value[key]
        if key == "as_of_window":
            sanitized[key] = _safe_date(item)
        elif key == "as_of":
            sanitized[key] = _safe_timestamp(item)
        elif key == "code":
            sanitized[key] = _safe_code(item)
        elif key == "indicators":
            sanitized[key] = _safe_enum_list(item)
        elif key in {"limit", "output_days", "window"}:
            sanitized[key] = (
                item if isinstance(item, int) and not isinstance(item, bool) else "invalid"
            )
    unknown_count = len(value) - len(keys)
    if unknown_count:
        sanitized["unrecognized_field_count"] = unknown_count
    return keys, sanitized


def _safe_code(value):
    if not isinstance(value, str) or len(value) > 16:
        return "invalid"
    head, separator, suffix = value.partition(".")
    if separator and head.isdigit() and suffix in {"SH", "SZ", "BJ"}:
        return value
    return "invalid"


def _safe_date(value):
    try:
        return date.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError):
        return "invalid"


def _safe_timestamp(value):
    try:
        return datetime.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError):
        return "invalid"


def _safe_enum_list(value):
    if not isinstance(value, list | tuple) or len(value) > 10:
        return ["invalid"]
    return [item if item in _AUDITED_ENUM_VALUES else "invalid" for item in value]


def _contains_invalid_date(arguments):
    if not isinstance(arguments, dict):
        return False
    for field in ("start", "end", "period_end", "on", "as_of_window"):
        if field in arguments and _safe_date(arguments[field]) == "invalid":
            return True
    if "as_of" in arguments and _safe_timestamp(arguments["as_of"]) == "invalid":
        return True
    return False


def _secure_tool_call(value, *, code, as_of, request_id):
    raw = dict(value or {})
    name = str(raw.get("name") or "")
    arguments = dict(raw.get("arguments") or {})
    if name == "get_stock_data":
        arguments = {}
    elif name == "get_indicators":
        arguments = {"indicators": arguments["indicators"]} if "indicators" in arguments else {}
    cutoff = as_of.date()
    arguments["code"] = code
    arguments["as_of"] = as_of.isoformat()
    if name == "get_stock_data":
        arguments["as_of_window"] = cutoff.isoformat()
        arguments["window"] = 60
    elif name == "get_indicators":
        arguments["as_of_window"] = cutoff.isoformat()
        arguments["output_days"] = 30
    for field in ("start", "end", "period_end", "on", "as_of_window"):
        current = arguments.get(field)
        if current is None:
            continue
        parsed = date.fromisoformat(str(current))
        arguments[field] = min(parsed, cutoff).isoformat()
    return {"name": name, "request_id": request_id, "arguments": arguments}


def _market_model_argument_schema(name):
    if name == "get_stock_data":
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }
    if name == "get_indicators":
        return {
            "type": "object",
            "properties": {
                "indicators": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": [
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
                        ],
                    },
                    "minItems": 1,
                    "maxItems": 8,
                    "uniqueItems": True,
                }
            },
            "required": ["indicators"],
            "additionalProperties": False,
        }
    raise ValueError("market_tool_name_invalid")


def _fundamental_model_argument_schema(name):
    if name in _FUNDAMENTAL_REQUIRED:
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }
    raise ValueError("fundamental_tool_name_invalid")


def _fundamental_model_tool_metadata(tools):
    return [
        {
            "name": item["name"],
            "description": item["description"],
            "argument_schema": _fundamental_model_argument_schema(item["name"]),
        }
        for item in tools
    ]


def _market_model_tool_metadata(tools):
    return [
        {
            "name": item["name"],
            "description": item["description"],
            "argument_schema": _market_model_argument_schema(item["name"]),
        }
        for item in tools
    ]


def _compact_tool_result(result):
    value = result.model_dump(mode="json")
    value["items"] = value["items"][:30]
    value["evidence"] = value["evidence"][:30]
    return value


def _compact_market_tool_result(result):
    value = result.model_dump(mode="json")
    limits = {
        "get_stock_data": 60,
        "get_indicators": 30,
    }
    value["items"] = value["items"][: limits.get(result.tool.value, 30)]
    for item in value["items"]:
        item.pop("evidence_id", None)
    value["evidence"] = []
    return value


def _tool_fingerprint(secured):
    canonical = json.dumps(
        {
            "name": secured["name"],
            "arguments": secured["arguments"],
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _coverage_status(results):
    if not results:
        return "coverage_incomplete"
    statuses = {item.status.value for item in results}
    if statuses & {"source_unavailable", "permission_denied"}:
        return "source_unavailable"
    if any(not item.coverage.complete for item in results) or "coverage_incomplete" in statuses:
        return "coverage_incomplete"
    if statuses & {"partial", "no_data"}:
        return "partial"
    return "complete"


def _limitations(results, *, news_available):
    values = [gap.reason for result in results for gap in result.gaps]
    if any(not result.coverage.complete for result in results):
        values.append("coverage_incomplete")
    news_results = [
        item
        for item in results
        if item.tool.value in {"get_stock_news", "get_announcements", "get_earnings_events"}
    ]
    if not news_available or any(
        item.status.value in {"source_unavailable", "permission_denied"} for item in news_results
    ):
        values.extend(("coverage_incomplete", "news_source_unavailable"))
    return tuple(dict.fromkeys(values))[:20]


def _normalize_assessment(assessment, results, *, news_available):
    derived = _limitations(results, news_available=news_available)
    limitations = tuple(dict.fromkeys((*assessment.limitations, *derived)))[:20]
    return assessment.model_copy(
        update={
            "coverage_status": _coverage_status(results),
            "limitations": limitations,
        }
    )


def _required_report_headings(role):
    if role == SpecialistRole.FUNDAMENTAL or role == "fundamental":
        return (
            "公司基础档案",
            "盈利",
            "资产负债",
            "现金流",
            "数据质量与限制",
        )
    if role == SpecialistRole.NEWS or role == "news":
        return ("公司新闻", "公告", "公司事件", "行业与宏观影响", "信号", "来源覆盖")
    return (
        "价格趋势",
        "移动均线",
        "MACD与动量",
        "波动率",
        "量价关系",
        "指标选择依据",
        "数据质量与限制",
    )


def _validate_specialist_content(assessment):
    compact_summary = "".join(assessment.summary.split())
    compact_report = "".join(assessment.report_markdown.split())
    headings = _required_report_headings(assessment.role)
    if (
        len(compact_summary) < 4
        or compact_summary.isdigit()
        or len(compact_report) < 40
        or compact_report.isdigit()
        or any(heading not in assessment.report_markdown for heading in headings)
    ):
        raise ValueError("specialist_report_content_invalid")


def _fallback_assessment(role, results, reason, *, news_available):
    coverage = _coverage_status(results)
    limitations = tuple(
        dict.fromkeys((reason, *_limitations(results, news_available=news_available)))
    )[:20]
    evidence_count = sum(len(item.evidence) for item in results)
    if role == SpecialistRole.FUNDAMENTAL:
        markdown = (
            "# 基本面分析\n\n"
            "## 公司基础档案\n仅陈述公司档案域已返回记录，未覆盖部分列入限制。\n\n"
            "## 盈利\n依据两年年度与最近四个独立单季记录，缺失数据不作推断。\n\n"
            "## 资产负债\n仅陈述最近四个季度末时点记录，未返回项目保持未知。\n\n"
            "## 现金流\n仅陈述最近四个独立单季记录，未返回项目保持未知。\n\n"
            f"## 数据质量与限制\n强制降级报告；证据数={evidence_count}；原因={reason}。"
        )
        summary = "基本面模型未正常结束，系统已根据现有工具证据保留降级报告。"
    elif role == SpecialistRole.NEWS:
        markdown = (
            "# 新闻分析\n\n"
            "## 公司新闻\n仅汇总已授权且实际返回的有限来源。\n\n"
            "## 公告\n公告工具当前不可用，不推断不存在公告。\n\n"
            "## 公司事件\n仅陈述已返回事件，未返回不代表没有事件。\n\n"
            "## 行业与宏观影响\n来源不足时保持未知，不以行情代替新闻。\n\n"
            "## 信号\n证据不足时不强行判断正面、负面或中性。\n\n"
            f"## 来源覆盖\ncoverage_incomplete；证据数={evidence_count}；原因={reason}。"
        )
        summary = "新闻模型未正常结束，系统已保留有限来源结果并明确覆盖不足。"
    else:
        markdown = (
            "# 技术面分析\n\n"
            "## 价格趋势\n仅使用as_of窗口内的OHLCV判断。\n\n"
            "## 移动均线\n只陈述已返回均线，不补造缺失指标。\n\n"
            "## MACD与动量\n依据MACD与RSI结果，证据不足时保持未知。\n\n"
            "## 波动率\n依据布林带与ATR，不把技术信号当基本面事实。\n\n"
            "## 量价关系\n依据成交量及VWMA，缺失时不推断。\n\n"
            "## 指标选择依据\n仅说明实际返回指标的选择范围，不补造未请求指标。\n\n"
            f"## 数据质量与限制\n强制降级报告；证据数={evidence_count}；原因={reason}。\n\n"
            "| 观察日期 | 主要信号 | 证据 | 条件 | 风险与限制 |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| 未完成 | 未知 | 仅限已返回证据 | 等待完整工具结果 | 强制降级 |"
        )
        summary = "技术面模型未正常结束，系统已保留行情与指标结果并明确覆盖不足。"
    return SpecialistAssessment(
        role=role.value,
        summary=summary,
        report_markdown=markdown,
        coverage_status=coverage,
        limitations=limitations,
    )


def _upstream_incomplete(fundamental, news, market):
    return any(
        item is None or item.coverage_status != "complete" or bool(item.limitations)
        for item in (fundamental, news, market)
    )
