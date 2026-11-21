"""Registration and policy boundary for bounded specialist business tools."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from app.contracts.business_tools import (
    BUSINESS_TOOL_CALL,
    BUSINESS_TOOL_CALL_MODELS,
    AgentRole,
    BusinessToolCall,
    BusinessToolGap,
    BusinessToolName,
    BusinessToolResult,
    BusinessToolStatus,
    ToolCapability,
    ToolCoverage,
)
from app.contracts.common import SHANGHAI

BusinessToolHandler = Callable[[BusinessToolCall], Awaitable[BusinessToolResult]]


@dataclass(frozen=True)
class BusinessToolDefinition:
    name: BusinessToolName
    version: str
    description: str
    allowed_agents: frozenset[AgentRole]
    required_domains: tuple[str, ...]
    capability: ToolCapability
    max_records: int


BUSINESS_TOOL_DEFINITIONS = {
    BusinessToolName.GET_STOCK_DATA: BusinessToolDefinition(
        BusinessToolName.GET_STOCK_DATA,
        "1.0.0",
        "Latest 60 published OHLCV daily bars",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.MARKET}),
        ("price_bar",),
        ToolCapability.AVAILABLE,
        60,
    ),
    BusinessToolName.GET_INDICATORS: BusinessToolDefinition(
        BusinessToolName.GET_INDICATORS,
        "1.0.0",
        "Latest 30 complete technical indicator dates",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.MARKET}),
        ("technical_indicator",),
        ToolCapability.AVAILABLE,
        30,
    ),
    BusinessToolName.GET_STOCK_NEWS: BusinessToolDefinition(
        BusinessToolName.GET_STOCK_NEWS,
        "1.0.0",
        "Company-matched news in the seven-day window",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.NEWS}),
        ("news",),
        ToolCapability.COVERAGE_DEPENDENT,
        10,
    ),
    BusinessToolName.GET_ANNOUNCEMENTS: BusinessToolDefinition(
        BusinessToolName.GET_ANNOUNCEMENTS,
        "1.0.0",
        "Company announcements in the seven-day window",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.NEWS}),
        ("announcements",),
        ToolCapability.COVERAGE_DEPENDENT,
        10,
    ),
    BusinessToolName.GET_EARNINGS_EVENTS: BusinessToolDefinition(
        BusinessToolName.GET_EARNINGS_EVENTS,
        "1.0.0",
        "Forecast and express events in the seven-day window",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.NEWS}),
        ("earnings_events",),
        ToolCapability.COVERAGE_DEPENDENT,
        10,
    ),
    BusinessToolName.GET_COMPANY_PROFILE: BusinessToolDefinition(
        BusinessToolName.GET_COMPANY_PROFILE,
        "1.0.0",
        "Published company profile and key metrics",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.FUNDAMENTAL}),
        ("company_profile",),
        ToolCapability.AVAILABLE,
        200,
    ),
    BusinessToolName.GET_PROFITABILITY: BusinessToolDefinition(
        BusinessToolName.GET_PROFITABILITY,
        "1.0.0",
        "Published annual and quarterly profitability",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.FUNDAMENTAL}),
        ("profitability",),
        ToolCapability.AVAILABLE,
        12,
    ),
    BusinessToolName.GET_BALANCE_SHEET: BusinessToolDefinition(
        BusinessToolName.GET_BALANCE_SHEET,
        "1.0.0",
        "Published recent balance sheets",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.FUNDAMENTAL}),
        ("balance_sheet",),
        ToolCapability.AVAILABLE,
        4,
    ),
    BusinessToolName.GET_CASH_FLOW: BusinessToolDefinition(
        BusinessToolName.GET_CASH_FLOW,
        "1.0.0",
        "Published recent cash flows",
        frozenset({AgentRole.ORCHESTRATOR, AgentRole.FUNDAMENTAL}),
        ("cash_flow",),
        ToolCapability.AVAILABLE,
        4,
    ),
}


class BusinessToolRegistry:
    """Expose only registered tools and enforce specialist-agent permissions."""

    def __init__(
        self,
        handlers: Mapping[BusinessToolName | str, BusinessToolHandler] | None = None,
        *,
        enabled_tools: frozenset[BusinessToolName] | None = None,
    ):
        self._handlers = {
            BusinessToolName(name): handler for name, handler in (handlers or {}).items()
        }
        self._enabled_tools = enabled_tools

    @property
    def names(self):
        return tuple(BUSINESS_TOOL_DEFINITIONS)

    def definitions_for(self, role: AgentRole):
        return tuple(
            definition
            for definition in BUSINESS_TOOL_DEFINITIONS.values()
            if role in definition.allowed_agents
            and (self._enabled_tools is None or definition.name in self._enabled_tools)
        )

    def input_schema(self, name: BusinessToolName | str):
        selected = BusinessToolName(name)
        return BUSINESS_TOOL_CALL_MODELS[selected].model_json_schema()

    def argument_schema(self, name: BusinessToolName | str):
        selected = BusinessToolName(name)
        argument_type = BUSINESS_TOOL_CALL_MODELS[selected].model_fields["arguments"].annotation
        return argument_type.model_json_schema()

    def metadata_for(self, role: AgentRole):
        """Return model-consumable registration metadata without executable handlers."""
        return tuple(
            {
                "name": definition.name.value,
                "version": definition.version,
                "description": definition.description,
                "required_domains": definition.required_domains,
                "capability": definition.capability.value,
                "max_records": definition.max_records,
                "input_schema": self.input_schema(definition.name),
                "argument_schema": self.argument_schema(definition.name),
            }
            for definition in self.definitions_for(role)
        )

    async def execute(self, value: BusinessToolCall | dict, role: AgentRole) -> BusinessToolResult:
        call = BUSINESS_TOOL_CALL.validate_python(value)
        name = BusinessToolName(call.name)
        definition = BUSINESS_TOOL_DEFINITIONS[name]
        if role not in definition.allowed_agents:
            return _unavailable(
                call,
                BusinessToolStatus.PERMISSION_DENIED,
                "agent_role_not_allowed_for_tool",
            )
        handler = self._handlers.get(name)
        if handler is None:
            return _unavailable(
                call,
                BusinessToolStatus.SOURCE_UNAVAILABLE,
                "tool_backend_not_configured",
            )
        result = await handler(call)
        if result.tool != name or result.request_id != call.request_id:
            raise ValueError("tool_handler_response_identity_mismatch")
        if len(result.items) > definition.max_records:
            raise ValueError("tool_handler_exceeded_record_limit")
        return result


def _unavailable(call, status: BusinessToolStatus, reason: str):
    as_of = call.arguments.as_of
    if as_of is None:
        as_of = datetime.now(SHANGHAI)
    return BusinessToolResult(
        request_id=call.request_id,
        tool=call.name,
        status=status,
        as_of=as_of,
        coverage=ToolCoverage(complete=False, note=reason),
        gaps=(BusinessToolGap(domain=call.name, status=status, reason=reason),),
    )
