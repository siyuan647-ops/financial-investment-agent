"""Single registration point for the five research roles."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from app.contracts.business_tools import AgentRole
from app.contracts.llm import ModelPurpose
from app.contracts.research import SpecialistRole
from app.core.config import SpecialistModelProfile
from app.services.business_tools import BUSINESS_TOOL_DEFINITIONS


@dataclass(frozen=True)
class RoleDefinition:
    role: SpecialistRole
    responsibility: str
    prompt_version: str
    prompt_digest: str
    tool_role: AgentRole | None
    model_purpose: ModelPurpose = ModelPurpose.GENERATE
    default_temperature: float | None = None


ROLE_DEFINITIONS = MappingProxyType(
    {
        SpecialistRole.FUNDAMENTAL: RoleDefinition(
            SpecialistRole.FUNDAMENTAL,
            "公司档案、财务报表及基本面质量分析",
            "specialist-fundamental-v7-collaboration-skeleton.537a097ec115",
            "537a097ec115fb7e9e16b47a240a1386d9419976a996f1d156b028808d301872",
            AgentRole.FUNDAMENTAL,
        ),
        SpecialistRole.NEWS: RoleDefinition(
            SpecialistRole.NEWS,
            "公司新闻、公告及业绩事件分析",
            "specialist-news-v6-collaboration-skeleton.723c8fb44af3",
            "723c8fb44af329ffa45dd0d9137e951c2eb27e91ec0fc4bad4fc220f04a8cb3d",
            AgentRole.NEWS,
        ),
        SpecialistRole.MARKET: RoleDefinition(
            SpecialistRole.MARKET,
            "行情与技术指标分析",
            "specialist-market-v6-full-tool-windows.c420167fb89c",
            "c420167fb89c84ba2cc35bae7f7c5dcb172b279fa953e10a253955a71affbc2e",
            AgentRole.MARKET,
        ),
        SpecialistRole.INVESTMENT: RoleDefinition(
            SpecialistRole.INVESTMENT,
            "融合三份报告形成投资初评；禁止工具调用",
            "specialist-investment-v4-split-two-stage.7146240a08b3",
            "7146240a08b3a5d93c51fca61aa0bd8714060e4ecbf947f7952cb81b64f770e6",
            None,
            default_temperature=0,
        ),
        SpecialistRole.RISK: RoleDefinition(
            SpecialistRole.RISK,
            "终审投资初评和上游风险；禁止工具调用",
            "specialist-risk-v10-markdown-final-review.cfb3d10db709",
            "cfb3d10db709ac72bf59ccbb1bd833850cd0b3a7c1f08f124c5ff24c93195a89",
            None,
        ),
    }
)


class RoleRegistry:
    def __init__(
        self,
        model_profiles: Mapping[SpecialistRole | str, SpecialistModelProfile] | None = None,
    ):
        self.definitions = ROLE_DEFINITIONS
        if set(self.definitions) != set(SpecialistRole):
            raise ValueError("research_role_registration_incomplete")
        self.model_profiles = {
            SpecialistRole(role): SpecialistModelProfile.model_validate(profile)
            for role, profile in (model_profiles or {}).items()
        }
        if set(self.model_profiles) - set(self.definitions):
            raise ValueError("research_role_model_unknown")
        for role, definition in self.definitions.items():
            if not definition.prompt_version.endswith("." + definition.prompt_digest[:12]):
                raise ValueError(f"research_prompt_version_digest_mismatch:{role.value}")
            expected_tool_role = (
                AgentRole(role.value)
                if role in {SpecialistRole.FUNDAMENTAL, SpecialistRole.NEWS, SpecialistRole.MARKET}
                else None
            )
            if definition.tool_role != expected_tool_role:
                raise ValueError("research_role_tool_policy_inconsistent")
        if any(
            {AgentRole.INVESTMENT, AgentRole.RISK} & definition.allowed_agents
            for definition in BUSINESS_TOOL_DEFINITIONS.values()
        ):
            raise ValueError("research_synthesis_tool_policy_inconsistent")

    def get(self, role: SpecialistRole | str) -> RoleDefinition:
        return self.definitions[SpecialistRole(role)]

    def model_profile(self, role: SpecialistRole | str) -> SpecialistModelProfile | None:
        return self.model_profiles.get(SpecialistRole(role))

    def validate_prompts(self, actual_digests: Mapping[SpecialistRole, str]):
        if set(actual_digests) != set(self.definitions):
            raise ValueError("research_prompt_registration_incomplete")
        for role, definition in self.definitions.items():
            if actual_digests[role] != definition.prompt_digest:
                raise ValueError(f"research_prompt_version_not_updated:{role.value}")
