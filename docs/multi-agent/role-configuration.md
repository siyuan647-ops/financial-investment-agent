# 五角色配置与 Prompt 版本

研究 Worker 从 `app.agents.role_registry.RoleRegistry` 读取 Fundamental、News、Market、
Investment、Risk 的职责摘要、Prompt 版本、模型用途和工具策略。前三个角色分别以自己的
`AgentRole` 调用授权工具；Investment 与 Risk 明确禁止工具调用。角色集合、工具策略和
Prompt 指纹在研究图构建时校验。

模型默认仍使用 `APP_QWEN_PROFILES.generate`，或 `APP_QWEN_MODEL`。可通过
`APP_SPECIALIST_MODEL_PROFILES` JSON 对单个角色覆盖完整模型配置。例如：

```json
{
  "market": {"model": "your-verified-market-model", "max_output_tokens": 4096, "temperature": 0.2},
  "risk": {"model": "your-verified-risk-model", "max_output_tokens": 8192, "temperature": 0}
}
```

未列出的角色继续使用 `generate` profile。每个角色还可配置成对的百万 Token 输入/输出价格和
币种；价格缺失时费用保持未知。逐角色 `max_output_tokens` 是模型响应上限，研究任务和单角色
Token/超时预算仍由 `APP_DEEP_RESEARCH_*` 设置独立控制。Investment 的结构化首选调用在
没有逐角色覆盖时仍使用原有 `temperature=0` 且不发送 profile 输出上限；有覆盖时应用覆盖的
模型和输出上限。Investment 的默认 `temperature=0` 同时适用于结构化调用及 Markdown 回退。

修改角色的 system prompt 后，运行：

```powershell
uv run --directory backend python -c "from app.agents.specialists import _current_prompt_digests; print({k.value:v for k,v in _current_prompt_digests().items()})"
```

把新摘要填入同一角色的 `prompt_digest`，并更新 `prompt_version` 的语义版本和末尾
12 位摘要。正文变化而版本或指纹未同步时，研究图初始化会拒绝启动；只更新指纹而保留
旧版本也会被拒绝。共享 Prompt 组件变化会影响多个角色，
须同步更新受影响的角色版本和指纹。

本轮首次加入摘要后，五个 Prompt 版本均追加了摘要后缀，因此部署后旧检查点会失配，
在任务重试时重新运行对应角色。

当前节点检查点仍仅按 Prompt 版本及输入校验，不按实际模型配置校验。切换角色模型时，
已有角色检查点可能被复用；需要强制重新生成旧任务时应同步提升相应 Prompt 版本，
或在后续独立迭代补上模型配置指纹。
