# 当前代码职责清单

日期：2026-11-21｜状态：本轮清理后待验收

当前运行链路：`Prefect DataOps → 已发布结构化数据 → Fundamental / News / Market → Investment → Risk → 研究报告`。旧 `docs/step-*` 是历史实施记录，不代表当前仍存在对应的运行功能。

| 目录或文件 | 当前职责 |
| --- | --- |
| `backend/app/dataops/` | 证券、行情、财务、公司行为、基本面域、技术指标的采集、归一化、质量和发布规则。 |
| `backend/app/workflows/` | Prefect 采集、补数、重扫、技术指标、质量与部署入口。 |
| `backend/app/repositories/` | 已发布数据、原始响应、任务、LLM 调用和研究检查点的数据库访问。 |
| `backend/app/providers/tushare.py`、`qwen.py` | 数据源和模型的外部 I/O。 |
| `backend/app/services/business_tool_dataops.py`、`business_tools.py` | 九个五角色业务工具的取数、覆盖状态、权限与输出契约。 |
| `backend/app/agents/specialists.py`、`direct_deep.py`、`role_registry.py` | 五角色执行、报告与风险终审。 |
| `backend/app/services/research.py`、`research_queue.py`、`backend/app/workers/research.py` | 研究任务提交、队列消费、取消和恢复。 |
| `backend/app/api/data.py`、`fundamental.py`、`analysis.py` | DataOps 查询与五角色研究 API。 |
| `backend/app/contracts/` | DataOps、工具、研究、模型和 API 的结构化契约。 |
| `backend/app/core/`、`observability/` | 配置、日志、健康检查和运行指标。 |
| `frontend/src/main.tsx` | 单一五角色研究工作台。 |
| `infra/` | 数据库、Redis、Prefect、API、研究 Worker 和运行指标的 Compose 部署。 |
| `backend/migrations/` | 既有数据库迁移历史；其中部分表属于已退役功能，但迁移不回滚，以保护已部署数据库。 |
| `backend/tests/`、`backend/evals/` | 当前链路的开发检查与样本；历史文档中的旧评测门槛不代表本轮已通过。 |

当前不再注册独立快问答、文档/向量知识库、图表、观察列表/告警和文本监控的 API 或 Prefect 部署。五角色 News 工具仍直接使用有界提供方接口；源错误与空窗口分别呈现。真实数据覆盖和研究结论仍须单独验证。
