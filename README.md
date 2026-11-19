# 智能运维异常检测与故障诊断 Agent

整合流式异常检测与基于知识图谱的 Agent 辅助诊断。

## 项目结构

- [`detector_engine/`](detector_engine/README.md)：Kafka、Flink CEP、Drools 异常检测，Isolation Forest 窗口评分，以及本地 Laya 诊断筛选。
- [`ai_bot/`](ai_bot/README.md)：LangGraph 诊断流程、Neo4j 知识图谱、RAG 历史案例检索与 MCP 工具调用。

## 启动

各子项目的依赖安装、环境变量和运行方式见其 README。检测链路在 `detector_engine/` 下使用 Docker Compose 启动。Agent 通过 FastAPI 提供诊断接口；检测链路到 Agent 的自动消费集成需按部署环境配置。

模型权重通过 Git LFS 保存，克隆后在仓库根目录执行 `git lfs pull`。API 密钥和本地环境配置不纳入版本管理，请参考 `.env.example` 配置。

部分演示数据为合成样本，部分 MCP 工具使用示例返回；本地评估结果不代表生产环境效果。

启动检测链路前，将 `detector_engine/.env.example` 复制为同目录的 `.env`，填写 PostgreSQL 与 pgAdmin 密码。Compose 会将数据库密码传给相关服务。
