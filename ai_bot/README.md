# AI-Bot

基于 **LangChain + LangGraph** 的基础 Agent（ReAct 模式），默认使用 Claude（`claude-opus-4-8`）。

## 项目结构

```
AI-Bot/
├── pyproject.toml            # 依赖管理（uv）
├── langgraph.json            # LangGraph 开发服务器配置（langgraph dev）
├── .env                      # API Key 等配置（不提交 git）
├── .env.example              # .env 模板
├── main.py                   # 根入口
├── src/ai_bot/
│   ├── config.py             # 配置加载（pydantic-settings 读 .env）
│   ├── llm.py                # 模型初始化（换模型只改这里）
│   ├── state.py              # AgentState 状态定义
│   ├── nodes.py              # 图节点函数
│   ├── graph.py              # LangGraph 组装（核心入口）
│   ├── prompts.py            # 系统提示词
│   ├── cli.py                # 命令行交互循环
│   └── tools/
│       ├── __init__.py       # 工具注册中心（all_tools）
│       ├── calculator.py     # 计算器（AST 安全求值）
│       └── search.py         # 搜索（占位实现，待接入）
└── tests/
    └── test_agent.py         # 冒烟测试（不消耗 API）
```

## 快速开始

```bash
# 1. 安装依赖
uv sync

# 2. 配置 API Key：编辑 .env，填入 ANTHROPIC_API_KEY

# 3. 启动命令行 Agent
uv run main.py        # 或 uv run ai-bot
```

试一句：`(123 * 456) + 789 等于多少？` —— 模型会调用 calculator 工具再回答。

## 工作原理

```
START --> agent --(模型要求调工具)--> tools --> agent --> ...
            |
            +--(模型直接回答)--> END
```

- `agent` 节点调用绑定了工具的 LLM；
- `tools_condition` 判断回复中是否带 `tool_calls`，有则路由到 `tools` 节点（`ToolNode` 自动执行）；
- 工具结果作为消息回到 `agent`，循环直到模型给出最终回答；
- `MemorySaver` checkpointer + `thread_id` 提供多轮对话记忆。

## 如何扩展

| 想做的事 | 改哪里 |
| --- | --- |
| 新增工具 | `src/ai_bot/tools/` 下新建文件，用 `@tool` 装饰，注册进 `tools/__init__.py` 的 `all_tools` |
| 换模型 | `.env` 里的 `MODEL_NAME`（如 `claude-sonnet-5`、`claude-haiku-4-5`） |
| 改人设/规则 | `prompts.py` 的 `SYSTEM_PROMPT` |
| 状态加字段 | `state.py` 的 `AgentState` |
| 接入真实搜索 | `tools/search.py`（推荐 `ddgs` 或 `langchain-tavily`） |
| 对外提供 HTTP 服务 | 新建 FastAPI 应用，引用 `graph.py` 的 `graph`，用 `graph.astream()` 流式输出 |
| 可视化调试 | `uv run langgraph dev`（LangGraph Studio） |

## L5 RAG 知识库

`src/ai_bot/rag/` 与 `src/ai_bot/knowledge/`（知识图谱）并列，把 AIOps Challenge 2020 的
历史故障数据做成可检索、可问答的知识库。

| 文件 | 作用 |
| --- | --- |
| `aiops_rag.py` | 语料生成：故障表 + 原始指标 → Markdown 语料 |
| `rag_qa.py` | 向量入库、检索、三道防答非所问闸门、问答 |
| `rag_evaluation.py` | 离线检索评估（10 个标注问题，不调用 LLM） |

```bash
# 语料（src/ai_bot/rag/corpus/）与向量库（.rag_runtime/）均不入库，首次需先按原始数据生成
set AIOPS_DATA_ROOT=E:\company\data
python -m ai_bot.rag.aiops_rag

python -m ai_bot.rag.rag_qa            # 检索问答（需在仓库根 .env 配 DEEPSEEK_API_KEY）
python -m ai_bot.rag.rag_evaluation    # 只跑检索评估（不耗 LLM 额度）
```

检索配置：similarity + K=2，BGE 中文向量（512 维，本地 onnx 推理），相关性阈值 0.95
（平方 L2；节点锁定时放宽到 1.1875）。离线评估 HitRate 1.00 / MRR 0.85。

## 测试

```bash
uv run pytest
```

测试只覆盖工具逻辑与图结构，不调用模型 API，不消耗额度。
