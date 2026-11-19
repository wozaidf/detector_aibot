"""执行 Planner 给出的组件图谱查询。"""

import asyncio
import logging

from neo4j.exceptions import DriverError, Neo4jError
from pydantic import TypeAdapter, ValidationError

from ai_bot.agent.state import AgentState, KGQuery
from ai_bot.knowledge.kg_query import get_kg_context

logger = logging.getLogger(__name__)
kg_query_adapter = TypeAdapter(KGQuery | None)


async def knowledge_graph_node(state: AgentState) -> AgentState:
    print("2、agent进入知识图谱查询阶段")
    """执行 kg_query，将图谱上下文和查询状态交给检索规划器。"""
    try:
        task = kg_query_adapter.validate_python(state.get("kg_query"), strict=True)
    except ValidationError as exc:
        error = exc.errors(include_input=False)[0]
        field = ".".join(map(str, ("kg_query", *error["loc"])))
        return {"kg_context": [], "kg_status": "error", "kg_error": f"{field}: {error['msg']}"}

    if task is None or not task["component_ids"]:
        return {"kg_context": [], "kg_status": "skipped", "kg_error": None}

    component_ids = list(dict.fromkeys(task["component_ids"]))
    try:
        context = await asyncio.to_thread(
            get_kg_context, component_ids, dataset=task.get("dataset"),
        )
    except SystemExit:
        # 现有驱动在缺少 NEO4J_PASSWORD 时会抛出 SystemExit。
        return {"kg_context": [], "kg_status": "error", "kg_error": "Neo4j 配置缺少 NEO4J_PASSWORD。"}
    except (DriverError, Neo4jError) as exc:
        logger.warning("知识图谱查询失败: %s", type(exc).__name__)
        return {
            "kg_context": [],
            "kg_status": "error",
            "kg_error": f"Neo4j 查询失败（{type(exc).__name__}）。",
        }

    return {
        "kg_context": context,
        "kg_status": "matched" if context else "no_match",
        "kg_error": None,
    }
