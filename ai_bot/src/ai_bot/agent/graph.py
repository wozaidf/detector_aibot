from langgraph.graph import StateGraph, START, END
from ai_bot.agent.kg_node import knowledge_graph_node
from ai_bot.agent.state import AgentState
from ai_bot.agent.nodes import (
    analysis_and_planning_node,
    evidence_fusion_node,
    rag_retrieval_node,
    retrieval_planning_node,
    tool_execution_node,
    diagnosis_node,
    validation_node,
    response_node,
)

MAX_RETRY = 2


# validation 后的条件路由
def validation_router(state: AgentState) -> str:
    """
    校验通过 -> response
    校验失败 -> retrieval_planning，重新规划补充证据
    """
    # 校验通过或重试次数达到上限时，进入输出节点。
    if state.get("is_valid", False) or state.get("retry_count", 0) >= MAX_RETRY:
        return "response"
    return "retrieval_planning"


def create_graph():
    # 1. 创建图
    builder = StateGraph(AgentState)

    # 2. 注册节点
    builder.add_node(
        "analysis_and_planning",
        analysis_and_planning_node
    )

    builder.add_node("knowledge_graph", knowledge_graph_node)
    builder.add_node("retrieval_planning", retrieval_planning_node)
    builder.add_node("rag_retrieval", rag_retrieval_node)
    builder.add_node("evidence_fusion", evidence_fusion_node)

    builder.add_node(
        "tool_execution",
        tool_execution_node
    )

    builder.add_node(
        "diagnosis",
        diagnosis_node
    )

    builder.add_node(
        "validation",
        validation_node
    )

    builder.add_node(
        "response",
        response_node
    )

    # 3. 普通流程
    builder.add_edge(
        START,
        "analysis_and_planning"
    )

    builder.add_edge("analysis_and_planning", "knowledge_graph")
    builder.add_edge("knowledge_graph", "retrieval_planning")
    builder.add_edge("retrieval_planning", "rag_retrieval")
    builder.add_edge("retrieval_planning", "tool_execution")
    builder.add_edge(["rag_retrieval", "tool_execution"], "evidence_fusion")
    builder.add_edge("evidence_fusion", "diagnosis")
    builder.add_edge(
        "diagnosis",
        "validation"
    )

    # 4. validation 条件路由
    builder.add_conditional_edges(
        "validation",
        validation_router,
        {
            "response": "response",
            "retrieval_planning": "retrieval_planning",
        }
    )

    # 5. 最终输出
    builder.add_edge(
        "response",
        END
    )

    # 6. 编译 Graph
    return builder.compile()


graph = create_graph()
