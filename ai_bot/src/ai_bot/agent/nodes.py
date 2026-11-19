"""图节点函数：每个节点接收 State，返回对 State 的更新。"""
import asyncio
import json

from jsonschema.exceptions import SchemaError, ValidationError as JSONSchemaValidationError
from jsonschema import validate as validate_json_schema
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import BaseTool
from pydantic import ValidationError
from ai_bot.llm.llm import llm
from ai_bot.agent.prompts import (
    ANALYSIS_AND_PLANNING_PROMPT,
    DIAGNOSIS_PROMPT,
    RETRIEVAL_PLANNING_PROMPT,
    SYSTEM_PROMPT,
    TOOL_DESCRIPTIONS_PROMPT,
    VALIDATION_PROMPT,
)
from ai_bot.agent.state import AgentState
from ai_bot.mcp.client import get_mcp_tools
from ai_bot.schemas import AnalysisPlan, DiagnosisEvidence, RetrievalPlan, ToolTask


async def analysis_and_planning_node(state: AgentState) -> dict:
    print("1、agent进入异常分析和规划阶段")
    """用一次模型调用理解异常并生成图谱查询参数。"""
    prompt = ANALYSIS_AND_PLANNING_PROMPT.substitute(
        anomaly_data=json.dumps(state["anomaly_data"], ensure_ascii=False),
    )
    response = await llm.ainvoke([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=prompt),
    ])
    return AnalysisPlan.model_validate_json(response.content).model_dump()



async def retrieval_planning_node(state: AgentState) -> dict:
    """根据异常和图谱结果生成 RAG 与 MCP 任务。"""
    tool_map = await get_mcp_tools()
    tool_descriptions = [
        {"name": tool.name, "description": tool.description, "parameters": tool.args}
        for tool in tool_map.values()
    ]

    retry_context = {}
    if state.get("retry_count", 0):
        retry_context = {
            "previous_plan": state.get("plan", []),
            "previous_rag_queries": state.get("rag_queries", []),
            "validation_result": state.get("validation_result", {}),
        }
    prompt = RETRIEVAL_PLANNING_PROMPT.substitute(
        anomaly_data=json.dumps(state["anomaly_data"], ensure_ascii=False),
        kg_evidence=json.dumps({
            "status": state.get("kg_status"),
            "context": state.get("kg_context", []),
            "error": state.get("kg_error"),
        }, ensure_ascii=False, default=str),
        retry_context=json.dumps(retry_context, ensure_ascii=False),
    )
    tool_prompt = TOOL_DESCRIPTIONS_PROMPT.substitute(
        tool_descriptions=json.dumps(tool_descriptions, ensure_ascii=False),
    )
    response = await llm.ainvoke([
        SystemMessage(content=SYSTEM_PROMPT),
        SystemMessage(content=tool_prompt),
        HumanMessage(content=prompt),
    ])
    return RetrievalPlan.model_validate_json(response.content).model_dump()


def validate_tool_arguments(tool: BaseTool, arguments: dict) -> None:
    """按工具声明的 schema 校验 MCP 或本地工具参数。"""
    schema = tool.args_schema
    if isinstance(schema, dict):
        validate_json_schema(arguments, schema)
    else:
        tool.get_input_schema().model_validate(arguments)


# 执行节点：接收规划结果，调用工具执行查询任务，返回对 State 的更新。
async def tool_execution_node(state: AgentState) -> dict:
    print("3、agent进入rag查询和工具执行阶段")
    tool_map = await get_mcp_tools()
    plan = state.get("plan", [])

    async def execute_task(raw_task):
        task_fields = raw_task if isinstance(raw_task, dict) else {}
        result_info = {
            "task_id": task_fields.get("task_id"),
            "task": task_fields.get("task"),
            "tool": task_fields.get("tool"),
            "arguments": task_fields.get("arguments", {}),
            "success": False,
            "result": None,
            "error": None
        }

        try:
            task = ToolTask.model_validate(raw_task)
        except ValidationError as e:
            result_info["error"] = f"Tool任务校验失败: {e}"
            return result_info

        tool = tool_map.get(task.tool)
        if tool is None:
            result_info["error"] = f"工具不存在: {task.tool}"
            return result_info

        try:
            validate_tool_arguments(tool, task.arguments)
        except (JSONSchemaValidationError, SchemaError, ValidationError) as e:
            result_info["error"] = f"Tool参数校验失败: {e}"
            return result_info

        try:
            result = await tool.ainvoke(task.arguments)
            is_error = getattr(result, "status", None) == "error"
            result_info["success"] = not is_error
            result_info["result"] = None if is_error else result
            result_info["error"] = str(result) if is_error else None

        except Exception as e:
            result_info["error"] = str(e)

        return result_info

    # 4. 并发执行工具
    tool_results = await asyncio.gather(
        *(execute_task(task) for task in plan)
    )
    return {
        "tool_results": tool_results
    }


def rag_retrieval_node(state: AgentState) -> dict:
    """检索历史案例，并保留原文及来源供诊断引用。"""
    queries = state.get("rag_queries", [])
    if not queries:
        return {"rag_results": []}

    from ai_bot.rag.rag_qa import get_vectorstore, retrieve_scored

    vectorstore = get_vectorstore()
    return {"rag_results": [
        {
            "query": query,
            "content": document.page_content,
            "source": document.metadata,
            "distance": distance,
        }
        for query in queries
        for document, distance in retrieve_scored(vectorstore, query, doc_type="incident")
    ]}


def evidence_fusion_node(state: AgentState) -> dict:
    """汇集图谱、历史案例和实时查询证据，保留各自的来源。"""
    return {"fused_evidence": {
        "knowledge_graph": {
            "status": state.get("kg_status"),
            "context": state.get("kg_context", []),
            "error": state.get("kg_error"),
        },
        "historical_cases": state.get("rag_results", []),
        "live_queries": state.get("tool_results", []),
    }}

# 诊断节点：接收工具执行结果，综合分析根因，返回对 State 的更新。
def diagnosis_node(state: AgentState) -> dict:
    print("3")
    prompt_text = DIAGNOSIS_PROMPT.substitute(
        anomaly_data=json.dumps(state["anomaly_data"], ensure_ascii=False, indent=2),
        fused_evidence=json.dumps(
            state.get("fused_evidence", {"live_queries": state.get("tool_results", [])}),
            ensure_ascii=False, indent=2, default=str,
        ),
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt_text)]

    try:
        response = llm.invoke(messages)
        response_json = json.loads(response.content)
        diagnosis = response_json.get("diagnosis", {})
        # 兜底值必须是 []，不能用 {}：state.py:28 声明的是 list[dict]，
        # 给 dict 会让「模型漏输出」和「正常输出」在类型上无法区分
        evidence_chain = response_json.get("evidence_chain", [])
        confidence = response_json.get("confidence", None)
        return {
            "diagnosis": diagnosis,
            "evidence_chain": evidence_chain,
            "confidence": confidence
        }
    except Exception as e:
        return {
            "errors": [
                f"diagnosis node failed: {e}"
            ]
        }


# 校验节点：接收诊断结果，进行结果校验，返回对 State 的更新。
def validation_node(state: AgentState) -> dict:
    print("4")
    tool_results = state.get("tool_results", [])
    anomaly_data = state.get("anomaly_data", {})

    issues = []

    try:
        evidence = DiagnosisEvidence.model_validate(state)
    except ValidationError as e:
        issues.extend(
            f"{'.'.join(map(str, error['loc']))}: {error['msg']}"
            for error in e.errors(include_input=False)
        )

    if not (state.get("kg_context") or state.get("rag_results") or
            any(item.get("success") is True for item in tool_results)):
        issues.append("图谱、历史案例和实时查询均未提供补充证据")

    # 基础检查已经失败
    if issues:
        return {
            "validation_result": {
                "passed": False,
                "issues": issues,
                "needs_retry": True,
                "retry_target": "retrieval_planning",
            },
            "is_valid": False,
            "retry_count": state.get("retry_count", 0) + 1
        }
    prompt_text = VALIDATION_PROMPT.substitute(
        anomaly_data=json.dumps(anomaly_data, ensure_ascii=False, indent=2),
        fused_evidence=json.dumps(
            state.get("fused_evidence", {"live_queries": tool_results}),
            ensure_ascii=False, indent=2, default=str,
        ),
        diagnosis=json.dumps(evidence.diagnosis, ensure_ascii=False, indent=2),
        evidence_chain=json.dumps(evidence.evidence_chain, ensure_ascii=False, indent=2),
        confidence=evidence.confidence
    )
    try:
        response = llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=prompt_text)
        ])

        response_json = json.loads(response.content)
        is_valid = (
                response_json.get("validation_result", {}).get("evidence_supported", False)
                and response_json.get("validation_result", {}).get("diagnosis_consistent", False)
                and response_json.get("validation_result", {}).get("confidence_reasonable", False)
        )
        return {
            "validation_result": response_json.get("validation_result", {}),
            "is_valid": is_valid,
            "retry_count": state.get("retry_count", 0) + 1
        }

    except Exception as e:
        return {
            "errors": [
                f"validation node failed: {e}"
            ],
            "retry_count": state.get("retry_count", 0) + 1
        }


# 输出节点：接收校验结果，生成最终报告，返回对 State 的更新。
def response_node(state: AgentState) -> dict:
    print("5")
    """
    输出节点：
    将经过校验的诊断结果整理为最终结构化输出。
    """

    anomaly_data = state.get("anomaly_data", {})
    diagnosis = state.get("diagnosis", {})
    evidence_chain = state.get("evidence_chain", [])
    confidence = state.get("confidence")
    validation_result = state.get("validation_result", {})
    is_valid = state.get("is_valid", False)

    final_response = {
        "event_id": anomaly_data.get("event_id"),
        "device_id": anomaly_data.get("device_id"),
        "status": "success" if is_valid else "validation_failed",

        "diagnosis": diagnosis,

        "evidence_chain": evidence_chain,

        "confidence": confidence,

        "validation": {
            "is_valid": is_valid,
            "issues": validation_result.get("issues", []),
            "evidence_supported": validation_result.get(
                "evidence_supported", False
            ),
            "diagnosis_consistent": validation_result.get(
                "diagnosis_consistent", False
            ),
            "confidence_reasonable": validation_result.get(
                "confidence_reasonable", False
            )
        }
    }

    return {
        "final_response": final_response
    }
