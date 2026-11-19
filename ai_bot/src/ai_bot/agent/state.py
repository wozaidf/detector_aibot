from typing import TypedDict, Annotated, Literal, NotRequired
from operator import add
from pydantic import StringConstraints

NonBlankString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class KGQuery(TypedDict):
    component_ids: list[NonBlankString]
    dataset: NotRequired[NonBlankString | None]


class AgentState(TypedDict, total=False):
    # 1. 输入阶段
    # 上游异常检测系统传入的标准化异常数据
    anomaly_data: dict

    # 2. 异常分析与规划阶段
    # 检索规划节点生成的 MCP 任务和 RAG 查询
    plan: list[dict]
    rag_queries: list[str]

    # 图谱查询任务及其结果，供检索规划节点使用
    kg_query: KGQuery
    kg_result: list[dict] # 知识图谱查询结果
    kg_status: Literal["skipped", "matched", "no_match", "error"]
    kg_error: str | None

    # 4. MCP/RAG执行阶段
    # 当前执行任务
    # current_task: dict
    # 工具调用结果
    tool_results: Annotated[list[dict], add]
    # RAG检索结果
    rag_results: Annotated[list[dict], add]

    # 5. 证据融合阶段+根因诊断阶段
    fused_evidence: dict
    # 生成的证据链
    evidence_chain: list[dict]
    # 根因分析结果
    diagnosis: dict
    # 置信度
    confidence: float

    # 6. 校验阶段
    validation_result: dict
    # 是否通过校验
    is_valid: bool

    # 7. 输出阶段
    final_response: dict

    # 8. 控制信息
    # errors: Annotated[list[str], add] # 错误信息列表
    retry_count: NotRequired[int] # 重试次数
    # node_status: dict[str, str]  # 当前状态：running / completed / failed
