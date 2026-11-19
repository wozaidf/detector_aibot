from typing import Any

from pydantic import BaseModel, ConfigDict, Field


from ai_bot.agent.state import KGQuery, NonBlankString


class ToolTask(BaseModel):
    """Planner 输出的一项工具调用。"""

    model_config = ConfigDict(strict=True)

    task_id: str | None = None
    task: str | None = None
    tool: str = Field(min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)


class DiagnosisEvidence(BaseModel):
    """提交给诊断校验节点的结果和证据。"""

    model_config = ConfigDict(strict=True)

    diagnosis: dict = Field(min_length=1)
    evidence_chain: list[dict] = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)

class AnalysisPlan(BaseModel):
    """首节点输出的图谱查询参数。"""
    model_config = ConfigDict(strict=True, extra="forbid")
    kg_query: KGQuery


class RetrievalPlan(BaseModel):
    """根据图谱上下文生成的检索计划。"""
    model_config = ConfigDict(strict=True, extra="forbid")
    plan: list[ToolTask]
    rag_queries: list[NonBlankString]
