"""FastAPI 接口：把异常诊断图包装成 HTTP 服务。

启动：
    uv run uvicorn ai_bot.api:app --reload --port 8000

调用：
    curl -X POST http://127.0.0.1:8000/diagnose \
         -H "Content-Type: application/json" \
         -d '{"anomaly_data": {"event_id": "EVT_001", "device_id": "router-001",
                               "severity": "high", "anomaly_type": "performance_degradation"}}'
"""

import asyncio
import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from langgraph.errors import GraphRecursionError
from pydantic import BaseModel, Field

from ai_bot.agent.graph import graph

# 没有配置 logging 时 INFO 不会输出；容器里跑记得 basicConfig 或让 uvicorn 转发
logger = logging.getLogger(__name__)

app = FastAPI(title="Network Anomaly Diagnosis Agent", version="1.0.0")

# 图步数上限。validation -> planner 的重试环目前没有自带上限
# （validation_node 的基础检查分支不写 retry_count，validation_router 也不看
# retry_count），不加这个限制会一路循环到 langgraph 默认的 10007 步 ——
# 真模型下就是几千次 API 调用、单个请求挂死几十分钟。
RECURSION_LIMIT = 25

# 单次请求的墙钟上限（秒）
REQUEST_TIMEOUT = 300


class DiagnoseRequest(BaseModel):
    anomaly_data: dict = Field(
        ...,
        description="上游异常检测系统下发的标准化异常数据",
        examples=[
            {
                "event_id": "EVT_001",
                "device_id": "router-001",
                "severity": "high",
                "anomaly_type": "performance_degradation",
            }
        ],
    )


class DiagnoseResponse(BaseModel):
    """直接复用 response 节点产出的 final_response。"""

    event_id: str | None = None
    device_id: str | None = None
    status: str
    diagnosis: dict
    # 不写死 list[dict]：evidence_chain 是模型原始输出原样透传，prompt 要求数组
    # 但模型偶尔返回对象，而 validation_node 只做真值检查拦不住，写死会判成 500
    evidence_chain: Any
    confidence: float | None = None
    validation: dict


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/diagnose", response_model=DiagnoseResponse)
async def diagnose(anomaly_request: DiagnoseRequest) -> dict:
    # 计算耗时
    start_time = time.perf_counter()

    """跑一遍完整诊断图，返回结构化诊断结果。"""
    initial_state = {
        "anomaly_data": anomaly_request.anomaly_data,
        "retry_count": 0,
        # 不要传 errors：它不在 AgentState 里（state.py 里还是注释状态），
        # langgraph 会静默丢弃，传了只会让人误以为节点异常被记录下来了
    }
    config = {"recursion_limit": RECURSION_LIMIT}

    try:
        result = await asyncio.wait_for(
            graph.ainvoke(initial_state, config), timeout=REQUEST_TIMEOUT
        )
    except GraphRecursionError as exc:
        # 撞到步数上限：校验反复不通过，重试环没收敛
        logger.warning("诊断未收敛，event_id=%s", anomaly_request.anomaly_data.get("event_id"))
        raise HTTPException(
            status_code=504,
            detail=f"诊断在 {RECURSION_LIMIT} 步内未收敛，请检查 anomaly_data 是否完整",
        ) from exc
    except asyncio.TimeoutError as exc:
        logger.warning("诊断超时，event_id=%s", anomaly_request.anomaly_data.get("event_id"))
        raise HTTPException(
            status_code=504, detail=f"诊断超过 {REQUEST_TIMEOUT} 秒未完成"
        ) from exc
    except KeyError as exc:
        # 上游节点 JSON 解析失败时返回的 errors 被 langgraph 丢弃，
        # 图带着缺失的 state 继续走，下游取 state["xxx"] 于是 KeyError
        logger.exception("图执行中断，缺失字段 %s", exc)
        raise HTTPException(
            status_code=502, detail=f"诊断流程中断：上游节点未产出 {exc}"
        ) from exc


    # 计算耗时
    elapsed_time = time.perf_counter() - start_time
    print(f"Graph 执行耗时：{elapsed_time:.3f} 秒")

    # 检查最终结果是否存在
    final_response = result.get("final_response")
    if not final_response:
        raise HTTPException(status_code=502, detail="诊断流程结束但没有产出最终结果")

    logger.info(
        "诊断完成 event_id=%s status=%s confidence=%s",
        final_response.get("event_id"),
        final_response.get("status"),
        final_response.get("confidence"),
    )
    return final_response
