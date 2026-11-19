from datetime import datetime, timezone


def build_l4_event(source_event: dict, jev_result: dict) -> dict:
    # source_event: 上游事件，L2 或 L2.5 Kafka 消息
    # jev_result: JEV 返回的决策结果，包含异常判断、诊断建议等信息
    """将 JEV 决策结果添加到 L4 阶段事件。"""

    return {
        **source_event,
        "stage": "L4",
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "l4_result": jev_result,
    }
