"""跑通整张图：uv run python tests/test_graph.py"""

from ai_bot.agent.graph import graph


if __name__ == "__main__":

    initial_state = {
        "anomaly_data": {
            "event_id": "EVT_001",
            "device_id": "router-001",
            "severity": "high",
            "anomaly_type": "performance_degradation"
        },
        "retry_count": 0,
    }


    # recursion_limit 兜底：校验失败时 validation -> planner 这个环目前没有
    # 重试上限，不加这个限制会一路循环到默认的 10007 步
    config = {"recursion_limit": 20}

    result = graph.invoke(initial_state, config)

    print(result)