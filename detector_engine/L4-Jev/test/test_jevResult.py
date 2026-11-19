from jev.jev_decide import jev_decide
from kafka_stream.JsonToState import build_jev_state
from kafka_stream.JevResultToEvent import build_l4_event

import json

with open("l2_out.json", "r") as f:
    data = json.load(f)

def test_out_jev_result():
    # 1、 用上游数据转化为jev的上下文状态
    state = build_jev_state(data)

    # 2、 使用jev处理消息，得到决策结果
    jev_result = jev_decide(state)
    if not isinstance(jev_result, dict):
        raise ValueError("JEV 没有返回有效 JSON 对象")

    # 4、 输出结果
    print(f"处理结果: {jev_result}")


if __name__ == "__main__":
    test_out_jev_result()