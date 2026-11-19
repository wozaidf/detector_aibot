from jev.jev_api import handle_exception_decision


# 定义 jev_decide 函数，用于处理异常决策逻辑
def jev_decide(state):
    # enumerate函数用于将一个可遍历的数据对象(如列表、元组或字符串)组合为一个索引序列，同时列出数据和数据下标，一般用在 for 循环当中。
    jev = {
        "state": state,
        "questions": {
            "needs_diagnosis": {
                "type": "noul",
                "instructions": "根据异常指标、检测证据和上游处理状态，判断该事件是否是异常事件，是否需要进一步诊断和排查。",
            },
        }
    }
    response = handle_exception_decision(jev)
    return response
