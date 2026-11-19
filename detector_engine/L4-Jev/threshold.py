"""L4 事件发布阈值筛选。"""

import math


def should_publish(result: dict, threshold: float) -> bool:
    """设立阈值判断 JEV 是否确认事件需要进一步诊断。"""
    return diagnosis_score(result) >= validate_threshold(threshold)


def diagnosis_score(result: dict) -> float:
    """读取 JEV 返回的事件级诊断分数。"""
    try:
        score = float(result["l4_result"]["answers"]["needs_diagnosis"]["noul"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("决策模型响应缺少有效的 l4_result.answers.needs_diagnosis.noul") from exc
    if not math.isfinite(score):
        raise ValueError("决策模型诊断分数必须是有限数值")
    return score


def validate_threshold(threshold: float) -> float:
    """校验 L4 事件级筛选阈值。"""
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("L4 发布阈值必须在 0 到 1 之间")
    return float(threshold)
