"""Isolation Forest 模型预测函数。

只负责对已准备好的特征数据执行已训练模型的预处理、异常评分和阈值判断；
不处理 Kafka 消息结构、文件或异常事件业务字段。
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def predict_iforest(
    features: pd.DataFrame,
    model: Any,
    preprocessor: Any,
    threshold: float,
) -> tuple[np.ndarray, np.ndarray]:
    """根据 IForest 模型为特征数据生成异常分数和判定标记。"""
    # 确保输入列及顺序符合训练时使用的特征清单。
    expected_columns = list(preprocessor.feature_columns)
    if list(features.columns) != expected_columns:
        raise ValueError("feature columns must match the fitted preprocessor order")

    # 应用训练阶段拟合好的缺失值处理。
    matrix = preprocessor.transform(features).to_numpy()
    scores = np.asarray(model.decision_function(matrix), dtype=float)
    if scores.ndim != 1 or len(scores) != len(features) or not np.isfinite(scores).all():
        raise ValueError("IForest returned an invalid score vector")

    # 用验证阶段确定的阈值生成异常标记。
    anomaly_flags = scores >= float(threshold)
    print(f"异常分数为: {scores}")
    print(f"异常标志为: {anomaly_flags}")
    return scores, anomaly_flags
