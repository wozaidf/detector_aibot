"""旧版模型包的反序列化兼容类定义。

已训练模型包引用 ``iforest_tool.MedianPreprocessor``。保留此模块名，
使 joblib 能够加载现有模型；此文件不提供训练或业务流程。
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


class MedianPreprocessor:
    """使用训练集保存的中位数填补缺失值，并检查模型输入列。"""

    def fit(self, frame: pd.DataFrame, columns: list[str]) -> "MedianPreprocessor":
        self.feature_columns = list(columns)
        clean = frame.reindex(columns=columns).replace([np.inf, -np.inf], np.nan)
        all_missing = [column for column in columns if clean[column].isna().all()]
        if all_missing:
            raise ValueError(f"训练集中整列缺失，拒绝静默填补: {all_missing}")
        self.medians = {column: float(clean[column].median()) for column in columns}
        self.training_missing_counts = {
            column: int(clean[column].isna().sum()) for column in columns
        }
        return self

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        missing = [column for column in self.feature_columns if column not in frame]
        extra = [column for column in frame if column not in self.feature_columns]
        if missing or extra:
            raise ValueError(f"特征列不匹配，缺失={missing}，多余={extra}")
        clean = frame[self.feature_columns].replace([np.inf, -np.inf], np.nan)
        return clean.fillna(pd.Series(self.medians)).astype(float)

    def manifest(self) -> dict[str, Any]:
        return {
            "feature_columns": self.feature_columns,
            "imputation": "仅使用 train.csv 每列中位数填补 null/NaN/正负无穷",
            "training_medians": self.medians,
            "training_missing_counts": self.training_missing_counts,
        }
