"""Train and evaluate PyOD IForest using the synthetic CSV train/val/test splits."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import time
from pathlib import Path
from typing import Any

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyod
import sklearn
from pyod.models.iforest import IForest
from sklearn.metrics import (average_precision_score, confusion_matrix,
                             f1_score, precision_score, recall_score,
                             precision_recall_curve, roc_auc_score)

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_DATASETS = PROJECT_DIR / "iforest-dataset" / "datasets"
DEFAULT_OUTPUT = PROJECT_DIR / "artifacts" / "模型验证与测试结果"
METADATA_COLUMNS = ["window_id", "entity_id", "window_end", "label"]


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_splits(dataset_dir: Path) -> tuple[dict[str, pd.DataFrame], list[str], dict[str, Any]]:
    """Load and validate fixed CSV splits; metadata and labels never enter X."""
    frames: dict[str, pd.DataFrame] = {}
    report: dict[str, Any] = {"dataset_dir": str(dataset_dir.resolve()), "splits": {}}
    for name in ("train", "val", "test"):
        path = dataset_dir / f"{name}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"缺少数据文件: {path}")
        frame = pd.read_csv(path, encoding="utf-8-sig")
        if frame.empty:
            raise ValueError(f"数据文件为空: {path}")
        if list(frame.columns).count("label") != 1 or any(c not in frame for c in METADATA_COLUMNS):
            raise ValueError(f"{name}.csv 必须包含元数据列: {METADATA_COLUMNS}")
        frames[name] = frame
    base_columns = list(frames["train"].columns)
    feature_columns = [c for c in base_columns if c not in METADATA_COLUMNS]
    if len(feature_columns) != 23:
        raise ValueError(f"预期 23 个数值特征列，实际 {len(feature_columns)}")
    for name, frame in frames.items():
        if list(frame.columns) != base_columns:
            raise ValueError(f"{name}.csv 的列名或顺序与 train.csv 不一致")
        if frame[METADATA_COLUMNS].isna().any().any():
            raise ValueError(f"{name}.csv 的元数据存在空值")
        if frame["window_id"].astype(str).duplicated().any():
            raise ValueError(f"{name}.csv 中 window_id 不唯一")
        labels = pd.to_numeric(frame["label"], errors="coerce")
        if labels.isna().any() or not set(labels.unique()).issubset({0, 1}):
            raise ValueError(f"{name}.csv 的 label 必须是 0/1")
        frame["label"] = labels.astype(int)
        for column in feature_columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        values = frame[feature_columns].replace([np.inf, -np.inf], np.nan)
        frame.loc[:, feature_columns] = values
        try:
            frame["window_end"] = pd.to_datetime(frame["window_end"], utc=True, errors="raise")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{name}.csv 的 window_end 含无效时间") from exc
        report["splits"][name] = {
            "rows": int(len(frame)),
            "label_counts": {str(int(k)): int(v) for k, v in frame["label"].value_counts().sort_index().items()},
            "entity_counts": {str(k): int(v) for k, v in frame["entity_id"].value_counts().items()},
            "window_end_min": frame["window_end"].min().isoformat(),
            "window_end_max": frame["window_end"].max().isoformat(),
            "missing_or_nonfinite_by_feature": {c: int(values[c].isna().sum()) for c in feature_columns},
        }
    if not (frames["train"]["label"] == 0).all():
        raise ValueError("train.csv 必须全部是 label=0 的正常训练样本")
    all_ids = pd.concat([f["window_id"].astype(str) for f in frames.values()], ignore_index=True)
    if all_ids.duplicated().any():
        raise ValueError("train/val/test 之间发现重复 window_id")
    # 每行窗口长度为 5 分钟；检查同实体 split 边界没有原始时间窗重叠。
    for entity in set().union(*(set(f.entity_id.astype(str)) for f in frames.values())):
        ends = {name: f.loc[f.entity_id.astype(str) == entity, "window_end"] for name, f in frames.items()}
        if all(len(ends[name]) for name in ("train", "val", "test")):
            train_end = ends["train"].max()
            val_start = ends["val"].min() - pd.Timedelta(minutes=5)
            val_end = ends["val"].max()
            test_start = ends["test"].min() - pd.Timedelta(minutes=5)
            if val_start < train_end or test_start < val_end:
                raise ValueError(f"实体 {entity} 的 train/val/test 时间窗口存在重叠")
    report["feature_count"] = len(feature_columns)
    report["feature_columns"] = feature_columns
    report["metadata_columns_excluded_from_model"] = METADATA_COLUMNS
    return frames, feature_columns, report


def load_train_validation(dataset_dir: Path) -> tuple[dict[str, pd.DataFrame], list[str], dict[str, Any]]:
    """Load only train and val for fitting/calibration; test.csv is never opened."""
    frames: dict[str, pd.DataFrame] = {}
    report: dict[str, Any] = {"dataset_dir": str(dataset_dir.resolve()), "splits_loaded": ["train", "val"],
                              "test_loaded": False, "splits": {}}
    for name in ("train", "val"):
        path = dataset_dir / f"{name}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"缺少数据文件: {path}")
        frame = pd.read_csv(path, encoding="utf-8-sig")
        if frame.empty or any(column not in frame for column in METADATA_COLUMNS):
            raise ValueError(f"{name}.csv 为空或缺少元数据列 {METADATA_COLUMNS}")
        frames[name] = frame

    base_columns = list(frames["train"].columns)
    features = [column for column in base_columns if column not in METADATA_COLUMNS]
    if len(features) != 23:
        raise ValueError(f"预期 23 个数值特征，实际 {len(features)}")
    seen_ids: set[str] = set()
    for name, frame in frames.items():
        if list(frame.columns) != base_columns:
            raise ValueError(f"{name}.csv 列名或顺序与 train.csv 不一致")
        if frame[METADATA_COLUMNS].isna().any().any():
            raise ValueError(f"{name}.csv 元数据存在空值")
        ids = frame.window_id.astype(str)
        if ids.duplicated().any() or seen_ids.intersection(ids):
            raise ValueError(f"{name}.csv 与前一 split 存在重复 window_id")
        seen_ids.update(ids)
        labels = pd.to_numeric(frame.label, errors="coerce")
        if labels.isna().any() or not set(labels.unique()).issubset({0, 1}):
            raise ValueError(f"{name}.csv 的 label 必须是 0/1")
        frame["label"] = labels.astype(int)
        for column in features:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        clean = frame[features].replace([np.inf, -np.inf], np.nan)
        frame.loc[:, features] = clean
        frame["window_end"] = pd.to_datetime(frame.window_end, utc=True, errors="raise")
        report["splits"][name] = {
            "rows": int(len(frame)),
            "label_counts": {str(int(k)): int(v) for k, v in frame.label.value_counts().sort_index().items()},
            "entity_counts": {str(k): int(v) for k, v in frame.entity_id.value_counts().items()},
            "window_end_min": frame.window_end.min().isoformat(),
            "window_end_max": frame.window_end.max().isoformat(),
            "missing_or_nonfinite_by_feature": {column: int(clean[column].isna().sum()) for column in features},
        }
    if not (frames["train"].label == 0).all():
        raise ValueError("train.csv 必须全部是正常样本")
    for entity in set(frames["train"].entity_id.astype(str)) & set(frames["val"].entity_id.astype(str)):
        train_end = frames["train"].loc[frames["train"].entity_id.astype(str) == entity, "window_end"].max()
        val_start = frames["val"].loc[frames["val"].entity_id.astype(str) == entity, "window_end"].min() - pd.Timedelta(minutes=5)
        if val_start < train_end:
            raise ValueError(f"实体 {entity} 的 train/val 窗口存在时间重叠")
    report["feature_count"] = len(features)
    report["feature_columns"] = features
    report["metadata_columns_excluded_from_model"] = METADATA_COLUMNS
    return frames, features, report


class MedianPreprocessor:
    """Persist training-only medians and an exact feature column order."""
    def fit(self, frame: pd.DataFrame, columns: list[str]) -> "MedianPreprocessor":
        self.feature_columns = list(columns)
        clean = frame.reindex(columns=columns).replace([np.inf, -np.inf], np.nan)
        all_missing = [c for c in columns if clean[c].isna().all()]
        if all_missing:
            raise ValueError(f"训练集中整列缺失，拒绝静默填补: {all_missing}")
        self.medians = {c: float(clean[c].median()) for c in columns}
        self.training_missing_counts = {c: int(clean[c].isna().sum()) for c in columns}
        return self

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.feature_columns if c not in frame]
        extra = [c for c in frame if c not in self.feature_columns]
        if missing or extra:
            raise ValueError(f"特征列不匹配，缺失={missing}，多余={extra}")
        clean = frame[self.feature_columns].replace([np.inf, -np.inf], np.nan)
        return clean.fillna(pd.Series(self.medians)).astype(float)

    def manifest(self) -> dict[str, Any]:
        return {"feature_columns": self.feature_columns,
                "imputation": "仅使用 train.csv 每列中位数填补 null/NaN/正负无穷",
                "training_medians": self.medians,
                "training_missing_counts": self.training_missing_counts}


def _metrics(labels: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, Any]:
    predicted = (scores >= threshold).astype(int)
    matrix = confusion_matrix(labels, predicted, labels=[0, 1])
    has_both = len(np.unique(labels)) == 2
    return {
        "count": int(len(labels)),
        "actual_label_counts": {str(int(k)): int(v) for k, v in pd.Series(labels).value_counts().sort_index().items()},
        "predicted_anomaly_count": int(predicted.sum()),
        "confusion_matrix": {"labels": [0, 1], "matrix": matrix.tolist(),
                             "layout": "[[TN, FP], [FN, TP]]"},
        "precision": float(precision_score(labels, predicted, zero_division=0)),
        "recall": float(recall_score(labels, predicted, zero_division=0)),
        "f1": float(f1_score(labels, predicted, zero_division=0)),
        "roc_auc": float(roc_auc_score(labels, scores)) if has_both else None,
        "pr_auc_average_precision": float(average_precision_score(labels, scores)) if has_both else None,
    }


def choose_validation_threshold(labels: np.ndarray, scores: np.ndarray) -> tuple[float, dict[str, Any]]:
    """Choose the score cutoff with best validation F1; tie-break toward precision/cutoff."""
    if len(np.unique(labels)) != 2:
        raise ValueError("val.csv 必须同时包含正常和异常标签，才能用 F1 校准阈值")
    candidates = np.r_[np.unique(scores), np.nextafter(np.max(scores), np.inf)]
    rows = []
    for threshold in candidates:
        predicted = scores >= threshold
        p = float(precision_score(labels, predicted, zero_division=0))
        r = float(recall_score(labels, predicted, zero_division=0))
        f = float(f1_score(labels, predicted, zero_division=0))
        rows.append((f, p, float(threshold), r))
    best = max(rows, key=lambda x: (x[0], x[1], x[2]))
    threshold = best[2]
    return threshold, {
        "method": "在 val.csv 上最大化 F1；并列时优先 precision，再选较高阈值",
        "score_direction": "IForest decision_function 分数越高越异常",
        "threshold": threshold,
        "validation_metrics_at_selected_threshold": _metrics(labels, scores, threshold),
        "pr_curve": {"precision": precision_recall_curve(labels, scores)[0].tolist(),
                     "recall": precision_recall_curve(labels, scores)[1].tolist()},
    }


def _score_frame(frame: pd.DataFrame, scores: np.ndarray, threshold: float) -> pd.DataFrame:
    return pd.DataFrame({
        "window_id": frame.window_id.astype(str),
        "entity_id": frame.entity_id.astype(str),
        "window_end": frame.window_end.astype(str),
        "label": frame.label.astype(int),
        "anomaly_score": scores.astype(float),
        "threshold": float(threshold),
        "predicted_label": (scores >= threshold).astype(int),
    })


def _score_distribution_by_label(frame: pd.DataFrame) -> dict[str, Any]:
    """Summarize score quantiles separately for normal and injected anomalies."""
    result: dict[str, Any] = {}
    quantiles = (0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0)
    for label, name in ((0, "normal_label_0"), (1, "anomaly_label_1")):
        subset = frame.loc[frame.label == label]
        if subset.empty:
            result[name] = {"count": 0, "score_quantiles": {}}
            continue
        scores = subset.anomaly_score.to_numpy(dtype=float)
        result[name] = {
            "count": int(len(subset)),
            "predicted_anomaly_count": int((subset.predicted_label == 1).sum()),
            "predicted_anomaly_rate": float((subset.predicted_label == 1).mean()),
            "score_quantiles": {
                str(int(q * 100)): float(v)
                for q, v in zip(quantiles, np.quantile(scores, quantiles))
            },
        }
    normal = result["normal_label_0"]["score_quantiles"]
    anomaly = result["anomaly_label_1"]["score_quantiles"]
    if normal and anomaly:
        overlap_start = max(normal["0"], anomaly["0"])
        overlap_end = min(normal["100"], anomaly["100"])
        result["score_ranges_overlap"] = overlap_start <= overlap_end
        result["overlap_score_range"] = [overlap_start, overlap_end] if overlap_start <= overlap_end else None
    return result


def _write_scores(frame: pd.DataFrame, output: Path, split: str) -> None:
    output.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output / f"{split}_scores.csv", index=False, encoding="utf-8-sig")


def _write_candidate_events(frame: pd.DataFrame, output: Path) -> None:
    """Write predicted test anomalies in an L2.5-friendly JSONL shape, without labels."""
    path = output / "l25_test_candidate_events.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        for row in frame.loc[frame.predicted_label == 1].to_dict(orient="records"):
            event = {
                "schema_version": "2.5.0",
                "event_id": f"L25-IFOREST-{row['window_id']}",
                "entity_id": row["entity_id"],
                "anomaly_type": "IFOREST_WINDOW_ANOMALY",
                "start_time": (pd.Timestamp(row["window_end"]) - pd.Timedelta(minutes=5)).isoformat(),
                "end_time": row["window_end"],
                "detection_sources": ["PYOD_IFOREST"],
                "matched_window_ids": [row["window_id"]],
                "source_refs": [{"topic": "l1-aligned-windows", "window_id": row["window_id"]}],
                "anomaly_score": float(row["anomaly_score"]),
                "threshold": float(row["threshold"]),
                "decision": {"anomaly": True, "action": "CANDIDATE",
                             "reason": "Validation-calibrated IForest score exceeded threshold"},
            }
            stream.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n")


def _plots(val: pd.DataFrame, test: pd.DataFrame, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 5))
    for label, name, color in ((0, "Normal", "#4C78A8"), (1, "Anomaly", "#E45756")):
        values = val.loc[val.label == label, "anomaly_score"]
        if len(values): ax.hist(values, bins=35, alpha=.58, label=name, color=color)
    ax.axvline(float(val.threshold.iloc[0]), color="#222", linestyle="--", label="Validation threshold")
    ax.set(title="Validation score distribution", xlabel="IForest anomaly score (higher = more anomalous)", ylabel="Window count")
    ax.legend(); fig.tight_layout(); fig.savefig(output / "validation_score_distribution.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 5))
    for label, name, color in ((0, "Normal", "#4C78A8"), (1, "Anomaly", "#E45756")):
        values = test.loc[test.label == label, "anomaly_score"]
        if len(values): ax.hist(values, bins=35, alpha=.58, label=name, color=color)
    ax.axvline(float(test.threshold.iloc[0]), color="#222", linestyle="--", label="Locked validation threshold")
    ax.set(title="Test score distribution", xlabel="IForest anomaly score (higher = more anomalous)", ylabel="Window count")
    ax.legend(); fig.tight_layout(); fig.savefig(output / "test_score_distribution.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 5))
    timeline = test.copy(); timeline["window_end"] = pd.to_datetime(timeline.window_end, utc=True)
    for entity, group in timeline.groupby("entity_id"):
        group = group.sort_values("window_end")
        ax.plot(group.window_end, group.anomaly_score, marker=".", markersize=2, linewidth=.8, label=entity)
    ax.axhline(float(test.threshold.iloc[0]), color="#222", linestyle="--", label="Locked validation threshold")
    ax.set(title="Test anomaly scores over time", xlabel="Window end (UTC)", ylabel="IForest anomaly score")
    ax.legend(); fig.autofmt_xdate(); fig.tight_layout(); fig.savefig(output / "test_score_timeline.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(5, 5))
    matrix = confusion_matrix(test.label, test.predicted_label, labels=[0, 1])
    ax.imshow(matrix, cmap="Blues")
    for (i, j), value in np.ndenumerate(matrix): ax.text(j, i, str(value), ha="center", va="center", color="#13294B", fontsize=14)
    ax.set(xticks=[0, 1], yticks=[0, 1], xticklabels=["Normal", "Anomaly"], yticklabels=["Normal", "Anomaly"],
           xlabel="Predicted label", ylabel="Actual label", title="Test confusion matrix")
    fig.tight_layout(); fig.savefig(output / "test_confusion_matrix.png", dpi=150); plt.close(fig)


def inspect(args: argparse.Namespace) -> dict[str, Any]:
    _, _, report = load_splits(args.datasets_dir)
    _write_json(args.output / "data_inspection_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def train(args: argparse.Namespace) -> None:
    # Load only train and val so retraining never reads test.csv.
    frames, columns, data_report = load_train_validation(args.datasets_dir)
    preprocessor = MedianPreprocessor().fit(frames["train"][columns], columns)
    x_train = preprocessor.transform(frames["train"][columns])
    model_parameters = {"n_estimators": args.n_estimators, "max_samples": args.max_samples,
                        "max_features": args.max_features, "contamination": args.model_contamination,
                        "random_state": args.seed}
    model = IForest(**model_parameters)
    # 只拟合 train 的正常样本；val/test 标签完全不参与 IForest.fit。
    started = time.perf_counter()
    model.fit(x_train.to_numpy())
    training_seconds = time.perf_counter() - started
    val_matrix = preprocessor.transform(frames["val"][columns]).to_numpy()
    inference_started = time.perf_counter()
    val_scores = np.asarray(model.decision_function(val_matrix), dtype=float)
    validation_inference_seconds = time.perf_counter() - inference_started
    threshold, threshold_report = choose_validation_threshold(frames["val"].label.to_numpy(), val_scores)
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "preprocessor": preprocessor, "threshold": threshold,
                 "feature_columns": columns, "model_parameters": model_parameters,
                 "threshold_source": "val.csv 独立选择最大 F1；score >= threshold；分数越高越异常"},
                output / "iforest_csv_bundle.joblib")
    _write_json(output / "data_inspection_report.json", data_report)
    _write_json(output / "feature_manifest.json", preprocessor.manifest())
    _write_json(output / "threshold_selection_report.json", threshold_report)
    config = {
        "dataset_dir": str(args.datasets_dir.resolve()), "output_dir": str(output.resolve()),
        "train_file_sha256": _sha256(args.datasets_dir / "train.csv"),
        "val_file_sha256": _sha256(args.datasets_dir / "val.csv"),
        "feature_columns": columns, "metadata_excluded": METADATA_COLUMNS,
        "model": "PyOD IForest", "n_estimators": args.n_estimators,
        "max_samples": args.max_samples, "max_features": args.max_features,
        "model_contamination_internal_only": args.model_contamination,
        "decision_threshold_source": "val.csv 上最大化 F1；最终业务判定不使用 PyOD contamination threshold_",
        "random_state": args.seed,
        "training_seconds": training_seconds,
        "validation_inference_seconds": validation_inference_seconds,
        "test_data_loaded": False,
        "split_policy": "固定的时间顺序 train/val/test；不进行随机切分",
        "versions": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__,
                     "pyod": pyod.__version__, "scikit_learn": sklearn.__version__},
    }
    _write_json(output / "training_config.json", config)
    val_frame = _score_frame(frames["val"], val_scores, threshold)
    threshold_report["validation_score_distribution_by_label"] = _score_distribution_by_label(val_frame)
    _write_json(output / "threshold_selection_report.json", threshold_report)
    _write_scores(val_frame, output, "val")
    _write_json(output / "training_summary.json", {
        "model_file": str((output / "iforest_csv_bundle.joblib").resolve()),
        "model_size_bytes": int((output / "iforest_csv_bundle.joblib").stat().st_size),
        "model_parameters": model_parameters,
        "feature_columns": columns,
        "selected_threshold": threshold,
        "threshold_selected_on": "val.csv only",
        "validation_metrics": threshold_report["validation_metrics_at_selected_threshold"],
        "training_seconds": training_seconds,
        "validation_inference_seconds": validation_inference_seconds,
        "test_data_loaded": False,
    })
    print(f"训练完成；模型、val 分数和已锁定阈值写入 {output.resolve()}")


def evaluate(args: argparse.Namespace) -> None:
    frames, columns, data_report = load_splits(args.datasets_dir)
    bundle = joblib.load(args.model)
    if bundle["feature_columns"] != columns:
        raise ValueError("当前 CSV 特征列与模型训练时的列清单不一致")
    model, preprocessor, threshold = bundle["model"], bundle["preprocessor"], float(bundle["threshold"])
    # 评估只读取 test 标签计算指标；不会在这里重新选择阈值。
    test_matrix = preprocessor.transform(frames["test"][columns]).to_numpy()
    test_scores = np.asarray(model.decision_function(test_matrix), dtype=float)
    test_frame = _score_frame(frames["test"], test_scores, threshold)
    _write_scores(test_frame, args.output, "test")
    _write_candidate_events(test_frame, args.output)
    overall = _metrics(frames["test"].label.to_numpy(), test_scores, threshold)
    by_entity = {}
    for entity, indices in frames["test"].groupby("entity_id").groups.items():
        ix = np.asarray(list(indices), dtype=int)
        by_entity[str(entity)] = _metrics(frames["test"].label.to_numpy()[ix], test_scores[ix], threshold)
    report = {"evaluation_split": "test", "threshold_locked_from": "val.csv",
              "threshold": threshold, "overall": overall, "by_entity": by_entity,
              "score_summary": {"min": float(test_scores.min()), "median": float(np.median(test_scores)), "max": float(test_scores.max())},
              "limitations": ["数据为合成模拟数据，测试指标仅衡量该合成分布。", "阈值在验证集按 F1 选择；测试集没有参与阈值选择。", "异常分数是排序分数，不是故障概率。"]}
    _write_json(args.output / "test_evaluation_report.json", report)
    # 验证集分数已在 train 阶段写出，用于绘制校准分数分布。
    val_path = args.output / "val_scores.csv"
    if val_path.exists():
        val_frame = pd.read_csv(val_path)
    else:
        val_matrix = preprocessor.transform(frames["val"][columns]).to_numpy()
        val_scores = np.asarray(model.decision_function(val_matrix), dtype=float)
        val_frame = _score_frame(frames["val"], val_scores, threshold)
        _write_scores(val_frame, args.output, "val")
    _write_json(args.output / "score_distribution_report.json", {
        "threshold": threshold,
        "threshold_source": "validation split only; maximum F1",
        "score_direction": "higher scores indicate more anomalous windows; scores are not probabilities",
        "validation": _score_distribution_by_label(val_frame),
        "test": _score_distribution_by_label(test_frame),
    })
    _plots(val_frame, test_frame, args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def run_all(args: argparse.Namespace) -> None:
    inspect(args)
    train(args)
    evaluate(argparse.Namespace(datasets_dir=args.datasets_dir,
                                 model=args.output / "iforest_csv_bundle.joblib", output=args.output))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CSV train/validation/test workflow for PyOD IForest")
    sub = parser.add_subparsers(dest="command", required=True)
    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--datasets-dir", type=Path, default=DEFAULT_DATASETS)
        p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p = sub.add_parser("inspect", help="检查三个 CSV split"); common(p); p.set_defaults(func=inspect)
    p = sub.add_parser("train", help="仅用正常 train 拟合，并在 val 选择阈值"); common(p)
    p.add_argument("--n-estimators", type=int, default=300)
    p.add_argument("--max-samples", type=int, default=1024)
    p.add_argument("--max-features", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--model-contamination", type=float, default=0.01); p.set_defaults(func=train)
    p = sub.add_parser("evaluate", help="锁定阈值后评估 test"); common(p)
    p.add_argument("--model", type=Path, default=DEFAULT_OUTPUT / "iforest_csv_bundle.joblib"); p.set_defaults(func=evaluate)
    p = sub.add_parser("all", help="检查、训练、val 校准、test 评估并绘图"); common(p)
    p.add_argument("--n-estimators", type=int, default=300)
    p.add_argument("--max-samples", type=int, default=1024)
    p.add_argument("--max-features", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--model-contamination", type=float, default=0.01); p.set_defaults(func=run_all)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    # Keep joblib artifacts importable outside this command-line process.
    sys.modules.setdefault("iforest_tool", sys.modules[__name__])
    MedianPreprocessor.__module__ = "iforest_tool"
    main()
