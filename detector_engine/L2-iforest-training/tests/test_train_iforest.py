import json

import numpy as np
import pandas as pd
import pytest

from train_iforest import (METADATA_COLUMNS, MedianPreprocessor,
                           choose_validation_threshold, load_splits, train, evaluate)


FEATURES = [f"feature_{i:02d}" for i in range(23)]


def make_splits(directory, n=60):
    directory.mkdir(parents=True, exist_ok=True)
    for split, count in (("train", n), ("val", 30), ("test", 30)):
        labels = np.zeros(count, dtype=int)
        if split != "train":
            labels[-6:] = 1
        data = {
            "window_id": [f"{split}-{i}" for i in range(count)],
            "entity_id": ["server-001"] * count,
            "window_end": pd.date_range(
                pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(minutes={"train": 0, "val": (n + 1) * 6, "test": (n + 1 + 31) * 6}[split]),
                periods=count, freq="6min").astype(str),
            "label": labels,
        }
        for j, column in enumerate(FEATURES):
            values = np.linspace(0, 1, count) + j
            if split != "train":
                values[-6:] += 20
            data[column] = values
        pd.DataFrame(data).to_csv(directory / f"{split}.csv", index=False, encoding="utf-8-sig")


def test_csv_loader_separates_23_features_from_metadata_and_labels(tmp_path):
    make_splits(tmp_path / "datasets")
    frames, columns, report = load_splits(tmp_path / "datasets")
    assert len(columns) == 23
    assert not set(columns) & set(METADATA_COLUMNS)
    assert len(frames["train"]) == 60
    assert report["splits"]["val"]["label_counts"] == {"0": 24, "1": 6}


def test_train_must_contain_only_normal_rows(tmp_path):
    data_dir = tmp_path / "datasets"
    make_splits(data_dir)
    train_df = pd.read_csv(data_dir / "train.csv")
    train_df.loc[0, "label"] = 1
    train_df.to_csv(data_dir / "train.csv", index=False)
    with pytest.raises(ValueError, match="label=0"):
        load_splits(data_dir)


def test_preprocessor_uses_training_median_and_rejects_all_missing_column():
    prep = MedianPreprocessor().fit(pd.DataFrame({"a": [1.0, np.nan, 5.0]}), ["a"])
    transformed = prep.transform(pd.DataFrame({"a": [np.inf, 10.0]}))
    assert transformed["a"].tolist() == [3.0, 10.0]
    with pytest.raises(ValueError, match="整列缺失"):
        MedianPreprocessor().fit(pd.DataFrame({"a": [np.nan, np.inf]}), ["a"])


def test_validation_threshold_uses_validation_labels_and_higher_scores_as_anomalies():
    labels = np.array([0, 0, 0, 1, 1, 1])
    scores = np.array([-3, -2, -1, 1, 2, 3], dtype=float)
    threshold, report = choose_validation_threshold(labels, scores)
    assert threshold > -1 and threshold <= 1
    assert report["validation_metrics_at_selected_threshold"]["f1"] == 1.0
    assert report["score_direction"].endswith("越高越异常")


def test_train_calibrates_on_val_and_evaluate_locks_threshold_for_test(tmp_path):
    data_dir = tmp_path / "datasets"
    output = tmp_path / "artifacts"
    make_splits(data_dir)
    args = type("Args", (), {"datasets_dir": data_dir, "output": output,
                             "n_estimators": 20, "max_samples": 1024,
                             "max_features": 1.0, "model_contamination": 0.1, "seed": 7})()
    train(args)
    bundle_path = output / "iforest_csv_bundle.joblib"
    assert bundle_path.exists()
    saved_threshold = json.loads((output / "threshold_selection_report.json").read_text(encoding="utf-8"))["threshold"]
    evaluate(type("EvalArgs", (), {"datasets_dir": data_dir, "output": output,
                                    "model": bundle_path})())
    report = json.loads((output / "test_evaluation_report.json").read_text(encoding="utf-8"))
    distributions = json.loads((output / "score_distribution_report.json").read_text(encoding="utf-8"))
    assert report["threshold_locked_from"] == "val.csv"
    assert report["threshold"] == saved_threshold
    assert distributions["validation"]["normal_label_0"]["count"] == 24
    assert distributions["validation"]["anomaly_label_1"]["count"] == 6
    assert "score_ranges_overlap" in distributions["validation"]
    scored = pd.read_csv(output / "test_scores.csv")
    assert {"window_id", "entity_id", "window_end", "label", "anomaly_score", "threshold", "predicted_label"} <= set(scored)
    assert (output / "l25_test_candidate_events.jsonl").exists()
    assert (output / "test_confusion_matrix.png").exists()
