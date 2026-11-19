import importlib.util
import sys
from pathlib import Path


GENERATOR_PATH = Path(__file__).resolve().parents[1] / "iforest-dataset" / "generate_dataset.py"
SPEC = importlib.util.spec_from_file_location("l1_aligned_dataset_generator", GENERATOR_PATH)
generator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = generator
SPEC.loader.exec_module(generator)


def test_training_csv_uses_l1_window_and_feature_statistics():
    columns = generator.load_columns(None)
    frame = generator.make_split(
        "train", 9, "2026-01-01T00:00:00Z", 42, columns,
        window_size_sec=300, slide_sec=60, metric_interval_sec=30,
        anomaly_ratio=0.0, scenario_frequency=0.0,
    )

    assert len(columns) == 23
    assert set(frame.entity_id) == set(generator.ENTITY_IDS)
    assert (frame.label == 0).all()
    assert set(frame.logs_total_per_minute) == {1.4}
    assert set(frame.traces_span_per_minute) == {2.0}
    assert ((frame.logs_error_rate + frame.logs_warn_rate + frame.logs_info_rate).round(6) == 1.0).all()
