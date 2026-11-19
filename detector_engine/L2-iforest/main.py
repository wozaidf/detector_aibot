"""L2.5 流程：消费 L2 事件，逐窗口评分并发布结果。"""

from __future__ import annotations

import json
import logging
from typing import Any

import joblib
import pandas as pd

from config import get_settings
from inference.feature_schema import FEATURE_COLUMNS
from inference.predictor import predict_iforest
from inference.window_adapter import build_prediction_event, build_window_result, expand_l2_event, extract_window_features
from kafka_stream.kafka_consumer import commit_message, create_consumer
from kafka_stream.kafka_producer import create_producer, publish_prediction

LOGGER = logging.getLogger("l2_iforest")


def _reject_json_constant(value: str) -> None:
    """验证输入 JSON 中的数值常量。"""
    raise ValueError(f"non-standard JSON constant: {value}")


def _load_bundle(model_path) -> tuple[Any, Any, float, list[str]]:
    """为推理准备模型、预处理器、阈值和特征列。"""
    if not model_path.is_file():
        raise FileNotFoundError(f"IForest 模型包不存在：{model_path}")
    # 从同一训练产物读取模型及其推理配置。
    bundle = joblib.load(model_path)
    model = bundle["model"]
    preprocessor = bundle["preprocessor"]
    threshold = float(bundle["threshold"])
    columns = list(bundle["feature_columns"])
    if columns != list(preprocessor.feature_columns):
        raise ValueError("模型包特征列与预处理器列顺序不一致")
    if columns != list(FEATURE_COLUMNS):
        raise ValueError("模型包特征列与当前共享特征 schema 不一致")
    return model, preprocessor, threshold, columns


def process_messages() -> None:
    """运行 L2 输入消费、逐窗口评分和 L2.5 事件发布流程。"""
    settings = get_settings()
    model_path = settings.model_path
    model, preprocessor, threshold, columns = _load_bundle(model_path)
    LOGGER.info(
        "IForest 服务启动：input=%s output=%s group=%s model=%s threshold=%s",
        settings.L25_INPUT_TOPIC, settings.L25_OUTPUT_TOPIC,
        settings.L25_GROUP_ID, model_path, threshold,
    )

    consumer = None
    producer = None
    try:
        consumer = create_consumer(settings)
        producer = create_producer(settings)
        for message in consumer:
            raw_value = message.value
            try:
                if raw_value is None:
                    raise ValueError("Kafka tombstone 消息没有 value")
                record = json.loads(raw_value.decode("utf-8"), parse_constant=_reject_json_constant)
                # 按统一 L2 结构提取待评分窗口。
                record, inputs, unscored_window_ids = expand_l2_event(record)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                LOGGER.error(
                    "跳过无效 Kafka 输入：topic=%s partition=%s offset=%s error=%s",
                    message.topic, message.partition, message.offset, exc,
                )
                # 记录无效消息并提交 offset，继续消费后续输入。
                commit_message(consumer, message)
                continue

            if not inputs and not unscored_window_ids:
                l2_result = record.get("l2_result") if isinstance(record.get("l2_result"), dict) else {}
                LOGGER.error(
                    "输入事件没有可评分的窗口特征：event_id=%s matched_window_ids=%s",
                    record.get("event_id"), l2_result.get("matched_window_ids"),
                )
                commit_message(consumer, message)
                continue

            window_results = []
            for item in inputs:
                window_record = item["window"]
                window_id = window_record.get("window_id")
                try:
                    features, warnings = extract_window_features(window_record)
                except (ValueError, TypeError) as exc:
                    LOGGER.error(
                        "跳过无法评分的窗口：event_id=%s window_id=%s error=%s",
                        record.get("event_id"), window_id, exc,
                    )
                    unscored_window_ids.append(window_id)
                    continue
                try:
                    # 单独构造一个窗口样本并计算分数。
                    frame = pd.DataFrame([features], columns=columns)
                    scores, flags = predict_iforest(frame, model, preprocessor, threshold)
                    score = float(scores[0])
                    anomaly = bool(flags[0])
                    window_results.append(build_window_result(
                        window_record, item["source_ref"], features, warnings,
                        score, threshold, anomaly,
                    ))
                except Exception as exc:
                    LOGGER.exception("窗口预测失败：event_id=%s window_id=%s error=%s", record.get("event_id"), window_id, exc)
                    unscored_window_ids.append(window_id)

            # 将评分明细和未评分窗口汇总到输入事件。
            event = build_prediction_event(record, window_results, unscored_window_ids, "PYOD_IFOREST")
            publish_prediction(producer, settings, str(record.get("entity_id", "")), event)

            # 输出确认后提交输入 offset。
            commit_message(consumer, message)
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.close(timeout=10)


def main() -> int:
    """启动 L2.5 Kafka 推理服务。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    try:
        process_messages()
    except KeyboardInterrupt:
        LOGGER.info("收到停止信号，关闭 IForest Kafka 服务")
    except Exception:
        LOGGER.exception("IForest Kafka 服务异常退出")
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
