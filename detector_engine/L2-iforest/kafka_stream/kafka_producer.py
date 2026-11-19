"""创建 L2.5 Kafka 生产者并同步确认预测结果。"""

import json

from kafka import KafkaProducer

from config import Settings


def create_producer(settings: Settings) -> KafkaProducer:
    """创建 L2.5 Kafka 输出生产者。"""
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=10,
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        # 使用标准 JSON 编码，并保留中文字段内容。
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8"),
        client_id=f"{settings.L25_GROUP_ID}-producer",
    )


def publish_prediction(producer: KafkaProducer, settings: Settings,
                       entity_id: str, event: dict) -> None:
    """发布评分事件并等待 broker 确认。"""
    # 等待发送结果，失败时由调用方处理异常。
    producer.send(settings.L25_OUTPUT_TOPIC, key=entity_id, value=event).get(timeout=30)
