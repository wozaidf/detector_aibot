"""创建 L2.5 输入 Topic 的 Kafka 消费者。"""

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from typing import Any

from config import Settings


def create_consumer(settings: Settings) -> KafkaConsumer:
    """创建 L2.5 输入消费者。"""
    return KafkaConsumer(
        settings.L25_INPUT_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.L25_GROUP_ID,
        auto_offset_reset=settings.L25_AUTO_OFFSET_RESET,
        # 由业务流程在输出确认后提交 offset。
        enable_auto_commit=False,
        client_id=f"{settings.L25_GROUP_ID}-consumer",
    )


def commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """提交已处理 Kafka 消息对应的 offset。"""
    # Kafka offset 表示下一条待读记录，因此提交当前 offset + 1。
    partition = TopicPartition(message.topic, message.partition)
    offsets = {
        partition: OffsetAndMetadata(offset=message.offset + 1, metadata=""),
    }
    consumer.commit(offsets=offsets)
