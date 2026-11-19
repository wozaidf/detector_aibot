from kafka import KafkaProducer, JsonSerializer

from config import get_settings

settings = get_settings()

# 1. 创建 Kafka 生产者实例
producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=JsonSerializer()  # 数据序列化
)


# 2. 定义发送消息到下游 Kafka Topic 的函数
def producer_send(message, key=None, topic=settings.L4_PRODUCER_TOPIC, ):
    if key is None:
        raise ValueError(f"压缩 Topic {topic} 的消息必须有非空 Key")
    if isinstance(key, str):
        key = key.encode("utf-8")
    metadata = producer.send(topic, key=key, value=message).get(timeout=30)
    # print(f"消息已写入 {topic}：partition={metadata.partition}, offset={metadata.offset}")
    return metadata
