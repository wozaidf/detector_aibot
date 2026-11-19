from kafka import KafkaConsumer
import json
from config import get_settings

settings = get_settings()

# 1. 创建 Kafka 消费者实例
consumer = KafkaConsumer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    group_id=settings.L4_GROUP_ID,
    # auto_offset_reset指的是消费进度的策略，设置为 'earliest' 表示从最早的消息开始消费，'latest' 表示从最新的消息开始消费
    auto_offset_reset=settings.L4_AUTO_OFFSET_RESET,
    # consumer_timeout_ms=5000, # 设置超时时间，避免无限阻塞
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # 数据反序列化
    enable_auto_commit=False,
    max_poll_interval_ms=60000,  # 设置最大轮询间隔时间，避免长时间处理导致消费者被认为失效
    max_poll_records=1  # 设置每次轮询获取的最大消息数，避免一次性拉取过多消息导致处理缓慢
)
# 2. 订阅 Kafka Topic
consumer.subscribe([settings.L4_CONSUMER_TOPIC])
