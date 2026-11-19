from jev.jev_decide import jev_decide
from laya_predict.laya_api import laya_decide
from threshold import diagnosis_score, should_publish
from config import get_settings
from kafka_stream.JsonToState import json_build_state
from kafka_stream.JevResultToEvent import build_l4_event
from kafka_stream.kafka_consumer import consumer
from kafka_stream.kafka_producer import producer_send
from kafka_stream.postgres_rejected_events import RejectedEventStore

settings = get_settings()
# 初始化拒绝事件存储
rejected_events = RejectedEventStore(settings.postgres_dsn)

try:
    for message in consumer:
        data = message.value
        # print(f"上游事件: {data}")
        # 1、 用上游数据转化为jev的上下文状态
        state = json_build_state(data)

        # 2、 使用jev处理消息，得到决策结果
        result = laya_decide(state)
        print(f"laya输出结果：{result}")

        # 3、先生成 L4 阶段事件，再按事件级分数筛选；未通过时保存完整 L4 输出。
        result = build_l4_event(data, result)
        # 没有达到阈值的事件，保存到 PostgreSQL 数据库中，避免丢失数据
        if not should_publish(result, settings.L4_DIAGNOSIS_THRESHOLD):
            score = diagnosis_score(result)
            rejected_events.store(message.topic, result, score, settings.L4_DIAGNOSIS_THRESHOLD, source_event=data)
            print(f"事件未达到 L4 发布阈值：event_id={result.get('event_id')}")
            consumer.commit()
            continue

        # 4、 发送达到阈值的 L4 事件到下一个服务
        entity_id = result.get("entity_id")
        if not entity_id:
            raise ValueError("L2 异常事件缺少 entity_id，无法写入压缩 Topic")
        producer_send(result, key=entity_id)
        # 确认处理成功，提交 Offset，避免重复消费，保存处理状态，更新消费进度
        consumer.commit()

except Exception as e:
    print(f"处理失败: {e}")

finally:
    # 程序结束时关闭消费者
    consumer.close()
    rejected_events.close()
