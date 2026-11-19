// 异常事件的 Kafka 序列化器：按 snake_case 输出 JSON，并以 entityId 作为消息 key。
package l2.sink;

import l2.model.AnomalyEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AnomalyKafkaSerializer implements KafkaRecordSerializationSchema<AnomalyEvent> {
    private final String topic;
    private transient ObjectMapper mapper;
    public AnomalyKafkaSerializer(String topic) { this.topic = topic; }
    @Override public ProducerRecord<byte[], byte[]> serialize(AnomalyEvent event, KafkaSinkContext context, Long timestamp) {
        try {
            if (mapper == null) mapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            return new ProducerRecord<>(topic, null, timestamp, event.entityId().getBytes(java.nio.charset.StandardCharsets.UTF_8), mapper.writeValueAsBytes(event));
        } catch (Exception e) { throw new IllegalStateException("Failed to serialize anomaly event", e); }
    }
}
