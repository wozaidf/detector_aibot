// 构建输出 L2 异常事件的 KafkaSink（至少一次投递语义）。
package l2.sink;

import l2.config.AppConfig;
import l2.model.AnomalyEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {}
    public static KafkaSink<AnomalyEvent> create(AppConfig c) {
        return KafkaSink.<AnomalyEvent>builder().setBootstrapServers(c.bootstrapServers()).setRecordSerializer(new AnomalyKafkaSerializer(c.outputTopic())).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }
}
