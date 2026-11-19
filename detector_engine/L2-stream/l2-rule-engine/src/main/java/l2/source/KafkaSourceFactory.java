// 构建读取 L1 对齐窗口主题的 KafkaSource（从最早位点开始消费）。
package l2.source;

import l2.config.AppConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class KafkaSourceFactory {
    private KafkaSourceFactory() {}
    public static KafkaSource<String> create(AppConfig c) {
        return KafkaSource.<String>builder().setBootstrapServers(c.bootstrapServers()).setTopics(c.inputTopic())
                .setGroupId(c.groupId()).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
    }
}
