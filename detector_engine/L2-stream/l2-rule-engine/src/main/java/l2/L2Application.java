// L2 规则引擎 Flink 作业入口：串联 Kafka 数据源、解析校验、重放去重、Drools 规则评估、CEP 检测、最终决策与 Kafka 输出。
package l2;

import l2.config.AppConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class L2Application {
    public static void main(String[] args) throws Exception {
        AppConfig c = AppConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        L2Pipeline.build(env, c);
        env.execute("L2 Rule Detection Engine");
    }
}
