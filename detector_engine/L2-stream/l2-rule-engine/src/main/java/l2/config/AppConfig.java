// 作业全局配置：统一从命令行参数与环境变量读取 Kafka、阈值、窗口等配置项。
package l2.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public record AppConfig(
        String bootstrapServers, String inputTopic, String outputTopic, String groupId,
        double cpuHighPct, double memoryHighPct, double latencyHighMs,
        double packetLossHighRatio, double logErrorRate, double spanFailureRate,
        double cpuSeverePct, double memorySeverePct, double latencySevereMs,
        double packetLossSevereRatio, double logErrorSevereRate, double spanFailureSevereRate,
        double jointNetworkLatencyMs, double jointNetworkPacketLossRatio,
        double jointLogErrorRate, double jointSpanFailureRate, int jointLogTraceMinOverlapCount,
        double jointMultiCpuPct, int jointMultiMinFailedSpanCount, int jointMultiMinAlarmCount,
        int repeatedErrorCount, int cepOccurrences, int cepComplexHorizonSeconds, int watermarkSeconds,
        int idleSeconds, int dedupSeconds, int associationWindowSeconds,
        int cepMaxSequenceSeconds, boolean printResults) implements Serializable {

    public static AppConfig fromArgs(String[] args) {
        Map<String, String> p = parseArgs(args);
        return new AppConfig(
                get(p, "bootstrap", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
                get(p, "input-topic", env("L1_INPUT_TOPIC", "l1-aligned-windows")),
                get(p, "output-topic", env("L2_OUTPUT_TOPIC", "l2-anomaly-events")),
                get(p, "group-id", env("L2_GROUP_ID", "l2-rule-engine-v1")),
                getDouble(p, "cpu-high", envDouble("L2_CPU_HIGH_PCT", 80.0)),
                getDouble(p, "memory-high", envDouble("L2_MEMORY_HIGH_PCT", 85.0)),
                getDouble(p, "latency-high", envDouble("L2_LATENCY_HIGH_MS", 160.0)),
                getDouble(p, "packet-loss-high", envDouble("L2_PACKET_LOSS_HIGH_RATIO", 0.03)),
                getDouble(p, "log-error-rate", envDouble("L2_LOG_ERROR_RATE", 0.30)),
                getDouble(p, "span-failure-rate", envDouble("L2_SPAN_FAILURE_RATE", 0.30)),
                getDouble(p, "cpu-severe", envDouble("L2_CPU_SEVERE_PCT", 95.0)),
                getDouble(p, "memory-severe", envDouble("L2_MEMORY_SEVERE_PCT", 92.0)),
                getDouble(p, "latency-severe", envDouble("L2_LATENCY_SEVERE_MS", 220.0)),
                getDouble(p, "packet-loss-severe", envDouble("L2_PACKET_LOSS_SEVERE_RATIO", 0.05)),
                getDouble(p, "log-error-severe", envDouble("L2_LOG_ERROR_SEVERE_RATE", 0.60)),
                getDouble(p, "span-failure-severe", envDouble("L2_SPAN_FAILURE_SEVERE_RATE", 0.80)),
                getDouble(p, "joint-network-latency", envDouble("L2_JOINT_NETWORK_LATENCY_MS", 160.0)),
                getDouble(p, "joint-network-packet-loss", envDouble("L2_JOINT_NETWORK_PACKET_LOSS_RATIO", 0.03)),
                getDouble(p, "joint-log-error-rate", envDouble("L2_JOINT_LOG_ERROR_RATE", 0.30)),
                getDouble(p, "joint-span-failure-rate", envDouble("L2_JOINT_SPAN_FAILURE_RATE", 0.30)),
                getInt(p, "joint-log-trace-min-overlap", envInt("L2_JOINT_LOG_TRACE_MIN_OVERLAP_COUNT", 1)),
                getDouble(p, "joint-multi-cpu", envDouble("L2_JOINT_MULTI_CPU_PCT", 80.0)),
                getInt(p, "joint-multi-min-failed-spans", envInt("L2_JOINT_MULTI_MIN_FAILED_SPAN_COUNT", 1)),
                getInt(p, "joint-multi-min-alarms", envInt("L2_JOINT_MULTI_MIN_ALARM_COUNT", 1)),
                getInt(p, "repeated-error-count", envInt("L2_REPEATED_ERROR_COUNT", 3)),
                getInt(p, "cep-occurrences", envInt("L2_CEP_OCCURRENCES", 3)),
                getInt(p, "cep-complex-horizon-seconds", envInt("L2_CEP_COMPLEX_HORIZON_SECONDS", 600)),
                getInt(p, "watermark-seconds", envInt("L2_WATERMARK_SECONDS", 5)),
                getInt(p, "idle-seconds", envInt("L2_IDLE_SECONDS", 30)),
                getInt(p, "dedup-seconds", envInt("L2_DEDUP_SECONDS", 300)),
                getInt(p, "association-window-seconds", envInt("L2_ASSOCIATION_WINDOW_SECONDS", 300)),
                getInt(p, "cep-max-sequence-seconds", envInt("L2_CEP_MAX_SEQUENCE_SECONDS", 604800)),
                Boolean.parseBoolean(get(p, "print-results", env("L2_PRINT_RESULTS", "true"))));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) throw new IllegalArgumentException("Unexpected argument: " + arg);
            String key = arg.substring(2); int equals = key.indexOf('=');
            if (equals >= 0) values.put(key.substring(0, equals), key.substring(equals + 1));
            else if (i + 1 < args.length && !args[i + 1].startsWith("--")) values.put(key, args[++i]);
            else values.put(key, "true");
        }
        return values;
    }
    private static String get(Map<String, String> p, String key, String fallback) { return p.getOrDefault(key, fallback); }
    private static double getDouble(Map<String, String> p, String key, double fallback) { return Double.parseDouble(get(p, key, Double.toString(fallback))); }
    private static int getInt(Map<String, String> p, String key, int fallback) { return Integer.parseInt(get(p, key, Integer.toString(fallback))); }

    private static String env(String name, String fallback) {
        String value = System.getenv(name); return value == null || value.isBlank() ? fallback : value;
    }
    private static double envDouble(String name, double fallback) { return Double.parseDouble(env(name, Double.toString(fallback))); }
    private static int envInt(String name, int fallback) { return Integer.parseInt(env(name, Integer.toString(fallback))); }
}
