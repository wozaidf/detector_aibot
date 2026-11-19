// L1 对齐窗口 JSON 解析器：校验结构并转换为 AlignedWindow，同时计算内容 SHA-256。
package l2.source;

import l2.model.AlignedWindow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class AlignedWindowParser implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> REQUIRED_OBJECTS = List.of("/window", "/events", "/features", "/context", "/quality", "/fusion_features");

    public AlignedWindow parse(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        requireText(root, "/schema_version");
        requireText(root, "/window_id");
        if (!"aligned_window".equals(requireText(root, "/record_type"))) throw new IllegalArgumentException("record_type must be aligned_window");
        requireText(root, "/entity_id"); requireText(root, "/entity_type");
        for (String pointer : REQUIRED_OBJECTS) if (!root.at(pointer).isObject()) throw new IllegalArgumentException(pointer + " must be an object");
        for (String pointer : List.of("/events/metrics", "/events/logs", "/events/traces", "/events/alarms")) if (!root.at(pointer).isArray()) throw new IllegalArgumentException(pointer + " must be an array");

        String end = requireText(root, "/window/end");
        String start = requireText(root, "/window/start");
        if (!root.at("/window/size_sec").canConvertToInt() || !root.at("/window/slide_sec").canConvertToInt()) throw new IllegalArgumentException("window size_sec and slide_sec must be integers");
        if (!Instant.parse(start).isBefore(Instant.parse(end))) throw new IllegalArgumentException("window start must precede end");

        AlignedWindow w = new AlignedWindow();
        w.setSchemaVersion(root.at("/schema_version").asText()); w.setWindowId(root.at("/window_id").asText());
        w.setEntityId(root.at("/entity_id").asText()); w.setEntityType(root.at("/entity_type").asText());
        w.setWindowStart(start); w.setWindowEnd(end); w.setWindowEndEpochMillis(Instant.parse(end).toEpochMilli());
        w.setWindowSlideSeconds(root.at("/window/slide_sec").asInt());
        w.setCpuMeanPct(number(root, "/features/metrics/cpu_mean_pct")); w.setCpuMaxPct(number(root, "/features/metrics/cpu_max_pct"));
        w.setMemoryMeanPct(number(root, "/features/metrics/memory_mean_pct")); w.setLatencyMeanMs(number(root, "/features/metrics/latency_mean_ms"));
        w.setPacketLossMeanRatio(number(root, "/features/metrics/packet_loss_mean_ratio"));
        w.setErrorCount(integer(root, "/features/logs/error_count")); w.setErrorRate(number(root, "/features/logs/error_rate"));
        w.setFailedSpanCount(integer(root, "/features/traces/failed_span_count")); w.setSpanFailureRate(number(root, "/features/traces/span_error_rate"));
        w.setAlarmCount(integer(root, "/features/alarms/alarm_count")); w.setCriticalAlarmCount(integer(root, "/features/alarms/critical_count"));
        w.setOverlapCount(integer(root, "/fusion_features/key_level/relation_statistics/error_failed_trace_overlap_count"));
        w.setMaintenanceActive(root.at("/context/maintenance_active").asBoolean(false));
        w.setQualityStatus(requireText(root, "/quality/status"));
        List<String> missing = new ArrayList<>(); root.at("/quality/missing_features").forEach(n -> missing.add(n.asText())); w.setMissingFeatures(missing);
        w.setRawJson(json); w.setContentSha256(sha256(json)); return w;
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        return java.util.HexFormat.of().formatHex(digest);
    }

    private static String requireText(JsonNode root, String pointer) {
        JsonNode n = root.at(pointer); if (!n.isTextual() || n.asText().isBlank()) throw new IllegalArgumentException(pointer + " must be non-empty text"); return n.asText();
    }
    private static double number(JsonNode root, String pointer) { JsonNode n = root.at(pointer); return n.isNumber() ? n.asDouble() : Double.NaN; }
    private static int integer(JsonNode root, String pointer) { JsonNode n = root.at(pointer); return n.canConvertToInt() ? n.asInt() : 0; }
}
