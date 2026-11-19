// Kafka 输出的统一异常事件；检测结果字段嵌套在 l2_result 中。
package l2.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record AnomalyEvent(
        String eventId,
        String eventType,
        String stage,
        String entityId,
        String entityType,
        String createdAt,
        String updatedAt,
        OriginalData originalData,
        L2Result l2Result,
        Object l25Result,
        Object l4Result) implements Serializable {

    public record OriginalData(
            String featureSchemaVersion,
            List<DetectionCandidate.SourceRef> sourceRefs,
            List<WindowSnapshot> windowSnapshots) implements Serializable {}

    public record L2Result(
            boolean anomaly,
            String anomalyType,
            String severity,
            String startTime,
            String endTime,
            String detectedAt,
            List<String> detectionSources,
            List<String> matchedRules,
            List<String> matchedWindowIds,
            List<String> sourceEventIds,
            Map<String, Object> evidence,
            Map<String, Object> sourceEvidence,
            String mergeStatus,
            String decisionReason,
            Decision decision,
            int windowCount,
            String qualityStatus,
            PayloadQuality payloadQuality) implements Serializable {}

    public record Decision(boolean anomaly, boolean suppressed, String action, String reason) implements Serializable {}
}
