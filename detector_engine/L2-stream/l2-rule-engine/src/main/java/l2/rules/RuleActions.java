// Drools 规则动作工具类：将命中的窗口指标转换为检测候选事件并补充证据。
package l2.rules;

import l2.model.AlignedWindow;
import l2.model.DetectionCandidate;
import l2.contract.WindowSnapshotSupport;

import java.util.List;
import java.util.Map;

public final class RuleActions {
    private RuleActions() {}
    public static String mark(String marker) { return marker; }
    public static DetectionCandidate single(AlignedWindow w, String type, String rule, String severity) {
        DetectionCandidate c = new DetectionCandidate(); c.setEntityId(w.getEntityId()); c.setAnomalyType(type); c.setSeverity(severity);
        c.setStartTime(w.getWindowStart()); c.setEndTime(w.getWindowEnd()); c.setMaintenanceActive(w.isMaintenanceActive()); c.setQualityStatus(w.getQualityStatus());
        c.setDetectionSources(List.of("DROOLS_WINDOW")); c.setMatchedRules(List.of(rule)); c.setMatchedWindowIds(List.of(w.getWindowId()));
        c.setSourceRefs(List.of(new DetectionCandidate.SourceRef("l1-aligned-windows", w.getWindowId(), w.getContentSha256())));
        c.setQualityStatus(w.getQualityStatus());
        switch (rule) {
            case "WINDOW_CPU_SEVERE" -> c.getEvidence().put("cpu_mean_pct", w.getCpuMeanPct());
            case "WINDOW_MEMORY_SEVERE" -> c.getEvidence().put("memory_mean_pct", w.getMemoryMeanPct());
            case "WINDOW_LATENCY_SEVERE" -> c.getEvidence().put("latency_mean_ms", w.getLatencyMeanMs());
            case "WINDOW_PACKET_LOSS_SEVERE" -> c.getEvidence().put("packet_loss_mean_ratio", w.getPacketLossMeanRatio());
            case "WINDOW_LOG_ERROR_SEVERE" -> c.getEvidence().put("error_rate", w.getErrorRate());
            case "WINDOW_SPAN_FAILURE_SEVERE" -> c.getEvidence().put("span_failure_rate", w.getSpanFailureRate());
            case "WINDOW_NETWORK_DEGRADATION" -> {
                c.getEvidence().put("latency_mean_ms", w.getLatencyMeanMs());
                c.getEvidence().put("packet_loss_mean_ratio", w.getPacketLossMeanRatio());
            }
            case "WINDOW_LOG_TRACE_CORRELATION" -> {
                c.getEvidence().put("error_rate", w.getErrorRate());
                c.getEvidence().put("span_failure_rate", w.getSpanFailureRate());
                c.getEvidence().put("trace_overlap_count", w.getOverlapCount());
            }
            case "WINDOW_MULTI_SIGNAL_ALARM" -> {
                c.getEvidence().put("cpu_mean_pct", w.getCpuMeanPct());
                c.getEvidence().put("failed_span_count", w.getFailedSpanCount());
                c.getEvidence().put("alarm_count", w.getAlarmCount());
            }
            default -> c.getEvidence().put("window_id", w.getWindowId());
        }
        c.getEvidence().put("window_id", w.getWindowId());
        c.getSourceEvidence().put("DROOLS_WINDOW", Map.copyOf(c.getEvidence()));
        c.setSourceEventIds(List.of(CandidateFusion.sourceEventId(c)));
        WindowSnapshotSupport.attach(c, List.of(w));
        return c;
    }
}
