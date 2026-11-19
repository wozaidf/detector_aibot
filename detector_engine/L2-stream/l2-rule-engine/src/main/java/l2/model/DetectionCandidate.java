// 检测候选事件模型，用于承载 Drools 与 CEP 两路检测结果并支持关联融合。
package l2.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DetectionCandidate implements Serializable {
    private String entityId;
    private String anomalyType;
    private String severity = "warning";
    private String startTime;
    private String endTime;
    private boolean maintenanceActive;
    private String qualityStatus;
    private List<String> detectionSources = new ArrayList<>();
    private List<String> matchedRules = new ArrayList<>();
    private List<String> matchedWindowIds = new ArrayList<>();
    private List<SourceRef> sourceRefs = new ArrayList<>();
    private Map<String, Object> evidence = new LinkedHashMap<>();
    private Map<String, Object> sourceEvidence = new LinkedHashMap<>();
    private List<String> sourceEventIds = new ArrayList<>();
    private List<WindowSnapshot> windowSnapshots = new ArrayList<>();
    private List<Map<String, String>> snapshotIssues = new ArrayList<>();
    private String decision = "ALERT";
    private boolean suppressed;
    private boolean duplicate;
    /** True only for a Drools candidate whose marked window may belong to a CEP CPU/memory sequence. */
    private boolean awaitCepClosure;

    public DetectionCandidate() {}
    public String getEntityId() { return entityId; }
    public void setEntityId(String v) { entityId = v; }
    public String getAnomalyType() { return anomalyType; }
    public void setAnomalyType(String v) { anomalyType = v; }
    public String getSeverity() { return severity; }
    public void setSeverity(String v) { severity = v; }
    public String getStartTime() { return startTime; }
    public void setStartTime(String v) { startTime = v; }
    public String getEndTime() { return endTime; }
    public void setEndTime(String v) { endTime = v; }
    public boolean isMaintenanceActive() { return maintenanceActive; }
    public void setMaintenanceActive(boolean v) { maintenanceActive = v; }
    public String getQualityStatus() { return qualityStatus; }
    public void setQualityStatus(String v) { qualityStatus = v; }
    public List<String> getDetectionSources() { return detectionSources; }
    public void setDetectionSources(List<String> v) { detectionSources = v; }
    public List<String> getMatchedRules() { return matchedRules; }
    public void setMatchedRules(List<String> v) { matchedRules = v; }
    public List<String> getMatchedWindowIds() { return matchedWindowIds; }
    public void setMatchedWindowIds(List<String> v) { matchedWindowIds = v; }
    public List<SourceRef> getSourceRefs() { return sourceRefs; }
    public void setSourceRefs(List<SourceRef> v) { sourceRefs = v; }
    public Map<String, Object> getEvidence() { return evidence; }
    public void setEvidence(Map<String, Object> v) { evidence = v; }
    public Map<String, Object> getSourceEvidence() { return sourceEvidence; }
    public void setSourceEvidence(Map<String, Object> v) { sourceEvidence = v; }
    public List<String> getSourceEventIds() { return sourceEventIds; }
    public void setSourceEventIds(List<String> v) { sourceEventIds = v; }
    public List<WindowSnapshot> getWindowSnapshots() { return windowSnapshots; }
    public void setWindowSnapshots(List<WindowSnapshot> v) { windowSnapshots = v; }
    public List<Map<String, String>> getSnapshotIssues() { return snapshotIssues; }
    public void setSnapshotIssues(List<Map<String, String>> v) { snapshotIssues = v; }
    public String getDecision() { return decision; }
    public void setDecision(String v) { decision = v; }
    public boolean isSuppressed() { return suppressed; }
    public void setSuppressed(boolean v) { suppressed = v; }
    public boolean isDuplicate() { return duplicate; }
    public void setDuplicate(boolean v) { duplicate = v; }
    public boolean isAwaitCepClosure() { return awaitCepClosure; }
    public void setAwaitCepClosure(boolean v) { awaitCepClosure = v; }

    public record SourceRef(String topic, String windowId, String contentSha256) implements Serializable {}
}
