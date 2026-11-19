package l2.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** 原窗口的裁剪快照；特征保持原值，不保存原始事件大数组。 */
public class WindowSnapshot implements Serializable {
    private String schemaVersion;
    private String recordType;
    private String windowId;
    private String entityId;
    private String entityType;
    private Map<String, Object> window = new LinkedHashMap<>();
    private Map<String, Object> features = new LinkedHashMap<>();
    private Map<String, Object> quality = new LinkedHashMap<>();
    private Map<String, Object> context = new LinkedHashMap<>();
    private Map<String, Object> relationStatistics = new LinkedHashMap<>();
    private DetectionCandidate.SourceRef sourceRef;

    public WindowSnapshot() {}
    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String v) { schemaVersion = v; }
    public String getRecordType() { return recordType; }
    public void setRecordType(String v) { recordType = v; }
    public String getWindowId() { return windowId; }
    public void setWindowId(String v) { windowId = v; }
    public String getEntityId() { return entityId; }
    public void setEntityId(String v) { entityId = v; }
    public String getEntityType() { return entityType; }
    public void setEntityType(String v) { entityType = v; }
    public Map<String, Object> getWindow() { return window; }
    public void setWindow(Map<String, Object> v) { window = v; }
    public Map<String, Object> getFeatures() { return features; }
    public void setFeatures(Map<String, Object> v) { features = v; }
    public Map<String, Object> getQuality() { return quality; }
    public void setQuality(Map<String, Object> v) { quality = v; }
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> v) { context = v; }
    public Map<String, Object> getRelationStatistics() { return relationStatistics; }
    public void setRelationStatistics(Map<String, Object> v) { relationStatistics = v; }
    public DetectionCandidate.SourceRef getSourceRef() { return sourceRef; }
    public void setSourceRef(DetectionCandidate.SourceRef v) { sourceRef = v; }
}
