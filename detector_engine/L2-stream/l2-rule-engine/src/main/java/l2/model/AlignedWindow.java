// L1 对齐窗口数据模型，承载实体、时间窗口、指标、日志、链路、告警及质量等特征。
package l2.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AlignedWindow implements Serializable {
    private String schemaVersion;
    private String windowId;
    private String entityId;
    private String entityType;
    private String windowStart;
    private String windowEnd;
    private long windowEndEpochMillis;
    private int windowSlideSeconds;
    private double cpuMeanPct;
    private double cpuMaxPct;
    private double memoryMeanPct;
    private double latencyMeanMs;
    private double packetLossMeanRatio;
    private int errorCount;
    private double errorRate;
    private int failedSpanCount;
    private double spanFailureRate;
    private int alarmCount;
    private int criticalAlarmCount;
    private int overlapCount;
    private boolean maintenanceActive;
    private String qualityStatus;
    private List<String> missingFeatures = new ArrayList<>();
    private String rawJson;
    private String contentSha256;

    public AlignedWindow() {}
    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String v) { schemaVersion = v; }
    public String getWindowId() { return windowId; }
    public void setWindowId(String v) { windowId = v; }
    public String getEntityId() { return entityId; }
    public void setEntityId(String v) { entityId = v; }
    public String getEntityType() { return entityType; }
    public void setEntityType(String v) { entityType = v; }
    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String v) { windowStart = v; }
    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String v) { windowEnd = v; }
    public long getWindowEndEpochMillis() { return windowEndEpochMillis; }
    public void setWindowEndEpochMillis(long v) { windowEndEpochMillis = v; }
    public int getWindowSlideSeconds() { return windowSlideSeconds; }
    public void setWindowSlideSeconds(int v) { windowSlideSeconds = v; }
    public double getCpuMeanPct() { return cpuMeanPct; }
    public void setCpuMeanPct(double v) { cpuMeanPct = v; }
    public double getCpuMaxPct() { return cpuMaxPct; }
    public void setCpuMaxPct(double v) { cpuMaxPct = v; }
    public double getMemoryMeanPct() { return memoryMeanPct; }
    public void setMemoryMeanPct(double v) { memoryMeanPct = v; }
    public double getLatencyMeanMs() { return latencyMeanMs; }
    public void setLatencyMeanMs(double v) { latencyMeanMs = v; }
    public double getPacketLossMeanRatio() { return packetLossMeanRatio; }
    public void setPacketLossMeanRatio(double v) { packetLossMeanRatio = v; }
    public int getErrorCount() { return errorCount; }
    public void setErrorCount(int v) { errorCount = v; }
    public double getErrorRate() { return errorRate; }
    public void setErrorRate(double v) { errorRate = v; }
    public int getFailedSpanCount() { return failedSpanCount; }
    public void setFailedSpanCount(int v) { failedSpanCount = v; }
    public double getSpanFailureRate() { return spanFailureRate; }
    public void setSpanFailureRate(double v) { spanFailureRate = v; }
    public int getAlarmCount() { return alarmCount; }
    public void setAlarmCount(int v) { alarmCount = v; }
    public int getCriticalAlarmCount() { return criticalAlarmCount; }
    public void setCriticalAlarmCount(int v) { criticalAlarmCount = v; }
    public int getOverlapCount() { return overlapCount; }
    public void setOverlapCount(int v) { overlapCount = v; }
    public boolean isMaintenanceActive() { return maintenanceActive; }
    public void setMaintenanceActive(boolean v) { maintenanceActive = v; }
    public String getQualityStatus() { return qualityStatus; }
    public void setQualityStatus(String v) { qualityStatus = v; }
    public List<String> getMissingFeatures() { return missingFeatures; }
    public void setMissingFeatures(List<String> v) { missingFeatures = v; }
    public String getRawJson() { return rawJson; }
    public void setRawJson(String v) { rawJson = v; }
    public String getContentSha256() { return contentSha256; }
    public void setContentSha256(String v) { contentSha256 = v; }
}
