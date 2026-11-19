// Drools 规则使用的指标阈值集合，作为事实对象插入规则引擎。
package l2.config;

import java.io.Serializable;

public class RuleThresholds implements Serializable {
    private double cpuHighPct;
    private double memoryHighPct;
    private double latencyHighMs;
    private double packetLossHighRatio;
    private double logErrorRate;
    private double spanFailureRate;
    private double cpuSeverePct;
    private double memorySeverePct;
    private double latencySevereMs;
    private double packetLossSevereRatio;
    private double logErrorSevereRate;
    private double spanFailureSevereRate;
    private double jointNetworkLatencyMs;
    private double jointNetworkPacketLossRatio;
    private double jointLogErrorRate;
    private double jointSpanFailureRate;
    private int jointLogTraceMinOverlapCount;
    private double jointMultiCpuPct;
    private int jointMultiMinFailedSpanCount;
    private int jointMultiMinAlarmCount;
    private int repeatedErrorCount;

    public RuleThresholds() {}
    public RuleThresholds(AppConfig c) {
        cpuHighPct = c.cpuHighPct(); memoryHighPct = c.memoryHighPct();
        latencyHighMs = c.latencyHighMs(); packetLossHighRatio = c.packetLossHighRatio();
        logErrorRate = c.logErrorRate(); spanFailureRate = c.spanFailureRate();
        cpuSeverePct = c.cpuSeverePct(); memorySeverePct = c.memorySeverePct();
        latencySevereMs = c.latencySevereMs(); packetLossSevereRatio = c.packetLossSevereRatio();
        logErrorSevereRate = c.logErrorSevereRate(); spanFailureSevereRate = c.spanFailureSevereRate();
        jointNetworkLatencyMs = c.jointNetworkLatencyMs(); jointNetworkPacketLossRatio = c.jointNetworkPacketLossRatio();
        jointLogErrorRate = c.jointLogErrorRate(); jointSpanFailureRate = c.jointSpanFailureRate();
        jointLogTraceMinOverlapCount = c.jointLogTraceMinOverlapCount(); jointMultiCpuPct = c.jointMultiCpuPct();
        jointMultiMinFailedSpanCount = c.jointMultiMinFailedSpanCount(); jointMultiMinAlarmCount = c.jointMultiMinAlarmCount();
        repeatedErrorCount = c.repeatedErrorCount();
    }
    public double getCpuHighPct() { return cpuHighPct; }
    public double getMemoryHighPct() { return memoryHighPct; }
    public double getLatencyHighMs() { return latencyHighMs; }
    public double getPacketLossHighRatio() { return packetLossHighRatio; }
    public double getLogErrorRate() { return logErrorRate; }
    public double getSpanFailureRate() { return spanFailureRate; }
    public double getCpuSeverePct() { return cpuSeverePct; }
    public double getMemorySeverePct() { return memorySeverePct; }
    public double getLatencySevereMs() { return latencySevereMs; }
    public double getPacketLossSevereRatio() { return packetLossSevereRatio; }
    public double getLogErrorSevereRate() { return logErrorSevereRate; }
    public double getSpanFailureSevereRate() { return spanFailureSevereRate; }
    public double getJointNetworkLatencyMs() { return jointNetworkLatencyMs; }
    public double getJointNetworkPacketLossRatio() { return jointNetworkPacketLossRatio; }
    public double getJointLogErrorRate() { return jointLogErrorRate; }
    public double getJointSpanFailureRate() { return jointSpanFailureRate; }
    public int getJointLogTraceMinOverlapCount() { return jointLogTraceMinOverlapCount; }
    public double getJointMultiCpuPct() { return jointMultiCpuPct; }
    public int getJointMultiMinFailedSpanCount() { return jointMultiMinFailedSpanCount; }
    public int getJointMultiMinAlarmCount() { return jointMultiMinAlarmCount; }
    public int getRepeatedErrorCount() { return repeatedErrorCount; }
}
