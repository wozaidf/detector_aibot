// 窗口评估结果封装：原始 L1 窗口加上 Drools 规则打上的异常标记。
package l2.model;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Internal L2 evaluation envelope. The original L1 JSON/model remains unchanged. */
public class WindowEvaluation implements Serializable {
    private AlignedWindow window;
    private Set<String> anomalyMarkers = new LinkedHashSet<>();
    private List<String> droolsCandidateEventIds = new ArrayList<>();
    private List<String> droolsCandidateTypes = new ArrayList<>();
    private List<String> matchedRules = new ArrayList<>();

    public WindowEvaluation() {}
    public WindowEvaluation(AlignedWindow window, Set<String> anomalyMarkers) {
        this.window = window;
        this.anomalyMarkers = new LinkedHashSet<>(anomalyMarkers);
    }
    public AlignedWindow getWindow() { return window; }
    public void setWindow(AlignedWindow value) { window = value; }
    public Set<String> getAnomalyMarkers() { return anomalyMarkers; }
    public void setAnomalyMarkers(Set<String> value) { anomalyMarkers = value; }
    public boolean hasMarker(String marker) { return anomalyMarkers != null && anomalyMarkers.contains(marker); }
    public List<String> getDroolsCandidateEventIds() { return droolsCandidateEventIds; }
    public void setDroolsCandidateEventIds(List<String> value) { droolsCandidateEventIds = value; }
    public List<String> getDroolsCandidateTypes() { return droolsCandidateTypes; }
    public void setDroolsCandidateTypes(List<String> value) { droolsCandidateTypes = value; }
    public List<String> getMatchedRules() { return matchedRules; }
    public void setMatchedRules(List<String> value) { matchedRules = value; }
}
