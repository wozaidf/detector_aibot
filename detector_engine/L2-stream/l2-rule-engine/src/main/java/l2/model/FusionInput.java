package l2.model;

import java.io.Serializable;
import java.util.List;

/** Internal tagged input to the L2 final fusion operator. */
public class FusionInput implements Serializable {
    private DetectionCandidate candidate;
    private DetectionCandidate closedSequence;
    private long closeTimeMillis;
    private boolean sequenceClosure;

    public FusionInput() {}

    public static FusionInput candidate(DetectionCandidate candidate) {
        FusionInput input = new FusionInput();
        input.candidate = candidate;
        return input;
    }

    public static FusionInput sequenceClosed(DetectionCandidate candidate, String entityId, String anomalyType,
            String startTime, String endTime, List<String> windowIds, long closeTimeMillis) {
        FusionInput input = new FusionInput();
        input.candidate = candidate;
        DetectionCandidate scope = new DetectionCandidate();
        scope.setEntityId(entityId);
        scope.setAnomalyType(anomalyType);
        scope.setStartTime(startTime);
        scope.setEndTime(endTime);
        scope.setMatchedWindowIds(windowIds);
        input.closedSequence = scope;
        input.closeTimeMillis = closeTimeMillis;
        input.sequenceClosure = true;
        return input;
    }

    public DetectionCandidate getCandidate() { return candidate; }
    public void setCandidate(DetectionCandidate value) { candidate = value; }
    public DetectionCandidate getClosedSequence() { return closedSequence; }
    public void setClosedSequence(DetectionCandidate value) { closedSequence = value; }
    public long getCloseTimeMillis() { return closeTimeMillis; }
    public void setCloseTimeMillis(long value) { closeTimeMillis = value; }
    public boolean isSequenceClosure() { return sequenceClosure; }
    public void setSequenceClosure(boolean value) { sequenceClosure = value; }
}
