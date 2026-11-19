package l2.model;

import java.io.Serializable;

/** Candidate retained until its event-time release deadline. */
public class PendingCandidate implements Serializable {
    private DetectionCandidate candidate;
    private long releaseAtMillis;

    public PendingCandidate() {}
    public PendingCandidate(DetectionCandidate candidate, long releaseAtMillis) {
        this.candidate = candidate;
        this.releaseAtMillis = releaseAtMillis;
    }
    public DetectionCandidate getCandidate() { return candidate; }
    public void setCandidate(DetectionCandidate value) { candidate = value; }
    public long getReleaseAtMillis() { return releaseAtMillis; }
    public void setReleaseAtMillis(long value) { releaseAtMillis = value; }
}
