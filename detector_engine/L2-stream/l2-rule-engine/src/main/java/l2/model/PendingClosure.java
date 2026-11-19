package l2.model;

import java.io.Serializable;

/** CEP sequence boundary retained long enough to match a late-arriving Drools candidate. */
public class PendingClosure implements Serializable {
    private DetectionCandidate scope;
    private long releaseAtMillis;

    public PendingClosure() {}
    public PendingClosure(DetectionCandidate scope, long releaseAtMillis) {
        this.scope = scope;
        this.releaseAtMillis = releaseAtMillis;
    }
    public DetectionCandidate getScope() { return scope; }
    public void setScope(DetectionCandidate value) { scope = value; }
    public long getReleaseAtMillis() { return releaseAtMillis; }
    public void setReleaseAtMillis(long value) { releaseAtMillis = value; }
}
