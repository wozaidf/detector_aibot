// 最终决策算子：按 CEP 序列封口信号和事件时间迟到余量融合两路候选。
package l2.rules;

import l2.model.AnomalyEvent;
import l2.model.DetectionCandidate;
import l2.model.FusionInput;
import l2.model.PendingCandidate;
import l2.model.PendingClosure;
import l2.contract.WindowSnapshotSupport;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Holds candidates until their CEP sequence closes and the event-time lateness boundary passes. */
public class FinalDecisionFunction extends KeyedProcessFunction<String, FusionInput, AnomalyEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(FinalDecisionFunction.class);
    private transient KieSession session;
    private transient ListState<PendingCandidate> pending;
    private transient ListState<PendingClosure> closures;
    private transient MapState<String, Long> emitted;
    private transient ValueState<Long> nextTimer;
    private final int dedupSeconds;
    private final int associationWindowSeconds;
    private final long allowedLatenessMillis;
    private final long maxSequenceMillis;

    public FinalDecisionFunction(int dedupSeconds, int associationWindowSeconds, int allowedLatenessSeconds, int maxSequenceSeconds) {
        this.dedupSeconds = dedupSeconds;
        this.associationWindowSeconds = associationWindowSeconds;
        this.allowedLatenessMillis = Math.max(0, allowedLatenessSeconds) * 1000L;
        this.maxSequenceMillis = Math.max(1, maxSequenceSeconds) * 1000L;
    }

    @Override public void open(OpenContext context) {
        session = DroolsSupport.build("rules/decision-rules.drl").newKieSession();
        pending = getRuntimeContext().getListState(new ListStateDescriptor<>("pending-fusion-candidates-v2", PendingCandidate.class));
        closures = getRuntimeContext().getListState(new ListStateDescriptor<>("pending-cep-sequence-closures-v2", PendingClosure.class));
        emitted = getRuntimeContext().getMapState(new MapStateDescriptor<>("emitted-final-event-ids", String.class, Long.class));
        nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("next-fusion-event-time-timer", Long.class));
    }

    @Override public void processElement(FusionInput input, Context ctx, Collector<AnomalyEvent> out) throws Exception {
        if (input == null) { LOG.error("Rejected null fusion input"); return; }

        // Store the sequence boundary before the candidate: the same CEP record carries both.
        if (input.isSequenceClosure()) {
            DetectionCandidate scope = input.getClosedSequence();
            long releaseAt = safeAdd(input.getCloseTimeMillis(), allowedLatenessMillis);
            List<PendingClosure> closureState = readClosures();
            closureState.add(new PendingClosure(scope, releaseAt));
            closures.update(closureState);
            releaseCandidatesMatchedBy(scope, releaseAt);
            registerTimer(releaseAt, ctx);
        }

        DetectionCandidate candidate = input.getCandidate();
        if (candidate == null) return;
        if (!valid(candidate)) { LOG.error("Rejected invalid detection candidate: {}", candidate); return; }
        applyExistingDecision(candidate);

        String sourceId = CandidateFusion.sourceEventId(candidate);
        List<PendingCandidate> current = readPending();
        if (current.stream().anyMatch(p -> sourceId.equals(CandidateFusion.sourceEventId(p.getCandidate())))) {
            for (PendingCandidate item : current) if (sourceId.equals(CandidateFusion.sourceEventId(item.getCandidate())))
                WindowSnapshotSupport.mergeInto(item.getCandidate(), candidate);
            pending.update(current);
            LOG.warn("Ignored replayed pending source candidate {}", sourceId);
            return;
        }

        long end = parseEnd(candidate);
        long releaseAt = safeAdd(end, allowedLatenessMillis);
        if (candidate.isAwaitCepClosure()) releaseAt = safeAdd(releaseAt, maxSequenceMillis);
        for (PendingClosure closure : readClosures()) {
            if (CandidateFusion.shouldAssociate(candidate, closure.getScope(), associationWindowSeconds))
                releaseAt = Math.min(releaseAt, Math.max(safeAdd(end, allowedLatenessMillis), closure.getReleaseAtMillis()));
        }
        current.add(new PendingCandidate(candidate, releaseAt));
        pending.update(current);
        registerTimer(releaseAt, ctx);
    }

    private void releaseCandidatesMatchedBy(DetectionCandidate scope, long releaseAt) throws Exception {
        List<PendingCandidate> updated = readPending();
        for (PendingCandidate item : updated) {
            DetectionCandidate candidate = item.getCandidate();
            if (candidate.isAwaitCepClosure() && CandidateFusion.shouldAssociate(candidate, scope, associationWindowSeconds))
                item.setReleaseAtMillis(Math.min(item.getReleaseAtMillis(),
                        Math.max(safeAdd(parseEnd(candidate), allowedLatenessMillis), releaseAt)));
        }
        pending.update(updated);
    }

    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<AnomalyEvent> out) throws Exception {
        List<PendingCandidate> ready = new ArrayList<>(), future = new ArrayList<>();
        for (PendingCandidate item : readPending()) {
            if (item.getReleaseAtMillis() <= timestamp) ready.add(item); else future.add(item);
        }
        pending.update(future);

        List<PendingClosure> remainingClosures = new ArrayList<>();
        for (PendingClosure closure : readClosures()) if (closure.getReleaseAtMillis() > timestamp) remainingClosures.add(closure);
        closures.update(remainingClosures);

        ready.sort(Comparator.comparing(item -> CandidateFusion.sourceEventId(item.getCandidate())));
        List<DetectionCandidate> batch = new ArrayList<>();
        var seenInBatch = new HashSet<String>();
        for (PendingCandidate item : ready) {
            DetectionCandidate candidate = item.getCandidate();
            String id = CandidateFusion.sourceEventId(candidate);
            if (seenInBatch.add(id)) batch.add(candidate);
            else {
                for (DetectionCandidate kept : batch) if (id.equals(CandidateFusion.sourceEventId(kept))) WindowSnapshotSupport.mergeInto(kept, candidate);
                LOG.warn("Collapsed duplicate source candidate {} in event-time batch", id);
            }
        }
        for (DetectionCandidate candidate : CandidateFusion.mergeAll(batch, associationWindowSeconds)) emitOnce(candidate, out);

        long earliest = Long.MAX_VALUE;
        for (PendingCandidate item : future) earliest = Math.min(earliest, item.getReleaseAtMillis());
        for (PendingClosure closure : remainingClosures) earliest = Math.min(earliest, closure.getReleaseAtMillis());
        if (earliest != Long.MAX_VALUE) {
            nextTimer.update(earliest);
            ctx.timerService().registerEventTimeTimer(earliest);
        } else nextTimer.clear();
    }

    private void registerTimer(long timestamp, Context ctx) throws Exception {
        Long existing = nextTimer.value();
        if (existing == null || timestamp < existing) {
            nextTimer.update(timestamp);
            ctx.timerService().registerEventTimeTimer(timestamp);
        }
    }

    private List<PendingCandidate> readPending() throws Exception {
        List<PendingCandidate> result = new ArrayList<>(); pending.get().forEach(result::add); return result;
    }
    private List<PendingClosure> readClosures() throws Exception {
        List<PendingClosure> result = new ArrayList<>(); closures.get().forEach(result::add); return result;
    }
    private static boolean valid(DetectionCandidate c) {
        if (c.getEntityId() == null || c.getEntityId().isBlank() || c.getAnomalyType() == null
                || c.getAnomalyType().isBlank() || c.getStartTime() == null || c.getEndTime() == null) return false;
        try { return !Instant.parse(c.getStartTime()).isAfter(Instant.parse(c.getEndTime())); }
        catch (RuntimeException e) { return false; }
    }
    private static long parseEnd(DetectionCandidate c) {
        return Instant.parse(c.getEndTime()).toEpochMilli();
    }
    private static long safeAdd(long a, long b) { return a > Long.MAX_VALUE - b ? Long.MAX_VALUE : a + b; }

    private void applyExistingDecision(DetectionCandidate candidate) {
        var handle = session.insert(candidate); session.fireAllRules(); session.delete(handle);
    }

    private void emitOnce(DetectionCandidate c, Collector<AnomalyEvent> out) throws Exception {
        List<String> sourceIds = c.getSourceEventIds() == null || c.getSourceEventIds().isEmpty()
                ? List.of(CandidateFusion.sourceEventId(c)) : c.getSourceEventIds().stream().distinct().sorted().toList();
        List<String> sources = c.getDetectionSources() == null ? List.of() : c.getDetectionSources().stream().distinct().sorted().toList();
        List<String> windows = c.getMatchedWindowIds() == null ? List.of() : c.getMatchedWindowIds().stream().distinct().sorted().toList();
        String eventId = CandidateFusion.stableFinalEventId(c);
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : iterable(emitted)) if (entry.getValue() < now) emitted.remove(entry.getKey());
        Long expires = emitted.get(eventId);
        if (expires != null && expires >= now) {
            LOG.warn("Suppressed replay of final L2 event {} (stable event_id); dedup window={}s", eventId, dedupSeconds); return;
        }
        emitted.put(eventId, now + dedupSeconds * 1000L);

        // Suppressed candidates remain detectable anomaly events in Kafka; suppression is
        // retained as a separate decision field so downstream consumers can filter them.
        boolean anomaly = true;
        boolean corroborated = sources.contains("DROOLS_WINDOW") && sources.contains("FLINK_CEP");
        String action = c.isSuppressed() ? "SUPPRESSED" : (corroborated ? "CORROBORATED" : "ALERT");
        String reason = c.isSuppressed() ? "maintenance window suppression"
                : (corroborated ? "Drools and CEP candidates matched entity, anomaly type, window ID, and overlapping time range"
                : "single detection branch candidate retained from " + String.join(",", sources));
        String mergeStatus = corroborated ? "MERGED_DROOLS_CEP" : "SINGLE_SOURCE";
        String quality = c.getQualityStatus() == null ? "unknown" : c.getQualityStatus();
        var payload = WindowSnapshotSupport.prepare(c);
        String outputTime = Instant.now().toString();
        out.collect(new AnomalyEvent(eventId, "ANOMALY_EVENT", "L2", c.getEntityId(), payload.entityType(),
                outputTime, outputTime,
                new AnomalyEvent.OriginalData(WindowSnapshotSupport.FEATURE_SCHEMA_VERSION,
                        c.getSourceRefs() == null ? List.of() : c.getSourceRefs(), payload.snapshots()),
                new AnomalyEvent.L2Result(anomaly, c.getAnomalyType(), c.getSeverity(), c.getStartTime(), c.getEndTime(),
                        Instant.now().toString(), sources,
                        c.getMatchedRules() == null ? List.of() : c.getMatchedRules(), windows, sourceIds,
                        c.getEvidence() == null ? Map.of() : c.getEvidence(),
                        c.getSourceEvidence() == null ? Map.of() : c.getSourceEvidence(),
                        mergeStatus, reason, new AnomalyEvent.Decision(anomaly, c.isSuppressed(), action, reason),
                        payload.windowCount(), quality, payload.quality()),
                null, null));
    }

    private static Iterable<Map.Entry<String, Long>> iterable(MapState<String, Long> state) {
        try { List<Map.Entry<String, Long>> copy = new ArrayList<>(); for (Map.Entry<String, Long> e : state.entries()) copy.add(Map.entry(e.getKey(), e.getValue())); return copy; }
        catch (Exception e) { return List.of(); }
    }
    @Override public void close() { if (session != null) session.dispose(); }
}
