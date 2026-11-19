// 基于 Flink CEP 检测连续异常，并将候选和序列封口信号交给最终融合算子。
package l2.cep;

import l2.config.AppConfig;
import l2.contract.WindowSnapshotSupport;
import l2.model.AlignedWindow;
import l2.model.DetectionCandidate;
import l2.model.FusionInput;
import l2.model.WindowEvaluation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CepDetector {
    private CepDetector() {}

    public static DataStream<FusionInput> detect(KeyedStream<WindowEvaluation, String> keyed, AppConfig c) {
        long maxSequenceMillis = Math.max(1L, c.cepMaxSequenceSeconds()) * 1000L;
        DataStream<FusionInput> cpu = repeating(keyed, "CPU_HIGH", "CPU_HIGH", "CEP_CPU_SUSTAINED", c.cepOccurrences(), maxSequenceMillis);
        DataStream<FusionInput> memory = repeating(keyed, "MEMORY_HIGH", "MEMORY_HIGH", "CEP_MEMORY_SUSTAINED", c.cepOccurrences(), maxSequenceMillis);
        DataStream<FusionInput> errors = repeating(keyed, "REPEATED_ERRORS", "REPEATED_ERRORS", "CEP_REPEATED_ERRORS", c.cepOccurrences(), maxSequenceMillis);

        // Preserve the two-window rule while allowing its event-time horizon to be configured.
        Pattern<WindowEvaluation, WindowEvaluation> complex = Pattern.<WindowEvaluation>begin("degraded")
                .where(new MarkerCondition("CPU_HIGH", "SPAN_FAILURE_RATE_HIGH"))
                .next("alarm").where(new ContinuousAfterCondition("degraded", "ALARM_PRESENT"))
                .within(Duration.ofSeconds(Math.max(1, c.cepComplexHorizonSeconds())));
        DataStream<FusionInput> correlated = process(CEP.pattern(keyed, complex),
                new CandidateSelect("COMPLEX_FAILURE_SEQUENCE", "CEP_COMPLEX_FAILURE_SEQUENCE", 1, maxSequenceMillis));
        return cpu.union(memory, errors, correlated).map(new CepMetricsFunction())
                .name("CEP sequence candidates and closure signals");
    }

    private static final class CepMetricsFunction extends RichMapFunction<FusionInput, FusionInput> {
        private transient Counter matchedSequences;
        private transient Counter matchedWindows;
        private transient Counter closedSequences;

        @Override public void open(OpenContext context) {
            var metrics = getRuntimeContext().getMetricGroup().addGroup("l2").addGroup("cep");
            matchedSequences = metrics.counter("matched_sequences");
            matchedWindows = metrics.counter("matched_windows");
            closedSequences = metrics.counter("closed_sequences");
        }

        @Override public FusionInput map(FusionInput input) {
            if (input.isSequenceClosure()) closedSequences.inc();
            DetectionCandidate candidate = input.getCandidate();
            if (candidate != null) {
                matchedSequences.inc();
                Object count = candidate.getEvidence() == null ? null : candidate.getEvidence().get("window_count");
                if (count instanceof Number n) matchedWindows.inc(n.longValue());
            }
            return input;
        }
    }

    private static DataStream<FusionInput> repeating(KeyedStream<WindowEvaluation, String> keyed,
            String marker, String type, String rule, int occurrences, long maxSequenceMillis) {
        // Greedy maximal run + skipPastLastEvent prevents overlapping sliding-window matches
        // from becoming repeated alerts. An unmarked/noncontinuous window closes the run.
        Pattern<WindowEvaluation, WindowEvaluation> pattern = Pattern.<WindowEvaluation>begin("run", AfterMatchSkipStrategy.skipToFirst("break"))
                .where(new ContinuousMarkerCondition(marker))
                .oneOrMore().consecutive().greedy()
                .next("break").where(new ContinuousBreakCondition("run", marker))
                .within(Duration.ofMillis(maxSequenceMillis));
        PatternStream<WindowEvaluation> stream = CEP.pattern(keyed, pattern);
        return process(stream, new CandidateSelect(type, rule, occurrences, maxSequenceMillis));
    }

    private static DataStream<FusionInput> process(PatternStream<WindowEvaluation> stream, CandidateSelect selector) {
        SingleOutputStreamOperator<FusionInput> matches = stream.process(selector);
        return matches.union(matches.getSideOutput(CandidateSelect.TIMED_OUT));
    }

    private static final class MarkerCondition extends SimpleCondition<WindowEvaluation> {
        private final List<String> markers;
        private MarkerCondition(String... markers) { this.markers = List.of(markers); }
        @Override public boolean filter(WindowEvaluation value) { return markers.stream().allMatch(value::hasMarker); }
    }

    private static final class ContinuousMarkerCondition extends IterativeCondition<WindowEvaluation> {
        private final String marker;
        private ContinuousMarkerCondition(String marker) { this.marker = marker; }
        @Override public boolean filter(WindowEvaluation current, Context<WindowEvaluation> context) throws Exception {
            if (!current.hasMarker(marker)) return false;
            WindowEvaluation previous = lastEvent(context.getEventsForPattern("run"));
            return previous == null || isContinuousWindow(previous.getWindow(), current.getWindow());
        }
    }

    private static final class ContinuousAfterCondition extends IterativeCondition<WindowEvaluation> {
        private final String previousName;
        private final String marker;
        private ContinuousAfterCondition(String previousName, String marker) { this.previousName = previousName; this.marker = marker; }
        @Override public boolean filter(WindowEvaluation current, Context<WindowEvaluation> context) throws Exception {
            if (!current.hasMarker(marker)) return false;
            WindowEvaluation previous = lastEvent(context.getEventsForPattern(previousName));
            return previous != null && isContinuousWindow(previous.getWindow(), current.getWindow());
        }
    }

    private static final class ContinuousBreakCondition extends IterativeCondition<WindowEvaluation> {
        private final String previousName;
        private final String marker;
        private ContinuousBreakCondition(String previousName, String marker) { this.previousName = previousName; this.marker = marker; }
        @Override public boolean filter(WindowEvaluation current, Context<WindowEvaluation> context) throws Exception {
            WindowEvaluation previous = lastEvent(context.getEventsForPattern(previousName));
            if (previous == null) return false;
            return !current.hasMarker(marker) || !isContinuousWindow(previous.getWindow(), current.getWindow());
        }
    }

    private static WindowEvaluation lastEvent(Iterable<WindowEvaluation> events) {
        if (events == null) return null;
        WindowEvaluation last = null;
        for (WindowEvaluation event : events) last = event;
        return last;
    }

    public static boolean isContinuousWindow(AlignedWindow before, AlignedWindow now) {
        if (before == null || now == null || before.getEntityId() == null || !before.getEntityId().equals(now.getEntityId())) return false;
        int slide = now.getWindowSlideSeconds();
        if (slide <= 0 || before.getWindowSlideSeconds() != slide) return false;
        try {
            long delta = java.time.Instant.parse(now.getWindowStart()).toEpochMilli()
                    - java.time.Instant.parse(before.getWindowStart()).toEpochMilli();
            return delta == slide * 1000L && now.getWindowEndEpochMillis() > before.getWindowEndEpochMillis();
        } catch (RuntimeException e) { return false; }
    }

    /** Emits every closed run, including runs shorter than the candidate threshold. */
    public static final class CandidateSelect extends PatternProcessFunction<WindowEvaluation, FusionInput>
            implements TimedOutPartialMatchHandler<WindowEvaluation> {
        private static final OutputTag<FusionInput> TIMED_OUT = new OutputTag<>("cep-sequence-timeouts") {};
        private final String anomalyType;
        private final String rule;
        private final int minimumWindows;
        private final long maxSequenceMillis;

        public CandidateSelect(String anomalyType, String rule) { this(anomalyType, rule, 1, Duration.ofDays(7).toMillis()); }
        public CandidateSelect(String anomalyType, String rule, int minimumWindows) { this(anomalyType, rule, minimumWindows, Duration.ofDays(7).toMillis()); }
        public CandidateSelect(String anomalyType, String rule, int minimumWindows, long maxSequenceMillis) {
            this.anomalyType = anomalyType; this.rule = rule; this.minimumWindows = minimumWindows; this.maxSequenceMillis = maxSequenceMillis;
        }

        @Override public void processMatch(Map<String, List<WindowEvaluation>> pattern, Context context, Collector<FusionInput> out) {
            out.collect(createClosedSequence(pattern, context.timestamp(), true));
        }

        @Override public void processTimedOutMatch(Map<String, List<WindowEvaluation>> pattern, Context context) {
            boolean candidateOnTimeout = !"COMPLEX_FAILURE_SEQUENCE".equals(anomalyType);
            FusionInput signal = createClosedSequence(pattern, context.timestamp(), candidateOnTimeout);
            if (signal != null) context.output(TIMED_OUT, signal);
        }

        private FusionInput createClosedSequence(Map<String, List<WindowEvaluation>> pattern, Long closureTimestamp, boolean allowCandidate) {
            List<WindowEvaluation> run = pattern.containsKey("run")
                    ? pattern.get("run") : pattern.values().stream().flatMap(List::stream).toList();
            List<WindowEvaluation> distinctRun = run == null ? List.of() : run.stream().distinct().toList();
            List<AlignedWindow> windows = distinctRun.stream().map(WindowEvaluation::getWindow).distinct()
                    .sorted(Comparator.comparingLong(AlignedWindow::getWindowEndEpochMillis)).toList();
            if (windows.isEmpty()) return null;

            AlignedWindow first = windows.get(0), last = windows.get(windows.size() - 1);
            DetectionCandidate candidate = !allowCandidate || windows.size() < minimumWindows ? null : createCandidate(pattern, windows, first, last);
            long closeAt = closureTimestamp == null ? last.getWindowEndEpochMillis() : Math.max(last.getWindowEndEpochMillis(), closureTimestamp);
            List<WindowEvaluation> breakWindows = pattern.get("break");
            if (breakWindows != null && !breakWindows.isEmpty()) closeAt = Math.max(closeAt, breakWindows.get(0).getWindow().getWindowEndEpochMillis());
            // Timed-out partial matches close at their configured horizon if CEP supplies no timestamp.
            if (closeAt == last.getWindowEndEpochMillis() && candidate == null && minimumWindows > windows.size()) closeAt += maxSequenceMillis;

            return FusionInput.sequenceClosed(candidate, first.getEntityId(), anomalyType,
                    first.getWindowStart(), last.getWindowEnd(), windows.stream().map(AlignedWindow::getWindowId).toList(), closeAt);
        }

        private DetectionCandidate createCandidate(Map<String, List<WindowEvaluation>> pattern, List<AlignedWindow> windows,
                AlignedWindow first, AlignedWindow last) {
            DetectionCandidate c = new DetectionCandidate(); c.setEntityId(first.getEntityId()); c.setAnomalyType(anomalyType); c.setSeverity("major");
            c.setStartTime(first.getWindowStart()); c.setEndTime(last.getWindowEnd());
            c.setMaintenanceActive(windows.stream().anyMatch(AlignedWindow::isMaintenanceActive));
            c.setQualityStatus(windows.stream().anyMatch(w -> !"complete".equals(w.getQualityStatus())) ? "partial" : "complete");
            c.setDetectionSources(List.of("FLINK_CEP")); c.setMatchedRules(List.of(rule));
            c.setMatchedWindowIds(windows.stream().map(AlignedWindow::getWindowId).toList());
            c.setSourceRefs(windows.stream().map(w -> new DetectionCandidate.SourceRef("l1-aligned-windows", w.getWindowId(), w.getContentSha256())).toList());

            Map<String, Object> evidence = new LinkedHashMap<>(); evidence.put("window_count", windows.size());
            evidence.put("max_cpu_pct", windows.stream().mapToDouble(AlignedWindow::getCpuMeanPct).filter(Double::isFinite).max().orElse(Double.NaN));
            evidence.put("max_memory_pct", windows.stream().mapToDouble(AlignedWindow::getMemoryMeanPct).filter(Double::isFinite).max().orElse(Double.NaN));
            evidence.put("total_errors", windows.stream().mapToInt(AlignedWindow::getErrorCount).sum());
            evidence.put("total_failed_spans", windows.stream().mapToInt(AlignedWindow::getFailedSpanCount).sum());
            List<Map<String, Object>> perWindow = new ArrayList<>();
            for (WindowEvaluation evaluation : pattern.values().stream().flatMap(List::stream).distinct().toList()) {
                AlignedWindow w = evaluation.getWindow(); Map<String, Object> item = new LinkedHashMap<>();
                item.put("window_id", w.getWindowId()); item.put("start_time", w.getWindowStart()); item.put("end_time", w.getWindowEnd());
                item.put("drools_markers", evaluation.getAnomalyMarkers());
                item.put("cpu_mean_pct", w.getCpuMeanPct()); item.put("memory_mean_pct", w.getMemoryMeanPct());
                item.put("latency_mean_ms", w.getLatencyMeanMs()); item.put("packet_loss_mean_ratio", w.getPacketLossMeanRatio());
                item.put("error_count", w.getErrorCount()); item.put("error_rate", w.getErrorRate());
                item.put("failed_span_count", w.getFailedSpanCount()); item.put("span_failure_rate", w.getSpanFailureRate());
                item.put("alarm_count", w.getAlarmCount()); perWindow.add(item);
            }
            evidence.put("matched_windows", perWindow);
            c.setEvidence(evidence); c.getSourceEvidence().put("FLINK_CEP", new LinkedHashMap<>(evidence));
            c.setSourceEventIds(List.of(l2.rules.CandidateFusion.sourceEventId(c)));
            WindowSnapshotSupport.attach(c, windows);
            return c;
        }
    }
}
