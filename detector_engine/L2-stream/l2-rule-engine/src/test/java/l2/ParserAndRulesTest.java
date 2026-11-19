package l2;

import l2.config.AppConfig;
import l2.config.RuleThresholds;
import l2.model.AlignedWindow;
import l2.model.DetectionCandidate;
import l2.rules.RuleActions;
import l2.rules.DroolsSupport;
import l2.rules.CandidateFusion;
import l2.cep.CepDetector;
import l2.source.AlignedWindowParser;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.KieSession;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParserAndRulesTest {
    private String fixture() throws Exception {
        try (var in = getClass().getResourceAsStream("/sample-window.json")) { return new String(in.readAllBytes(), StandardCharsets.UTF_8); }
    }
    private AppConfig config() { return new AppConfig("localhost:9092", "in", "out", "test", 80, 85, 160, .03, .3, .3,
            95, 92, 220, .05, .6, .8, 160, .03, .3, .3, 1, 80, 1, 1, 3, 3, 600, 5, 30, 300, 300, 1000, true); }

    @Test void parsesL1ContractAndEventTime() throws Exception {
        AlignedWindow w = new AlignedWindowParser().parse(fixture());
        assertThat(w.getEntityId()).isEqualTo("server-001"); assertThat(w.getCpuMeanPct()).isEqualTo(91); assertThat(w.getWindowEndEpochMillis()).isPositive();
        assertThat(w.getWindowSlideSeconds()).isEqualTo(60);
        assertThat(w.getRawJson()).contains("WIN-TEST-001"); assertThat(w.getContentSha256()).hasSize(64);
    }

    @Test void singleWindowCandidateCarriesQueryableSourceReference() throws Exception {
        AlignedWindow w = new AlignedWindowParser().parse(fixture());
        DetectionCandidate candidate = RuleActions.single(w, "CPU_HIGH", "WINDOW_CPU_HIGH", "major");

        assertThat(candidate.getMatchedWindowIds()).containsExactly("WIN-TEST-001");
        assertThat(candidate.getSourceRefs()).hasSize(1);
        assertThat(candidate.getSourceRefs().get(0).topic()).isEqualTo("l1-aligned-windows");
        assertThat(candidate.getSourceRefs().get(0).windowId()).isEqualTo("WIN-TEST-001");
        assertThat(candidate.getSourceRefs().get(0).contentSha256()).isEqualTo(w.getContentSha256());
        assertThat(candidate.getEvidence()).doesNotContainKey("raw_json");
    }

    @Test void rejectsInvalidContract() {
        assertThatThrownBy(() -> new AlignedWindowParser().parse("{\"record_type\":\"wrong\"}"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void droolsMarksOrdinaryCpuWindowWithoutCreatingCandidate() throws Exception {
        AlignedWindow w = new AlignedWindowParser().parse(fixture());
        KieSession session = DroolsSupport.build("rules/window-rules.drl").newKieSession();
        List<DetectionCandidate> results = new ArrayList<>(); Set<String> markers = new LinkedHashSet<>();
        session.setGlobal("results", results); session.setGlobal("markers", markers); session.insert(new RuleThresholds(config())); session.insert(w); session.fireAllRules(); session.dispose();
        assertThat(markers).contains("CPU_HIGH"); assertThat(results).isEmpty();
    }

    @Test void severeCpuThresholdCreatesCandidate() throws Exception {
        AlignedWindow w = new AlignedWindowParser().parse(fixture());
        AppConfig c = new AppConfig("localhost:9092", "in", "out", "test", 80, 85, 160, .03, .3, .3,
                90, 92, 220, .05, .6, .8, 160, .03, .3, .3, 1, 80, 1, 1, 3, 3, 600, 5, 30, 300, 300, 1000, true);
        KieSession session = DroolsSupport.build("rules/window-rules.drl").newKieSession();
        List<DetectionCandidate> results = new ArrayList<>(); Set<String> markers = new LinkedHashSet<>();
        session.setGlobal("results", results); session.setGlobal("markers", markers); session.insert(new RuleThresholds(c)); session.insert(w); session.fireAllRules(); session.dispose();
        assertThat(results).extracting(DetectionCandidate::getAnomalyType).contains("CPU_HIGH");
        assertThat(results.get(0).getSourceEvidence()).containsKey("DROOLS_WINDOW");
    }

    @Test void finalDecisionSuppressesMaintenance() {
        DetectionCandidate c = new DetectionCandidate(); c.setEntityId("server-001"); c.setAnomalyType("CPU_HIGH"); c.setMaintenanceActive(true);
        KieSession session = DroolsSupport.build("rules/decision-rules.drl").newKieSession(); session.insert(c); session.fireAllRules(); session.dispose();
        assertThat(c.isSuppressed()).isTrue(); assertThat(c.getDecision()).isEqualTo("SUPPRESSED"); assertThat(c.getSeverity()).isEqualTo("info");
    }

    @Test void invalidJsonIsRejectedRatherThanSilentlySkipped() {
        assertThatThrownBy(() -> new AlignedWindowParser().parse("{not-json"))
                .isInstanceOf(Exception.class);
    }

    @Test void singleL2CandidateIsKeptAndReplayIdIsStable() {
        DetectionCandidate l2 = candidate("server-001", "DROOLS_WINDOW", List.of("WIN-1"), "2026-10-09T09:00:00Z", "2026-10-09T09:05:00Z");
        List<DetectionCandidate> replayed = CandidateFusion.mergeAll(List.of(l2, l2), 300);
        assertThat(replayed).hasSize(1);
        assertThat(CandidateFusion.stableFinalEventId(l2)).isEqualTo(CandidateFusion.stableFinalEventId(replayed.get(0)));
        assertThat(replayed.get(0).getSourceEvidence()).containsOnlyKeys("DROOLS_WINDOW");
    }

    @Test void candidatesFromBothDetectorsMergeAndPreserveIndependentEvidence() {
        DetectionCandidate l2 = candidate("server-001", "DROOLS_WINDOW", List.of("WIN-1"), "2026-10-09T09:00:00Z", "2026-10-09T09:05:00Z");
        DetectionCandidate cep = candidate("server-001", "FLINK_CEP", List.of("WIN-1", "WIN-2"), "2026-10-09T09:00:00Z", "2026-10-09T09:06:00Z");
        List<DetectionCandidate> result = CandidateFusion.mergeAll(List.of(cep, l2), 300);
        assertThat(result).hasSize(1); assertThat(result.get(0).getDetectionSources()).containsExactlyInAnyOrder("DROOLS_WINDOW", "FLINK_CEP");
        assertThat(result.get(0).getSourceEventIds()).hasSize(2); assertThat(result.get(0).getSourceEvidence()).containsKeys("DROOLS_WINDOW", "FLINK_CEP");
    }

    @Test void overlappingTimesWithoutWindowIdsCanAssociateForSameEntityAndType() {
        DetectionCandidate l2 = candidate("server-001", "DROOLS_WINDOW", List.of(), "2026-10-09T09:00:00Z", "2026-10-09T09:05:00Z");
        DetectionCandidate cep = candidate("server-001", "FLINK_CEP", List.of("WIN-2"), "2026-10-09T09:04:00Z", "2026-10-09T09:08:00Z");
        assertThat(CandidateFusion.mergeAll(List.of(l2, cep), 300)).hasSize(1);
    }

    @Test void differentEntitiesAndDifferentWindowIdsNeverMergeOnTimeAlone() {
        DetectionCandidate a = candidate("server-001", "DROOLS_WINDOW", List.of("WIN-1"), "2026-10-09T09:00:00Z", "2026-10-09T09:05:00Z");
        DetectionCandidate otherEntity = candidate("server-002", "FLINK_CEP", List.of("WIN-1"), "2026-10-09T09:00:00Z", "2026-10-09T09:05:00Z");
        DetectionCandidate otherWindows = candidate("server-001", "FLINK_CEP", List.of("WIN-9"), "2026-10-09T09:01:00Z", "2026-10-09T09:04:00Z");
        assertThat(CandidateFusion.mergeAll(List.of(a, otherEntity, otherWindows), 300)).hasSize(3);
    }

    @Test void cepContinuityRequiresSameEntityAndExactWindowSlide() {
        AlignedWindow previous = new AlignedWindow(); previous.setEntityId("server-001"); previous.setWindowStart("2026-10-09T09:00:00Z");
        previous.setWindowEndEpochMillis(java.time.Instant.parse("2026-10-09T09:05:00Z").toEpochMilli()); previous.setWindowSlideSeconds(60);
        AlignedWindow next = new AlignedWindow(); next.setEntityId("server-001"); next.setWindowStart("2026-10-09T09:01:00Z");
        next.setWindowEndEpochMillis(java.time.Instant.parse("2026-10-09T09:06:00Z").toEpochMilli()); next.setWindowSlideSeconds(60);
        assertThat(CepDetector.isContinuousWindow(previous, next)).isTrue();
        next.setWindowStart("2026-10-09T09:02:00Z");
        assertThat(CepDetector.isContinuousWindow(previous, next)).isFalse();
        next.setWindowStart("2026-10-09T09:01:00Z"); next.setEntityId("server-002");
        assertThat(CepDetector.isContinuousWindow(previous, next)).isFalse();
    }

    private DetectionCandidate candidate(String entity, String source, List<String> windows, String start, String end) {
        DetectionCandidate c = new DetectionCandidate(); c.setEntityId(entity); c.setAnomalyType("CPU_HIGH"); c.setSeverity("major");
        c.setStartTime(start); c.setEndTime(end); c.setDetectionSources(List.of(source)); c.setMatchedWindowIds(windows);
        c.setMatchedRules(List.of(source + "_CPU")); c.getEvidence().put("evidence", source); c.getSourceEvidence().put(source, Map.of("value", source));
        c.setSourceEventIds(List.of(CandidateFusion.sourceEventId(c))); return c;
    }
}
