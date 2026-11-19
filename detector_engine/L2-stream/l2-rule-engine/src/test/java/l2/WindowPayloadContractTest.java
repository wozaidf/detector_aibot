package l2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import l2.contract.WindowSnapshotSupport;
import l2.model.DetectionCandidate;
import l2.model.WindowSnapshot;
import l2.model.PendingCandidate;
import l2.model.FusionInput;
import l2.rules.CandidateFusion;
import l2.rules.FinalDecisionFunction;
import l2.rules.DroolsSupport;
import l2.rules.RuleActions;
import l2.source.AlignedWindowParser;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

class WindowPayloadContractTest {
    @Test void droolsCarriesOriginalFullFeatureTreeAndHashWithoutRawEvents() throws Exception {
        var window = ContractTestFixtures.window(0, 96, false);
        var candidate = ContractTestFixtures.drools(window).get(0);
        var payload = WindowSnapshotSupport.prepare(candidate);
        assertThat(payload.entityType()).isEqualTo("virtual_machine");
        assertThat(payload.windowCount()).isEqualTo(1);
        assertThat(payload.quality().snapshotStatus()).isEqualTo("COMPLETE");
        WindowSnapshot snapshot = payload.snapshots().get(0);
        JsonNode raw = ContractTestFixtures.JSON.readTree(window.getRawJson());
        JsonNode exported = ContractTestFixtures.JSON.valueToTree(snapshot);
        for (String field : List.of("features", "window", "quality", "context")) assertThat(exported.get(field)).isEqualTo(raw.get(field));
        assertThat(snapshot.getSourceRef().contentSha256()).isEqualTo(window.getContentSha256());
        assertThat(exported.has("events")).isFalse();
        assertThat(snapshot.getFeatures().get("metrics")).asString().contains("memory_max_pct", "bandwidth_utilization_mean_pct");
    }

    @Test void cepRetainsSeparateWindowVectorsAndExcludesTheBreakWindow() throws Exception {
        var windows = List.of(ContractTestFixtures.window(0, 96, false), ContractTestFixtures.window(1, 97, false), ContractTestFixtures.window(2, 98, false));
        var candidate = ContractTestFixtures.cep(windows, ContractTestFixtures.window(3, 30, false));
        var payload = WindowSnapshotSupport.prepare(candidate);
        assertThat(payload.windowCount()).isEqualTo(3);
        assertThat(payload.snapshots()).extracting(WindowSnapshot::getWindowId).containsExactlyElementsOf(candidate.getMatchedWindowIds());
        assertThat(payload.snapshots()).extracting(s -> ContractTestFixtures.JSON.valueToTree(s.getFeatures()).at("/metrics/cpu_mean_pct").asDouble())
                .containsExactly(96.0, 97.0, 98.0);
        assertThat(payload.snapshots()).extracting(s -> s.getWindow().get("size_sec")).containsOnly(300);
        assertThat(payload.snapshots()).extracting(WindowSnapshot::getWindowId).doesNotContain("WIN-CONTRACT-004");
    }

    @Test void fusionDeduplicatesSharedSnapshotsWithoutChangingLegacyIdsAndIsOrderStable() throws Exception {
        var first = ContractTestFixtures.window(0, 96, false);
        var drools = ContractTestFixtures.drools(first).get(0);
        var cep = ContractTestFixtures.cep(List.of(first, ContractTestFixtures.window(1, 97, false), ContractTestFixtures.window(2, 98, false)), null);
        var a = CandidateFusion.merge(drools, cep);
        var b = CandidateFusion.merge(cep, drools);
        assertThat(WindowSnapshotSupport.prepare(a).snapshots()).hasSize(3);
        assertThat(ContractTestFixtures.JSON.<JsonNode>valueToTree(WindowSnapshotSupport.prepare(a)))
                .isEqualTo(ContractTestFixtures.JSON.valueToTree(WindowSnapshotSupport.prepare(b)));
        assertThat(CandidateFusion.stableFinalEventId(a)).isEqualTo(CandidateFusion.stableFinalEventId(b));
    }

    @Test void duplicateSourceCandidatesKeepConflictingVersionsButOnlyOneBusinessCandidate() throws Exception {
        var first = ContractTestFixtures.window(0, 96, false);
        var changed = ContractTestFixtures.window(0, 97, false);
        var a = ContractTestFixtures.drools(first).get(0);
        var b = ContractTestFixtures.drools(changed).get(0);
        String originalId = CandidateFusion.stableFinalEventId(a);
        var result = CandidateFusion.mergeAll(List.of(a, b), 300);
        assertThat(result).hasSize(1);
        var payload = WindowSnapshotSupport.prepare(result.get(0));
        assertThat(payload.windowCount()).isEqualTo(1);
        assertThat(payload.snapshots()).hasSize(2);
        assertThat(payload.quality().snapshotStatus()).isEqualTo("CONFLICT");
        assertThat(payload.quality().snapshotConflicts()).hasSize(1);
        assertThat(payload.quality().snapshotConflicts().get(0).contentSha256s()).containsExactlyInAnyOrder(first.getContentSha256(), changed.getContentSha256());
        assertThat(CandidateFusion.stableFinalEventId(result.get(0))).isEqualTo(originalId);
        var reverseA = ContractTestFixtures.drools(changed).get(0);
        var reverseB = ContractTestFixtures.drools(first).get(0);
        assertThat(ContractTestFixtures.JSON.<JsonNode>valueToTree(payload)).isEqualTo(ContractTestFixtures.JSON.valueToTree(
                WindowSnapshotSupport.prepare(CandidateFusion.mergeAll(List.of(reverseA, reverseB), 300).get(0))));
    }

    @Test void foreignEntityAndUnmatchedWindowAreExcludedAndReported() throws Exception {
        var candidate = ContractTestFixtures.drools(ContractTestFixtures.window(0, 96, false)).get(0);
        var wrongWindow = ContractTestFixtures.drools(ContractTestFixtures.window(10, 96, false)).get(0).getWindowSnapshots().get(0);
        var otherNode = ContractTestFixtures.windowJson(0, 96, false).put("entity_id", "other-device");
        var wrongEntity = ContractTestFixtures.drools(new AlignedWindowParser().parse(otherNode.toString())).get(0).getWindowSnapshots().get(0);
        candidate.getWindowSnapshots().addAll(List.of(wrongEntity, wrongWindow));
        var payload = WindowSnapshotSupport.prepare(candidate);
        assertThat(payload.snapshots()).hasSize(1);
        assertThat(payload.quality().snapshotWarnings()).hasSize(2);
        assertThat(payload.quality().snapshotStatus()).isEqualTo("INCOMPLETE");
    }

    @Test void missingRawWindowDoesNotDeleteOrChangeTheAlert() throws Exception {
        var window = ContractTestFixtures.window(0, 96, false);
        var normal = ContractTestFixtures.decideAndEmit(ContractTestFixtures.drools(window)).get(0);
        window.setRawJson(null);
        var incomplete = ContractTestFixtures.decideAndEmit(ContractTestFixtures.drools(window)).get(0);
        assertThat(incomplete.l2Result().payloadQuality().snapshotStatus()).isEqualTo("INCOMPLETE");
        assertThat(incomplete.l2Result().payloadQuality().missingWindowIds()).containsExactly(window.getWindowId());
        assertThat(incomplete.l2Result().payloadQuality().snapshotWarnings()).isNotEmpty();
        assertThat(ContractTestFixtures.legacyProjection(ContractTestFixtures.json(incomplete)))
                .isEqualTo(ContractTestFixtures.legacyProjection(ContractTestFixtures.json(normal)));
    }

    @Test void nullZeroAndMissingFeaturesRemainDistinctAndJsonIsStandard() throws Exception {
        var node = ContractTestFixtures.windowJson(0, 96, false);
        ((ObjectNode) node.at("/features/metrics")).putNull("memory_max_pct").remove("network_traffic_mean_mbps");
        var candidate = ContractTestFixtures.drools(new AlignedWindowParser().parse(node.toString())).get(0);
        var event = ContractTestFixtures.decideAndEmit(List.of(candidate)).get(0);
        var json = ContractTestFixtures.json(event);
        assertThat(json.at("/original_data/window_snapshots/0/features/metrics/memory_max_pct").isNull()).isTrue();
        assertThat(json.at("/original_data/window_snapshots/0/features/metrics/network_traffic_mean_mbps").isMissingNode()).isTrue();
        assertThat(json.at("/original_data/window_snapshots/0/features/alarms/alarm_per_minute").asDouble()).isZero();
        assertThat(event.l2Result().qualityStatus()).isEqualTo("complete");
        assertThat(event.l2Result().payloadQuality().snapshotStatus()).isEqualTo("INCOMPLETE");
        assertThat(event.l2Result().payloadQuality().featureWarnings()).extracting(w -> w.path())
                .containsExactly("/features/metrics/memory_max_pct", "/features/metrics/network_traffic_mean_mbps");
        assertThat(json.has("window_id")).isFalse(); assertThat(json.has("features")).isFalse();
        assertThat(json.has("schema_version")).isFalse();
        assertThat(json.path("event_type").asText()).isEqualTo("ANOMALY_EVENT");
        assertThat(json.path("stage").asText()).isEqualTo("L2");
        assertThat(json.path("l25_result").isNull()).isTrue();
        assertThat(json.path("l4_result").isNull()).isTrue();
        assertThat(json.path("created_at").asText()).isEqualTo(json.path("updated_at").asText());
        assertThat(json.at("/l2_result/detected_at").isTextual()).isTrue();
    }

    @Test void nonNumericFeatureIsRetainedAndExplainedInsteadOfImputed() throws Exception {
        var node = ContractTestFixtures.windowJson(0, 96, false);
        ((ObjectNode) node.at("/features/traces")).put("avg_duration_ms", "unknown");
        var payload = WindowSnapshotSupport.prepare(ContractTestFixtures.drools(new AlignedWindowParser().parse(node.toString())).get(0));
        assertThat(ContractTestFixtures.JSON.valueToTree(payload.snapshots().get(0).getFeatures()).at("/traces/avg_duration_ms").asText()).isEqualTo("unknown");
        assertThat(payload.quality().featureWarnings()).extracting(w -> w.reason()).containsExactly("non-numeric value");
    }

    @Test void candidateSnapshotSurvivesFlinkStateSerialization() throws Exception {
        var candidate = ContractTestFixtures.drools(ContractTestFixtures.window(0, 96, false)).get(0);
        var serializer = TypeInformation.of(DetectionCandidate.class).createSerializer(new ExecutionConfig().getSerializerConfig());
        var bytes = new DataOutputSerializer(4096);
        serializer.serialize(candidate, bytes);
        var restored = serializer.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
        assertThat(ContractTestFixtures.JSON.<JsonNode>valueToTree(WindowSnapshotSupport.prepare(restored)))
                .isEqualTo(ContractTestFixtures.JSON.valueToTree(WindowSnapshotSupport.prepare(candidate)));
    }

    @Test void pendingDuplicateEnrichesOnlyPayloadAndKeepsReleaseTime() throws Exception {
        var original = ContractTestFixtures.drools(ContractTestFixtures.window(0, 96, false)).get(0);
        var replay = ContractTestFixtures.drools(ContractTestFixtures.window(0, 97, false)).get(0);
        long release = 123456789L;
        List<PendingCandidate> state = new ArrayList<>(List.of(new PendingCandidate(original, release)));
        Object pending = Proxy.newProxyInstance(ListState.class.getClassLoader(), new Class<?>[]{ListState.class}, (proxy, method, args) -> switch (method.getName()) {
            case "get" -> new ArrayList<>(state);
            case "update" -> { state.clear(); state.addAll((List<PendingCandidate>) args[0]); yield null; }
            default -> throw new UnsupportedOperationException(method.getName());
        });
        var function = new FinalDecisionFunction(300, 300, 5, 604800);
        var pendingField = FinalDecisionFunction.class.getDeclaredField("pending"); pendingField.setAccessible(true); pendingField.set(function, pending);
        var sessionField = FinalDecisionFunction.class.getDeclaredField("session"); sessionField.setAccessible(true);
        sessionField.set(function, DroolsSupport.build("rules/decision-rules.drl").newKieSession());
        try {
            // 重复分支不应使用 Context 注册新定时器，也不应输出第二条候选。
            function.processElement(FusionInput.candidate(replay), null, null);
            assertThat(state).hasSize(1);
            assertThat(state.get(0).getReleaseAtMillis()).isEqualTo(release);
            assertThat(state.get(0).getCandidate().getSeverity()).isEqualTo("critical");
            assertThat(WindowSnapshotSupport.prepare(state.get(0).getCandidate()).quality().snapshotStatus()).isEqualTo("CONFLICT");
        } finally { function.close(); }
    }

    @Test void missingReferencedWindowIsReportedAndWindowCountUsesUniqueIds() throws Exception {
        var candidate = ContractTestFixtures.drools(ContractTestFixtures.window(0, 96, false)).get(0);
        candidate.setMatchedWindowIds(List.of("WIN-CONTRACT-001", "WIN-CONTRACT-001", "WIN-MISSING"));
        var payload = WindowSnapshotSupport.prepare(candidate);
        assertThat(payload.windowCount()).isEqualTo(2);
        assertThat(payload.quality().missingWindowIds()).containsExactly("WIN-MISSING");
        assertThat(payload.quality().snapshotStatus()).isEqualTo("INCOMPLETE");
    }

    @Test void allLegacyDetectionFieldsMatchTheFrozen20Implementation() throws Exception {
        JsonNode expected;
        try (var in = getClass().getResourceAsStream("/contracts/legacy-projection.json")) { expected = ContractTestFixtures.JSON.readTree(in); }
        for (var entry : ContractTestFixtures.scenarios().entrySet()) {
            List<JsonNode> actual = new ArrayList<>();
            for (var event : entry.getValue()) actual.add(ContractTestFixtures.legacyProjection(ContractTestFixtures.json(event)));
            assertThat(ContractTestFixtures.JSON.<JsonNode>valueToTree(actual)).as(entry.getKey()).isEqualTo(expected.get(entry.getKey()));
        }
    }
}
