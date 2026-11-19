package l2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import l2.cep.CepDetector;
import l2.config.AppConfig;
import l2.config.RuleThresholds;
import l2.model.AlignedWindow;
import l2.model.AnomalyEvent;
import l2.model.DetectionCandidate;
import l2.model.FusionInput;
import l2.model.WindowEvaluation;
import l2.rules.CandidateFusion;
import l2.rules.DroolsSupport;
import l2.rules.FinalDecisionFunction;
import l2.sink.AnomalyKafkaSerializer;
import l2.source.AlignedWindowParser;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.util.Collector;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** 明确标注的合成契约测试数据，不代表实际模型或检测运行指标。可同时运行于 2.0 和 2.1。 */
public final class ContractTestFixtures {
    static final ObjectMapper JSON = new ObjectMapper();
    private ContractTestFixtures() {}

    static ObjectNode windowJson(int index, double cpu, boolean maintenance) throws Exception {
        ObjectNode node;
        try (var in = ContractTestFixtures.class.getResourceAsStream("/contracts/window.json")) {
            node = (ObjectNode) JSON.readTree(in);
        }
        node.put("window_id", String.format("WIN-CONTRACT-%03d", index + 1));
        ((ObjectNode) node.get("window")).put("start", Instant.parse("2026-10-09T09:00:00Z").plusSeconds(index * 60L).toString())
                .put("end", Instant.parse("2026-10-09T09:05:00Z").plusSeconds(index * 60L).toString());
        ((ObjectNode) node.at("/features/metrics")).put("cpu_mean_pct", cpu).put("cpu_max_pct", Math.min(100, cpu + 3));
        ((ObjectNode) node.get("context")).put("maintenance_active", maintenance);
        return node;
    }

    static AlignedWindow window(int index, double cpu, boolean maintenance) throws Exception {
        return new AlignedWindowParser().parse(JSON.writeValueAsString(windowJson(index, cpu, maintenance)));
    }

    static AppConfig config() {
        return new AppConfig("localhost:9092", "in", "out", "test", 80, 85, 160, .03, .3, .3,
                95, 92, 220, .05, .6, .8, 160, .03, .3, .3, 1, 80, 1, 1, 3, 3, 600, 5, 30, 300, 300, 1000, true);
    }

    static List<DetectionCandidate> drools(AlignedWindow window) {
        var session = DroolsSupport.build("rules/window-rules.drl").newKieSession();
        List<DetectionCandidate> candidates = new ArrayList<>();
        try {
            session.setGlobal("results", candidates); session.setGlobal("markers", new LinkedHashSet<String>());
            session.insert(new RuleThresholds(config())); session.insert(window); session.fireAllRules();
            return candidates;
        } finally { session.dispose(); }
    }

    static DetectionCandidate cep(List<AlignedWindow> windows, AlignedWindow breakWindow) throws Exception {
        var selector = new CepDetector.CandidateSelect("CPU_HIGH", "CEP_CPU_SUSTAINED", 3);
        Map<String, List<WindowEvaluation>> pattern = new LinkedHashMap<>();
        pattern.put("run", windows.stream().map(w -> new WindowEvaluation(w, java.util.Set.of("CPU_HIGH"))).toList());
        if (breakWindow != null) pattern.put("break", List.of(new WindowEvaluation(breakWindow, java.util.Set.of())));
        var method = CepDetector.CandidateSelect.class.getDeclaredMethod("createClosedSequence", Map.class, Long.class, boolean.class);
        method.setAccessible(true);
        FusionInput input = (FusionInput) method.invoke(selector, pattern, windows.get(windows.size() - 1).getWindowEndEpochMillis(), true);
        return input.getCandidate();
    }

    static List<AnomalyEvent> decideAndEmit(List<DetectionCandidate> candidates) throws Exception {
        var session = DroolsSupport.build("rules/decision-rules.drl").newKieSession();
        try {
            for (DetectionCandidate candidate : candidates) {
                // 模拟候选通过 Flink ListSerializer 后的可变列表，供现有决策规则追加规则名。
                candidate.setMatchedRules(new ArrayList<>(candidate.getMatchedRules()));
                var handle = session.insert(candidate); session.fireAllRules(); session.delete(handle);
            }
        } finally { session.dispose(); }
        List<AnomalyEvent> output = new ArrayList<>();
        var function = new FinalDecisionFunction(300, 300, 5, 604800);
        Map<String, Long> seen = new LinkedHashMap<>();
        Object state = Proxy.newProxyInstance(MapState.class.getClassLoader(), new Class<?>[]{MapState.class}, (proxy, method, args) -> switch (method.getName()) {
            case "get" -> seen.get(args[0]);
            case "put" -> { seen.put((String) args[0], (Long) args[1]); yield null; }
            case "remove" -> { seen.remove(args[0]); yield null; }
            case "entries" -> new ArrayList<>(seen.entrySet());
            default -> throw new UnsupportedOperationException(method.getName());
        });
        var field = FinalDecisionFunction.class.getDeclaredField("emitted"); field.setAccessible(true); field.set(function, state);
        var emit = FinalDecisionFunction.class.getDeclaredMethod("emitOnce", DetectionCandidate.class, Collector.class); emit.setAccessible(true);
        Collector<AnomalyEvent> collector = new Collector<>() {
            @Override public void collect(AnomalyEvent value) { output.add(value); }
            @Override public void close() {}
        };
        for (DetectionCandidate candidate : CandidateFusion.mergeAll(candidates, 300)) emit.invoke(function, candidate, collector);
        return output;
    }

    static ObjectNode json(AnomalyEvent event) throws Exception {
        byte[] bytes = new AnomalyKafkaSerializer("test-out").serialize(event, null, null).value();
        return (ObjectNode) JSON.readTree(new String(bytes, StandardCharsets.UTF_8));
    }

    static ObjectNode legacyProjection(ObjectNode node) {
        ObjectNode result = JSON.createObjectNode();
        result.put("event_id", node.path("event_id").asText());
        result.put("entity_id", node.path("entity_id").asText());
        result.setAll((ObjectNode) node.path("l2_result"));
        result.set("source_refs", node.path("original_data").path("source_refs").deepCopy());
        result.remove(List.of("detected_at", "window_count", "payload_quality"));
        return result;
    }

    static Map<String, List<AnomalyEvent>> scenarios() throws Exception {
        Map<String, List<AnomalyEvent>> cases = new LinkedHashMap<>();
        cases.put("single", decideAndEmit(drools(window(0, 96, false))));
        cases.put("cep", decideAndEmit(List.of(cep(List.of(window(0, 96, false), window(1, 97, false), window(2, 98, false)), window(3, 30, false)))));
        List<DetectionCandidate> merged = new ArrayList<>(drools(window(0, 96, false)));
        merged.add(cep(List.of(window(0, 96, false), window(1, 97, false), window(2, 98, false)), window(3, 30, false)));
        cases.put("merged", decideAndEmit(merged));
        cases.put("maintenance", decideAndEmit(drools(window(0, 96, true))));
        List<DetectionCandidate> independent = new ArrayList<>(drools(window(0, 96, false)));
        independent.addAll(drools(window(20, 97, false)));
        cases.put("independent", decideAndEmit(independent));
        return cases;
    }
}
