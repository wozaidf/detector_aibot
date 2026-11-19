package l2.contract;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import l2.model.AlignedWindow;
import l2.model.DetectionCandidate;
import l2.model.PayloadQuality;
import l2.model.WindowSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** 只负责快照提取、合并和质量说明，不参与检测、关联或事件 ID 计算。 */
public final class WindowSnapshotSupport {
    public static final String FEATURE_SCHEMA_VERSION = "l1-window-features-v1";
    private static final Logger LOG = LoggerFactory.getLogger(WindowSnapshotSupport.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final List<String> FEATURE_PATHS = loadFeaturePaths();
    private static final Comparator<WindowSnapshot> ORDER = Comparator
            .comparing((WindowSnapshot s) -> time(s.getWindow().get("start")))
            .thenComparing(s -> time(s.getWindow().get("end")))
            .thenComparing(s -> text(s.getWindowId()))
            .thenComparing(s -> text(s.getSourceRef() == null ? null : s.getSourceRef().contentSha256()));

    private WindowSnapshotSupport() {}

    public record Payload(String entityType, int windowCount, List<WindowSnapshot> snapshots,
                          PayloadQuality quality) {}

    private static List<String> loadFeaturePaths() {
        try (var in = WindowSnapshotSupport.class.getResourceAsStream("/contracts/l1-window-features-v1.json")) {
            if (in == null) throw new IllegalStateException("Missing feature contract resource");
            List<String> paths = new ArrayList<>();
            MAPPER.readTree(in).get("features").forEach(f -> paths.add(f.get("path").asText()));
            if (paths.size() != 23) throw new IllegalStateException("Feature contract must contain 23 columns");
            return List.copyOf(paths);
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    public static void attach(DetectionCandidate candidate, List<AlignedWindow> windows) {
        List<WindowSnapshot> snapshots = new ArrayList<>();
        List<Map<String, String>> issues = new ArrayList<>();
        for (AlignedWindow window : windows) {
            try {
                var ref = candidate.getSourceRefs().stream()
                        .filter(r -> Objects.equals(window.getWindowId(), r.windowId())
                                && Objects.equals(window.getContentSha256(), r.contentSha256()))
                        .findFirst().orElseThrow(() -> new IllegalArgumentException("missing matching source_ref"));
                JsonNode root = MAPPER.readTree(window.getRawJson());
                if (root == null || !root.isObject()) throw new IllegalArgumentException("missing original window JSON");
                if (!Objects.equals(root.path("entity_id").asText(), window.getEntityId())
                        || !Objects.equals(root.path("window_id").asText(), window.getWindowId()))
                    throw new IllegalArgumentException("original JSON identity differs from parsed window");
                WindowSnapshot snapshot = new WindowSnapshot();
                snapshot.setSchemaVersion(root.path("schema_version").asText(null));
                snapshot.setRecordType(root.path("record_type").asText(null));
                snapshot.setWindowId(window.getWindowId()); snapshot.setEntityId(window.getEntityId());
                snapshot.setEntityType(root.path("entity_type").asText(null));
                snapshot.setWindow(object(root, "/window")); snapshot.setFeatures(object(root, "/features"));
                snapshot.setQuality(object(root, "/quality")); snapshot.setContext(object(root, "/context"));
                if (root.at("/fusion_features/key_level/relation_statistics").isObject())
                    snapshot.setRelationStatistics(object(root, "/fusion_features/key_level/relation_statistics"));
                else issues.add(issue(window.getWindowId(), "relation_statistics", "missing original relation_statistics object"));
                snapshot.setSourceRef(ref); snapshots.add(snapshot);
            } catch (Exception e) {
                issues.add(issue(window.getWindowId(), "window_snapshots", "snapshot extraction failed: " + e.getMessage()));
                LOG.warn("Window snapshot unavailable for entity={} window={}: {}", window.getEntityId(), window.getWindowId(), e.getMessage());
            }
        }
        candidate.setWindowSnapshots(unionSnapshots(List.of(), snapshots));
        candidate.setSnapshotIssues(unionIssues(List.of(), issues));
    }

    private static Map<String, Object> object(JsonNode root, String pointer) {
        JsonNode value = root.at(pointer);
        if (!value.isObject()) throw new IllegalArgumentException(pointer + " must be an object for snapshot export");
        return MAPPER.convertValue(value, MAP_TYPE);
    }

    /** 重复候选只合并新载荷，不改变业务字段或去重结果。 */
    public static void mergeInto(DetectionCandidate target, DetectionCandidate source) {
        target.setWindowSnapshots(unionSnapshots(target.getWindowSnapshots(), source.getWindowSnapshots()));
        target.setSnapshotIssues(unionIssues(target.getSnapshotIssues(), source.getSnapshotIssues()));
    }

    private static List<WindowSnapshot> unionSnapshots(List<WindowSnapshot> first, List<WindowSnapshot> second) {
        TreeMap<String, WindowSnapshot> unique = new TreeMap<>();
        for (List<WindowSnapshot> items : List.of(safe(first), safe(second))) {
            for (WindowSnapshot s : items) {
                if (s == null) continue;
                String digest = s.getSourceRef() == null ? "" : text(s.getSourceRef().contentSha256());
                unique.putIfAbsent(text(s.getEntityId()) + "|" + text(s.getWindowId()) + "|" + digest, s);
            }
        }
        return unique.values().stream().sorted(ORDER).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
    }

    private static List<Map<String, String>> unionIssues(List<Map<String, String>> first, List<Map<String, String>> second) {
        TreeMap<String, Map<String, String>> unique = new TreeMap<>();
        for (var issues : List.of(safe(first), safe(second))) for (var issue : issues)
            unique.putIfAbsent(text(issue.get("window_id")) + "|" + text(issue.get("path")) + "|" + text(issue.get("reason")), issue);
        return new ArrayList<>(unique.values());
    }

    public static Payload prepare(DetectionCandidate candidate) {
        TreeSet<String> expected = new TreeSet<>(safe(candidate.getMatchedWindowIds()));
        List<WindowSnapshot> accepted = new ArrayList<>();
        List<PayloadQuality.Warning> snapshotWarnings = new ArrayList<>();
        for (var issue : safe(candidate.getSnapshotIssues())) snapshotWarnings.add(new PayloadQuality.Warning(
                issue.get("window_id"), issue.get("path"), issue.get("reason")));
        if (expected.isEmpty()) snapshotWarnings.add(new PayloadQuality.Warning(null, "matched_window_ids", "no referenced windows available for model input"));
        for (WindowSnapshot s : unionSnapshots(candidate.getWindowSnapshots(), List.of())) {
            String reason = null;
            if (!Objects.equals(candidate.getEntityId(), s.getEntityId())) reason = "snapshot belongs to another entity";
            else if (!expected.contains(s.getWindowId())) reason = "snapshot window is not in matched_window_ids";
            else if (s.getSourceRef() == null || !Objects.equals(s.getWindowId(), s.getSourceRef().windowId())
                    || text(s.getSourceRef().contentSha256()).isBlank()) reason = "snapshot source_ref is missing or inconsistent";
            if (reason == null) accepted.add(s);
            else snapshotWarnings.add(new PayloadQuality.Warning(s.getWindowId(), "window_snapshots", reason));
        }

        TreeSet<String> present = new TreeSet<>(), types = new TreeSet<>();
        TreeMap<String, TreeSet<String>> versions = new TreeMap<>();
        List<PayloadQuality.Warning> featureWarnings = new ArrayList<>();
        for (WindowSnapshot s : accepted) {
            present.add(s.getWindowId());
            if (s.getEntityType() != null && !s.getEntityType().isBlank()) types.add(s.getEntityType());
            else snapshotWarnings.add(new PayloadQuality.Warning(s.getWindowId(), "entity_type", "missing original entity_type"));
            versions.computeIfAbsent(s.getWindowId(), ignored -> new TreeSet<>()).add(s.getSourceRef().contentSha256());
            JsonNode features = MAPPER.valueToTree(s.getFeatures());
            for (String path : FEATURE_PATHS) {
                JsonNode value = features.at(path.substring("/features".length()));
                String reason = value.isMissingNode() ? "missing field" : value.isNull() ? "null value"
                        : !value.isNumber() ? "non-numeric value"
                        : !Double.isFinite(value.doubleValue()) ? "non-finite model value" : null;
                if (reason != null) featureWarnings.add(new PayloadQuality.Warning(s.getWindowId(), path, reason));
            }
        }
        if (types.size() > 1) snapshotWarnings.add(new PayloadQuality.Warning(null, "entity_type", "conflicting entity types: " + String.join(",", types)));
        List<String> missing = expected.stream().filter(id -> !present.contains(id)).toList();
        List<PayloadQuality.SnapshotConflict> conflicts = new ArrayList<>();
        versions.forEach((id, hashes) -> { if (hashes.size() > 1) conflicts.add(new PayloadQuality.SnapshotConflict(candidate.getEntityId(), id, new ArrayList<>(hashes))); });
        Comparator<PayloadQuality.Warning> warningOrder = Comparator.comparing((PayloadQuality.Warning w) -> text(w.windowId()))
                .thenComparing(w -> text(w.path())).thenComparing(w -> text(w.reason()));
        featureWarnings = featureWarnings.stream().distinct().sorted(warningOrder).toList();
        snapshotWarnings = snapshotWarnings.stream().distinct().sorted(warningOrder).toList();
        String status = !conflicts.isEmpty() ? "CONFLICT"
                : !missing.isEmpty() || !featureWarnings.isEmpty() || !snapshotWarnings.isEmpty() ? "INCOMPLETE" : "COMPLETE";
        PayloadQuality quality = new PayloadQuality(status, missing, featureWarnings, conflicts, snapshotWarnings);
        if (!"COMPLETE".equals(status)) LOG.warn("L2 snapshot payload entity={} windows={} status={} missing={} feature_warnings={} conflicts={} snapshot_warnings={}",
                candidate.getEntityId(), expected, status, missing, featureWarnings.size(), conflicts.size(), snapshotWarnings.size());
        return new Payload(types.size() == 1 ? types.first() : null, expected.size(), accepted, quality);
    }

    private static Map<String, String> issue(String id, String path, String reason) {
        Map<String, String> result = new LinkedHashMap<>(); result.put("window_id", id); result.put("path", path); result.put("reason", reason); return result;
    }
    private static Instant time(Object value) {
        try { return Instant.parse(text(value)); } catch (RuntimeException e) { return Instant.MAX; }
    }
    private static String text(Object value) { return value == null ? "" : value.toString(); }
    private static <T> List<T> safe(List<T> values) { return values == null ? List.of() : values; }
}
