// Drools 与 CEP 候选事件的确定性关联、合并及稳定事件 ID 生成工具。
package l2.rules;

import l2.model.DetectionCandidate;
import l2.contract.WindowSnapshotSupport;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Deterministic association and merge rules for Drools and CEP candidates. */
public final class CandidateFusion {
    private CandidateFusion() {}

    public static boolean shouldAssociate(DetectionCandidate a, DetectionCandidate b, int associationWindowSeconds) {
        if (!same(a.getEntityId(), b.getEntityId()) || !same(a.getAnomalyType(), b.getAnomalyType())) return false;
        Set<String> aIds = nonEmpty(a.getMatchedWindowIds()), bIds = nonEmpty(b.getMatchedWindowIds());
        boolean overlaps = overlaps(a.getStartTime(), a.getEndTime(), b.getStartTime(), b.getEndTime());
        if (!aIds.isEmpty() && !bIds.isEmpty()) return overlaps && aIds.stream().anyMatch(bIds::contains);
        if (!overlaps) return false;
        try {
            return Math.abs(Instant.parse(a.getStartTime()).toEpochMilli() - Instant.parse(b.getStartTime()).toEpochMilli())
                    <= associationWindowSeconds * 1000L;
        } catch (RuntimeException e) { return false; }
    }

    public static DetectionCandidate merge(DetectionCandidate a, DetectionCandidate b) {
        DetectionCandidate out = new DetectionCandidate();
        out.setEntityId(a.getEntityId()); out.setAnomalyType(a.getAnomalyType());
        out.setSeverity(rank(a.getSeverity()) >= rank(b.getSeverity()) ? a.getSeverity() : b.getSeverity());
        out.setStartTime(minTime(a.getStartTime(), b.getStartTime())); out.setEndTime(maxTime(a.getEndTime(), b.getEndTime()));
        out.setMaintenanceActive(a.isMaintenanceActive() || b.isMaintenanceActive());
        out.setQualityStatus("partial".equals(a.getQualityStatus()) || "partial".equals(b.getQualityStatus()) ? "partial" : "complete");
        out.setDecision("ALERT"); out.setSuppressed(a.isSuppressed() || b.isSuppressed());
        out.setDuplicate(a.isDuplicate() || b.isDuplicate());
        LinkedHashSet<String> sources = new LinkedHashSet<>(); sources.addAll(safe(a.getDetectionSources())); sources.addAll(safe(b.getDetectionSources()));
        LinkedHashSet<String> rules = new LinkedHashSet<>(); rules.addAll(safe(a.getMatchedRules())); rules.addAll(safe(b.getMatchedRules()));
        LinkedHashSet<String> windows = new LinkedHashSet<>(); windows.addAll(safe(a.getMatchedWindowIds())); windows.addAll(safe(b.getMatchedWindowIds()));
        out.setDetectionSources(new ArrayList<>(sources)); out.setMatchedRules(new ArrayList<>(rules)); out.setMatchedWindowIds(new ArrayList<>(windows));
        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        sourceIds.addAll(a.getSourceEventIds() == null || a.getSourceEventIds().isEmpty() ? List.of(sourceEventId(a)) : a.getSourceEventIds());
        sourceIds.addAll(b.getSourceEventIds() == null || b.getSourceEventIds().isEmpty() ? List.of(sourceEventId(b)) : b.getSourceEventIds());
        out.setSourceEventIds(new ArrayList<>(sourceIds));
        LinkedHashMap<String, DetectionCandidate.SourceRef> refs = new LinkedHashMap<>();
        for (var ref : concat(a.getSourceRefs(), b.getSourceRefs())) refs.putIfAbsent(ref.topic() + "|" + ref.windowId() + "|" + ref.contentSha256(), ref);
        out.setSourceRefs(new ArrayList<>(refs.values()));
        Map<String, Object> evidence = new LinkedHashMap<>(); evidence.put("DROOLS_AND_CEP_CANDIDATES", List.of(a.getEvidence(), b.getEvidence())); out.setEvidence(evidence);
        Map<String, Object> sourceEvidence = new LinkedHashMap<>();
        appendSourceEvidence(sourceEvidence, a); appendSourceEvidence(sourceEvidence, b); out.setSourceEvidence(sourceEvidence);
        WindowSnapshotSupport.mergeInto(out, a); WindowSnapshotSupport.mergeInto(out, b);
        return out;
    }

    public static List<DetectionCandidate> mergeAll(List<DetectionCandidate> candidates, int associationWindowSeconds) {
        List<DetectionCandidate> ordered = new ArrayList<>(candidates);
        ordered.sort(Comparator.comparing(CandidateFusion::sourceEventId));
        Map<String, List<DetectionCandidate>> payloadVersions = new LinkedHashMap<>();
        for (DetectionCandidate next : ordered) payloadVersions.computeIfAbsent(sourceEventId(next), ignored -> new ArrayList<>()).add(next);
        List<DetectionCandidate> result = new ArrayList<>();
        Set<String> seenCandidates = new LinkedHashSet<>();
        for (DetectionCandidate next : ordered) {
            if (!seenCandidates.add(sourceEventId(next))) continue;
            for (DetectionCandidate version : payloadVersions.get(sourceEventId(next))) WindowSnapshotSupport.mergeInto(next, version);
            int index = -1;
            for (int i = 0; i < result.size(); i++) if (shouldAssociate(result.get(i), next, associationWindowSeconds)) { index = i; break; }
            if (index < 0) result.add(next); else result.set(index, merge(result.get(index), next));
        }
        return result;
    }

    public static String stableFinalEventId(DetectionCandidate c) {
        List<String> sourceIds = c.getSourceEventIds() == null || c.getSourceEventIds().isEmpty()
                ? List.of(sourceEventId(c)) : c.getSourceEventIds();
        return "L2-" + sha256(String.join("|", c.getEntityId(), c.getAnomalyType(),
                String.join(",", safe(c.getMatchedWindowIds()).stream().sorted().toList()),
                String.join(",", sourceIds.stream().sorted().toList()))).substring(0, 20).toUpperCase();
    }

    public static String sourceEventId(DetectionCandidate c) {
        String source = safe(c.getDetectionSources()).stream().sorted().findFirst().orElse("UNKNOWN");
        String key = String.join("|", c.getEntityId(), c.getAnomalyType(), source,
                String.join(",", safe(c.getMatchedWindowIds()).stream().sorted().toList()),
                String.join(",", safe(c.getMatchedRules()).stream().sorted().toList()));
        return "SRC-" + sha256(key).substring(0, 20).toUpperCase();
    }

    private static void appendSourceEvidence(Map<String, Object> target, DetectionCandidate c) {
        Map<String, Object> current = c.getSourceEvidence() == null || c.getSourceEvidence().isEmpty()
                ? Map.of(String.join("+", safe(c.getDetectionSources())), c.getEvidence()) : c.getSourceEvidence();
        current.forEach((k, v) -> {
            if (!target.containsKey(k)) target.put(k, v);
            else {
                Object old = target.get(k);
                List<Object> values = old instanceof List<?> list ? new ArrayList<>(list) : new ArrayList<>(List.of(old));
                values.add(v); target.put(k, values);
            }
        });
    }
    private static boolean overlaps(String as, String ae, String bs, String be) {
        try { return !Instant.parse(as).isAfter(Instant.parse(be)) && !Instant.parse(bs).isAfter(Instant.parse(ae)); }
        catch (RuntimeException e) { return false; }
    }
    private static String minTime(String a, String b) { return Instant.parse(a).isBefore(Instant.parse(b)) ? a : b; }
    private static String maxTime(String a, String b) { return Instant.parse(a).isAfter(Instant.parse(b)) ? a : b; }
    private static int rank(String severity) { return switch (severity == null ? "" : severity.toLowerCase()) { case "critical" -> 4; case "major" -> 3; case "warning" -> 2; case "minor" -> 1; default -> 0; }; }
    private static boolean same(String a, String b) { return a != null && !a.isBlank() && a.equals(b); }
    private static List<String> safe(List<String> v) { return v == null ? List.of() : v; }
    private static Set<String> nonEmpty(List<String> v) { return new LinkedHashSet<>(safe(v)); }
    private static List<DetectionCandidate.SourceRef> concat(List<DetectionCandidate.SourceRef> a, List<DetectionCandidate.SourceRef> b) { List<DetectionCandidate.SourceRef> r = new ArrayList<>(a == null ? List.of() : a); r.addAll(b == null ? List.of() : b); return r; }
    private static String sha256(String value) {
        try { return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
