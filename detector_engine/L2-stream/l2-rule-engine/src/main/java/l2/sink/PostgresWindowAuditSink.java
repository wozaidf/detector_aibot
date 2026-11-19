package l2.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import l2.model.AlignedWindow;
import l2.model.WindowEvaluation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Persist evaluated windows; final L2 events remove their associated windows from this table. */
public final class PostgresWindowAuditSink extends RichSinkFunction<WindowEvaluation> {
    private static final ObjectMapper JSON = new ObjectMapper();
    private transient Connection connection;
    private transient PreparedStatement upsert;

    @Override public void open(OpenContext context) throws Exception {
        connection = DriverManager.getConnection(dbUrl(), env("POSTGRES_USER", "qingtian"), env("POSTGRES_PASSWORD", "2002"));
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS l2_window_audit (
                  entity_id TEXT NOT NULL, window_id TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                  window_start TIMESTAMPTZ, window_end TIMESTAMPTZ, entity_type TEXT,
                  window_snapshot JSONB NOT NULL, drools_markers JSONB NOT NULL DEFAULT '[]'::jsonb,
                  drools_candidate_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                  drools_candidate_types JSONB NOT NULL DEFAULT '[]'::jsonb,
                  matched_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
                  has_final_event BOOLEAN NOT NULL DEFAULT FALSE,
                  final_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                  l2_output_payload JSONB,
                  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (entity_id, window_id, content_sha256)
                )
                """);
            statement.execute("ALTER TABLE l2_window_audit ADD COLUMN IF NOT EXISTS l2_output_payload JSONB");
            statement.execute("DELETE FROM l2_window_audit WHERE has_final_event OR final_event_ids <> '[]'::jsonb");
            statement.execute("""
                UPDATE l2_window_audit SET l2_output_payload = jsonb_build_object(
                  'stage', 'L2', 'entity_id', entity_id, 'entity_type', entity_type,
                  'original_data', jsonb_build_object('source_refs', jsonb_build_array(window_snapshot->'source_ref'),
                    'window_snapshots', jsonb_build_array(window_snapshot)),
                  'l2_result', jsonb_build_object('matched_window_ids', jsonb_build_array(window_id),
                    'anomaly_markers', drools_markers, 'drools_candidate_event_ids', drools_candidate_event_ids,
                    'drools_candidate_types', drools_candidate_types, 'matched_rules', matched_rules,
                    'has_final_event', has_final_event, 'final_event_ids', final_event_ids))
                WHERE l2_output_payload IS NULL
                """);
            statement.execute("CREATE INDEX IF NOT EXISTS idx_l2_window_audit_time ON l2_window_audit (window_start, entity_id)");
            statement.execute("CREATE OR REPLACE VIEW l2_non_anomaly_windows AS SELECT * FROM l2_window_audit WHERE NOT has_final_event");
        }
        upsert = connection.prepareStatement("""
            INSERT INTO l2_window_audit(entity_id, window_id, content_sha256, window_start, window_end, entity_type,
              window_snapshot, drools_markers, drools_candidate_event_ids, drools_candidate_types, matched_rules, l2_output_payload)
            VALUES (?, ?, ?, NULLIF(?, '')::timestamptz, NULLIF(?, '')::timestamptz, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb)
            ON CONFLICT(entity_id, window_id, content_sha256) DO UPDATE SET
              window_start=EXCLUDED.window_start, window_end=EXCLUDED.window_end, entity_type=EXCLUDED.entity_type,
              window_snapshot=EXCLUDED.window_snapshot, drools_markers=EXCLUDED.drools_markers,
              drools_candidate_event_ids=EXCLUDED.drools_candidate_event_ids,
              drools_candidate_types=EXCLUDED.drools_candidate_types, matched_rules=EXCLUDED.matched_rules,
              l2_output_payload=jsonb_set(EXCLUDED.l2_output_payload, '{l2_result}',
                (EXCLUDED.l2_output_payload->'l2_result') || jsonb_build_object(
                  'has_final_event', l2_window_audit.has_final_event, 'final_event_ids', l2_window_audit.final_event_ids)),
              last_seen_at=now()
            """);
    }

    @Override public void invoke(WindowEvaluation evaluation, SinkFunction.Context context) throws Exception {
        AlignedWindow window = evaluation.getWindow();
        if (window == null || blank(window.getEntityId()) || blank(window.getWindowId()) || blank(window.getContentSha256()))
            throw new IllegalArgumentException("L2 window audit requires entity_id, window_id and content_sha256");
        List<?> markers = safe(evaluation.getAnomalyMarkers());
        List<?> candidateEventIds = safe(evaluation.getDroolsCandidateEventIds());
        List<?> candidateTypes = safe(evaluation.getDroolsCandidateTypes());
        String raw = window.getRawJson();
        JsonNode source = JSON.readTree(raw);
        ObjectNode snapshot = JSON.createObjectNode();
        for (String key : List.of("schema_version", "record_type", "window_id", "entity_id", "entity_type", "window", "features", "quality", "context")) {
            JsonNode value = source.get(key);
            if (value != null) snapshot.set(key, value);
        }
        JsonNode relation = source.at("/fusion_features/key_level/relation_statistics");
        if (!relation.isMissingNode()) snapshot.set("relation_statistics", relation);
        ObjectNode sourceRef = JSON.createObjectNode();
        sourceRef.put("topic", "l1-aligned-windows"); sourceRef.put("window_id", window.getWindowId());
        sourceRef.put("content_sha256", window.getContentSha256()); snapshot.set("source_ref", sourceRef);

        upsert.setString(1, window.getEntityId()); upsert.setString(2, window.getWindowId()); upsert.setString(3, window.getContentSha256());
        upsert.setString(4, window.getWindowStart()); upsert.setString(5, window.getWindowEnd()); upsert.setString(6, window.getEntityType());
        upsert.setString(7, JSON.writeValueAsString(snapshot)); upsert.setString(8, JSON.writeValueAsString(markers));
        upsert.setString(9, JSON.writeValueAsString(candidateEventIds));
        upsert.setString(10, JSON.writeValueAsString(candidateTypes));
        upsert.setString(11, JSON.writeValueAsString(safe(evaluation.getMatchedRules())));
        ObjectNode payload = JSON.createObjectNode();
        payload.put("stage", "L2"); payload.put("entity_id", window.getEntityId());
        payload.put("entity_type", window.getEntityType());
        ObjectNode original = payload.putObject("original_data");
        original.putArray("source_refs").add(sourceRef);
        original.putArray("window_snapshots").add(snapshot);
        ObjectNode result = payload.putObject("l2_result");
        result.putArray("matched_window_ids").add(window.getWindowId());
        result.set("anomaly_markers", JSON.valueToTree(safe(evaluation.getAnomalyMarkers())));
        result.set("drools_candidate_event_ids", JSON.valueToTree(safe(evaluation.getDroolsCandidateEventIds())));
        result.set("drools_candidate_types", JSON.valueToTree(safe(evaluation.getDroolsCandidateTypes())));
        result.set("matched_rules", JSON.valueToTree(safe(evaluation.getMatchedRules())));
        result.put("has_final_event", false); result.putArray("final_event_ids");
        upsert.setString(12, JSON.writeValueAsString(payload)); upsert.executeUpdate();
    }

    @Override public void close() throws Exception {
        if (upsert != null) upsert.close();
        if (connection != null) connection.close();
    }

    static String dbUrl() {
        String host = env("POSTGRES_HOST", "postgres"); String port = env("POSTGRES_PORT", "5432");
        String database = env("POSTGRES_DB", "anomaly_db"); return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }
    private static String env(String key, String fallback) { String value = System.getenv(key); return value == null || value.isBlank() ? fallback : value; }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static List<?> safe(Iterable<?> values) { List<Object> result = new ArrayList<>(); if (values != null) values.forEach(result::add); return result; }
}
