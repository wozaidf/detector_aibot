package l2.sink;

import l2.model.AnomalyEvent;
import l2.model.DetectionCandidate;
import l2.model.WindowSnapshot;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/** Removes windows associated with final L2 events from the non-event window table. */
public final class PostgresFinalEventLinkSink extends RichSinkFunction<AnomalyEvent> {
    private transient Connection connection;
    private transient PreparedStatement delete;

    @Override public void open(OpenContext context) throws Exception {
        connection = DriverManager.getConnection(PostgresWindowAuditSink.dbUrl(), env("POSTGRES_USER", "qingtian"), env("POSTGRES_PASSWORD", "2002"));
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
            statement.execute("CREATE OR REPLACE VIEW l2_non_anomaly_windows AS SELECT * FROM l2_window_audit WHERE NOT has_final_event");
        }
        delete = connection.prepareStatement(
                "DELETE FROM l2_window_audit WHERE entity_id=? AND window_id=? AND content_sha256=?");
    }

    @Override public void invoke(AnomalyEvent event, SinkFunction.Context context) throws Exception {
        if (event == null || event.originalData() == null || event.originalData().windowSnapshots() == null) return;
        for (WindowSnapshot snapshot : event.originalData().windowSnapshots()) {
            if (snapshot == null || snapshot.getSourceRef() == null || snapshot.getWindow() == null) continue;
            DetectionCandidate.SourceRef sourceRef = snapshot.getSourceRef();
            String hash = sourceRef.contentSha256();
            if (blank(event.entityId()) || blank(snapshot.getWindowId()) || blank(hash))
                throw new IllegalArgumentException("L2 final event link requires entity_id, window_id and content_sha256");
            delete.setString(1, event.entityId());
            delete.setString(2, snapshot.getWindowId());
            delete.setString(3, hash);
            delete.executeUpdate();
        }
    }

    @Override public void close() throws Exception {
        if (delete != null) delete.close();
        if (connection != null) connection.close();
    }
    private static String env(String key, String fallback) { String value = System.getenv(key); return value == null || value.isBlank() ? fallback : value; }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
}
