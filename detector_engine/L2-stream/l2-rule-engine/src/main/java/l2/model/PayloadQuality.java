package l2.model;

import java.io.Serializable;
import java.util.List;

/** 新增载荷的质量，不参与既有报警或严重度判断。 */
public record PayloadQuality(
        String snapshotStatus, List<String> missingWindowIds,
        List<Warning> featureWarnings, List<SnapshotConflict> snapshotConflicts,
        List<Warning> snapshotWarnings) implements Serializable {
    public record Warning(String windowId, String path, String reason) implements Serializable {}
    public record SnapshotConflict(String entityId, String windowId, List<String> contentSha256s) implements Serializable {}
}
