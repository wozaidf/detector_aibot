// 解析失败记录的模型，保存失败原因与原始 JSON 报文。
package l2.model;

import java.io.Serializable;

public record InvalidRecord(String reason, String rawJson) implements Serializable {}
