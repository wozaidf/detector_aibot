# L2 统一异常事件输出结构

本次只重排 Kafka 最终 JSON 的字段结构，不改 L1 解析、Drools 规则、CEP 匹配、候选融合、事件 ID、去重、分级、Kafka Topic 或消息 Key。

## 顶层字段

| 字段 | 取值/来源 |
|---|---|
| `event_id` | 既有稳定 L2 事件 ID，生成规则不变 |
| `event_type` | 固定 `ANOMALY_EVENT` |
| `stage` | 固定 `L2` |
| `entity_id` / `entity_type` | 原事件实体及关联快照设备类型 |
| `created_at` | L2 输出异常事件时的时间 |
| `updated_at` | 本次 L2 最终输出处理完成时间；当前事件只输出一次，与 `created_at` 同时生成 |
| `original_data` | `feature_schema_version`、`source_refs`、`window_snapshots` |
| `l2_result` | 原 L2 检测结果字段，见下表 |
| `l25_result` / `l4_result` | 当前阶段固定为 JSON `null` |

顶层不再输出原 `schema_version`。原 L1 窗口快照中的 `schema_version` 是源数据字段，仍保留在 `original_data.window_snapshots` 内。

## `l2_result`

包含 `anomaly`、`anomaly_type`、`severity`、`start_time`、`end_time`、`detected_at`、`detection_sources`、`matched_rules`、`matched_window_ids`、`source_event_ids`、`evidence`、`source_evidence`、`merge_status`、`decision_reason`、`decision`、`window_count`、`quality_status` 和 `payload_quality`。这些字段沿用原值与原语义，仅改变 JSON 层级。

`detected_at` 沿用原 L2 输出时间字段；`created_at` 和 `updated_at` 是外层事件生命周期时间，当前均记录 L2 输出该异常事件的时间。L2 当前候选没有独立的检测生成时间，因此不会伪造另一种检测时间。

## 原始数据与快照

`original_data.feature_schema_version` 当前为 `l1-window-features-v1`。`source_refs` 和 `window_snapshots` 来自实际关联的原始 L1 窗口，快照仍保留原窗口结构，包括其自身 `schema_version`、特征树、质量、上下文和来源摘要。不会从没有来源的输入补造字段。

`l2_result.window_count`、`matched_window_ids`、质量状态和证据仍由原 L2 逻辑生成。CEP 多窗口事件保留逐窗口快照，不做特征平均或合并。滑动窗口统计可能重复覆盖同一原始日志或 Span，计数不能解释为去重后的唯一原始记录数。

## 示例、Schema 和校验

- [单窗口示例](examples/l2-single-event.json)
- [CEP 多窗口示例](examples/l2-cep-event.json)
- [JSON Schema](l2-anomaly-event.schema.json)
- `python docs/validate_examples.py` 校验两个示例、嵌套载荷和窗口关系。

示例使用合成测试数据，不代表实际运行结果。该格式改变了字段路径；下游若依赖旧的顶层 L2 字段，需要按新路径读取，例如 `severity` 改为 `l2_result.severity`，窗口引用改为 `original_data.window_snapshots`。
