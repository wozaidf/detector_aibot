# L2 规则检测与统一决策

本模块从 Kafka `l1-aligned-windows` 读取既有 L1 `aligned_window` JSON，不改变 L1 输入结构。Flink 按 `window.end` 分配事件时间并以 `entity_id` 分区；Drools 为每个窗口产生异常标记或单窗口候选事件，所有窗口（包括无候选事件的窗口）继续流向 CEP。CEP 按同一实体和事件时间顺序读取 Drools 标记，只将满足连续条件的窗口序列聚合为候选事件。Drools 与 CEP 候选经过关联、合并、去重和统一决策后写入 `l2-anomaly-events`。

基线：Java 17、Flink 2.2.1、Kafka Connector 5.0.0-2.2、Drools 10.1.0。

## 实现位置

- `src/main/java/l2/rules/WindowEvaluationFunction.java`：每个 L1 窗口执行 Drools，主流输出内部窗口标记，侧输出严重单指标及联合规则候选。
- `src/main/resources/rules/window-rules.drl`：普通阈值仅生成标记；严重单指标阈值和现有网络、日志/Trace、多信号联合规则生成候选。
- `src/main/java/l2/model/WindowEvaluation.java`：内部适配信封，携带原 L1 窗口对象和 Drools 标记，不改变外部 L1 JSON。
- `src/main/java/l2/rules/L1WindowReplayDeduplicator.java`：同实体、同窗口 ID 的重复 L1 消息先行去重；内容哈希冲突会记录错误日志。
- `src/main/java/l2/cep/CepDetector.java`：基于同实体标记序列与事件时间执行 CEP；连续标记序列中任何未标记的相邻窗口都会打断匹配。最大连续段贪婪匹配并跳过已匹配尾窗口，避免滑动窗口的重叠匹配重复报警。
- `src/main/java/l2/rules/CandidateFusion.java`：确定性关联、合并及稳定 ID。
- `src/main/java/l2/rules/FinalDecisionFunction.java`：基于事件时间封口信号融合候选，执行维护期决策、稳定事件 ID 与状态去重。
- `src/main/java/l2/model/FusionInput.java`：内部融合信封，可携带候选事件、CEP 序列封口范围或两者；不改变 Kafka L1 输入和最终事件 JSON。
- `src/main/java/l2/model/AnomalyEvent.java`：最终 JSON Schema。
- `src/main/java/l2/sink/PostgresWindowAuditSink.java` 与 `PostgresFinalEventLinkSink.java`：将所有 Drools 评估窗口（包括带普通标记但未形成最终事件的窗口）写入 PostgreSQL；最终 L2 事件关联到的窗口会从该表删除。

## 看板指标口径

看板顶部只展示五项：L1 输入窗口数、Drools 普通阈值标记窗口数、Drools 候选事件数、CEP 候选事件数、最终 `anomaly=true` 的 L2 事件数。Drools 标记窗口数由 `l2.drools.ordinary_marked_windows` 计数；窗口命中一个或多个普通阈值均只计一次，且即使该窗口同时生成严重/联合候选仍计入。普通阈值指 CPU、内存、延迟、丢包率、日志错误率、Span 失败率，不包含 `REPEATED_ERRORS` 和 `ALARM_PRESENT` 标记。CEP 卡片只显示 `l2.cep.matched_sequences` 候选事件数，不展示 CEP 算子指标。看板后端按 Flink 指标名跨作业顶点查找计数，不依赖算子展示名称。

## 关联、合并和决策

候选必须先满足相同 `entity_id` 与相同 `anomaly_type`。当两侧都有窗口 ID 时，要求窗口 ID 有交集且事件时间区间重叠；时间重叠本身不够。当任一侧缺少窗口 ID 时，才回退到同实体、相同异常类型、有效时间区间重叠，并要求开始时间差不超过 `L2_ASSOCIATION_WINDOW_SECONDS`。不按相似异常名称或不同实体合并。多个 Drools 规则同窗命中也可合并，以规则集合和来源证据分别保留。

仅一侧命中的候选继续输出，`merge_status=SINGLE_SOURCE`；Drools 与 CEP 匹配的候选输出为 `MERGED_DROOLS_CEP`，`decision=CORROBORATED`。严重度保留候选原严重度中的最高等级；双引擎佐证不会自动提升严重度或生成概率。维护期抑制规则照常生效。滑动窗口 CEP 使用贪婪连续段匹配和 `skipPastLastEvent`，而不是把不同窗口只因时间重叠合并。

Drools 候选不会再按固定处理时间延迟输出。CEP 对每段连续标记序列在遇到不连续/未标记窗口时发送封口信号（即使该段短于 CEP 报警窗口数，也会发送封口）；达到 `L2_CEP_MAX_SEQUENCE_SECONDS` 的开放序列通过 CEP 超时处理封口。融合算子按同实体、同异常类型、窗口 ID/时间范围匹配候选，并将匹配到的 Drools 与 CEP 候选暂存至封口事件时间加 `L2_WATERMARK_SECONDS` 后输出；`L2_WATERMARK_SECONDS` 同时是乱序容忍和融合迟到余量。无对应 CEP 序列的单侧候选在自身结束时间加迟到余量后输出。超过水印迟到余量到达的数据按迟到数据处理，不保证与已输出事件重新合并。

相同 L1 窗口重放会先按实体、窗口 ID 与内容 SHA-256 去重；相同候选重放会生成相同 source/final event ID；`L2_DEDUP_SECONDS` 内重复最终 ID 不再次写入，并通过 Flink 日志记录审计信息。不同窗口 ID 的独立事件不会仅因重叠时间被抑制。

## 严重阈值依据

普通阈值保留从原配置迁移后的现有值。新增严重单指标阈值默认为：CPU 均值 95%、内存均值 92%、延迟均值 220ms、丢包率 0.05、日志错误率 0.60、Span 失败率 0.80。工作区 `L1生成结果/l1_windows.jsonl` 当前 500 个窗口的经验分布分别为 CPU P99/最大值 93.38/96.94，内存 81.71/92.24，延迟 207.78/228.33，丢包率 0.0513/0.0562，日志错误率 0.571/0.571，Span 失败率 0.80/0.80。CPU、内存和延迟严重值高于观测 P99；丢包率按 5% 的明显网络退化设定；错误率 60% 是保守的极端错误比例；Span 失败率 80% 对应当前样本中最高尾部。它们是工程初值，不是生产环境校准结果。需结合真实业务 SLO 和更多正常/故障窗口再校准。

## 最终事件 JSON

L2 输出统一的 `ANOMALY_EVENT` 外层结构。原 L1 来源字段放在 `original_data`，L2 检测结果放在 `l2_result`；当前阶段的 `l25_result` 和 `l4_result` 为 `null`。顶层不输出 `schema_version`。事件 ID、检测来源、规则证据、严重度、决策、融合和去重算法保持不变。

`created_at` 和 `updated_at` 记录 L2 输出异常事件的时间；当前 L2 在最终输出时生成两者。`detected_at` 保留在 `l2_result`，表示现有 L2 检测时间字段。

`original_data.window_snapshots` 仍保留事件实际关联的窗口快照，不将多窗口特征合并，也不复制原始事件大数组。缺失或冲突仍通过 `l2_result.payload_quality` 说明。

- [字段映射与时间语义](docs/event-contract-2.1.md)
- [可校验 JSON Schema](docs/l2-anomaly-event.schema.json)
- [单窗口完整 JSON 示例](docs/examples/l2-single-event.json)
- [CEP 多窗口完整 JSON 示例](docs/examples/l2-cep-event.json)

`event_id` 仍由实体、异常类型、匹配窗口和候选 ID 稳定计算；`source_refs.content_sha256` 仍表示原输入 JSON。Kafka Key 继续使用 entity_id，Sink 继续为 AT_LEAST_ONCE，Topic 保留策略由部署配置决定，本次没有调整。

## PostgreSQL 窗口审计

`l2_window_audit` 保存所有 Drools 评估过、且尚未关联最终 L2 异常事件的单窗口快照；普通标记或 Drools 候选本身不会阻止写入。最终事件 Sink 按 `entity_id + window_id + content_sha256` 删除已关联窗口。快照保留 Drools 标记、候选 ID/类型及命中规则，便于区分无标记窗口与“有标记但未形成最终事件”的窗口。重复输入使用该三元组幂等 upsert。`l2_non_anomaly_windows` 视图只筛选未关联最终事件的窗口。Sink 启动时会清理旧版遗留的 `has_final_event=true` 或 `final_event_ids` 非空记录。

`l2_output_payload` 保留逐窗口 JSON 结构：顶层包含 `stage=L2`、`entity_id`、`entity_type`，`original_data.source_refs/window_snapshots` 保存原窗口及来源，`l2_result` 保存 Drools 标记、候选 ID/类型、命中规则和最终事件关联状态。未形成异常事件的窗口没有正式的异常事件 ID、严重度或决策，因此不生成这些字段。此调整不改动 Kafka 的 L2 异常事件格式。

Compose 中 L2 的 JobManager/TaskManager 等待 PostgreSQL 健康后启动，数据库连接通过 `POSTGRES_HOST`、`POSTGRES_PORT`、`POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD` 配置。L2 会在 Sink 启动时幂等创建表和视图。数据库写入失败会使 Flink Sink 失败并由作业恢复机制重试；Kafka 输出仍写原 Topic。

## 配置

| 环境变量                                                                      | 默认值 | 用途 |
|---------------------------------------------------------------------------|---:|---|
| `L1_OUTPUT_TOPIC`                                                         | `l1-aligned-windows` | L1 Kafka 输入 |
| `L2_OUTPUT_TOPIC`                                                         | `l2-anomaly-events` | 融合后的 L2 JSON 输出 |
| `L2_CPU_HIGH_PCT` / `L2_MEMORY_HIGH_PCT`                                  | Compose 90 / 90 (Java default 80 / 85) | CPU、内存普通标记阈值 |
| `L2_LATENCY_HIGH_MS` / `L2_PACKET_LOSS_HIGH_RATIO`                        | Compose 200 / 0.03 (Java default 160 / 0.03) | 延迟、丢包普通单指标标记阈值 |
| `L2_LOG_ERROR_RATE` / `L2_SPAN_FAILURE_RATE`                              | 0.30 / 0.30 | 日志、Span 普通单指标标记阈值 |
| `L2_CPU_SEVERE_PCT`                                                       | 95 | CPU 严重阈值 |
| `L2_MEMORY_SEVERE_PCT`                                                    | 92 | 内存严重阈值 |
| `L2_LATENCY_SEVERE_MS`                                                    | 220 | 延迟严重阈值 |
| `L2_PACKET_LOSS_SEVERE_RATIO`                                             | 0.05 | 丢包严重阈值 |
| `L2_LOG_ERROR_SEVERE_RATE`                                                | 0.60 | 日志错误率严重阈值 |
| `L2_SPAN_FAILURE_SEVERE_RATE`                                             | 0.80 | Span 失败率严重阈值 |
| `L2_JOINT_NETWORK_LATENCY_MS` / `L2_JOINT_NETWORK_PACKET_LOSS_RATIO`      | rules.env 200 / 0.03 (Java fallback 160 / 0.03) | 网络联合规则：延迟与丢包必须同时达到阈值 |
| `L2_JOINT_LOG_ERROR_RATE` / `L2_JOINT_SPAN_FAILURE_RATE`                  | 0.30 / 0.30 | 日志-Trace 联合规则的独立比率阈值 |
| `L2_JOINT_LOG_TRACE_MIN_OVERLAP_COUNT`                                    | 1 | 日志-Trace 联合规则要求的最少关联数 |
| `L2_JOINT_MULTI_CPU_PCT`                                                  | rules.env 90 (Java fallback 80) | 多信号联合规则 CPU 阈值 |
| `L2_JOINT_MULTI_MIN_FAILED_SPAN_COUNT` / `L2_JOINT_MULTI_MIN_ALARM_COUNT` | 1 / 1 | 多信号联合规则要求的最少失败 Span 数与告警数 |
| `L2_CEP_OCCURRENCES`                                                      | 3 | CEP 最少连续标记窗口数 |
| `L2_CEP_COMPLEX_HORIZON_SECONDS`                                          | 600 | CEP CPU/Span 后接告警的两窗口关联最大事件时间跨度（秒） |
| `L2_ASSOCIATION_WINDOW_SECONDS`                                           | 300 | 缺少窗口 ID 时的时间关联窗口 |
| `L2_CEP_MAX_SEQUENCE_SECONDS`                                             | 604800（7 天） | CEP 连续段最大时长；到期时超时封口，避免开放序列长期占用状态 |
| `L2_DEDUP_SECONDS`                                                        | 300 | 稳定最终事件 ID 的重放去重时限 |

在 Compose 部署中，配置位于 `config/l2/rules.env`。看板的“联合规则阈值 · Drools 候选”分组对应上表的 `L2_JOINT_*` 项；这些配置与普通单指标阈值独立。默认值沿用当前联合规则的有效门槛，因此不改变默认检测行为。通过页面保存后，重新创建 L2 作业使配置生效。

## 构建和验证

```powershell
cd .\L2-stream\l2-rule-engine
mvn test
mvn package
```

使用 Compose 启动本地验证环境：

```powershell
cd .\L2-stream
docker compose up -d --build
python -m l1_stream_simulator.replay L1生成结果/l1_windows.jsonl --speed 60 --kafka-bootstrap localhost:9092 --topic l1-aligned-windows
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic l2-anomaly-events --from-beginning
```

Compose 只启动 L2 所需的 Kafka 输入、L2 Flink 作业和看板；L2 输入 Topic 为 `l1-aligned-windows`，最终输出 Topic 为 `l2-anomaly-events`。本地验证不需要真实生产 Kafka。
