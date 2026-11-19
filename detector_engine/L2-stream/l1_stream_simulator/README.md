# L1 Stream Simulator

新手启动 Kafka 和数据流请先阅读项目根目录的 [`启动数据流操作说明.md`](../启动数据流操作说明.md)。

用于为 L2（Flink CEP + Drools）生成确定性、可回放的 `aligned_window` 测试数据。原始事件先生成在全局时间轴，再按左闭右开区间 `[start, end)` 进入重叠窗口；窗口统计、融合特征和质量字段均由实际事件计算。

## 快速开始

从仓库根目录运行：

```powershell
python -m l1_stream_simulator.generate
python -m l1_stream_simulator.validate L1生成结果/l1_windows.jsonl
python -m l1_stream_simulator.replay L1生成结果/l1_windows.jsonl --speed 600
python -m unittest discover -s l1_stream_simulator/tests -v
```

默认配置生成 3 个实体、6 小时时间轴中的前 500 个按结束时间排序的窗口，随机种子为 42。去掉数量上限可在配置中将 `max_windows` 设为 `null`。常用覆盖参数：

```powershell
python -m l1_stream_simulator.generate --entities 10 --hours 24 --max-windows 10000 --seed 7 --output L1生成结果/full
```

生成文件：

- `L1生成结果/l1_windows.jsonl`：每行一个完整 L1 窗口，不含真值标签。
- `L1生成结果/ground_truth.jsonl`：注入场景真值。
- `L1生成结果/manifest.json`：实体、窗口、原始事件和场景分布统计。

## 回放与 Kafka

`--speed 60` 表示模拟时间 60 倍速。相同窗口结束时间的不同实体之间不等待；程序读取一行即发送一行，不预载整个文件。回放程序默认发送到本机 Kafka `localhost:9092` 的 `l1-aligned-windows` Topic，消息 Key 为 `entity_id`，消息 Value 为原始窗口 JSON。

回放程序始终通过 Kafka Producer 发送数据，不提供 stdout 回放模式。先安装 Kafka Producer 依赖并启动 Kafka：

```powershell
pip install -r l1_stream_simulator/requirements.txt
python -m l1_stream_simulator.replay L1生成结果/l1_windows.jsonl --speed 600
```

默认连接 `localhost:9092`，发送到 `l1-aligned-windows` Topic，消息 Key 为 `entity_id`，Value 为窗口 JSON。可用 `--kafka-bootstrap` 和 `--topic` 覆盖连接地址及输入 Topic。Kafka 不可连接、依赖未安装或输入 JSON 无效时，命令会失败并显示错误，不会退回到终端输出。若只需查看 JSONL 文件而不发送消息，可直接使用 `Get-Content L1生成结果/l1_windows.jsonl`。

## 独立回放控制服务

Docker Compose 中的 `l1-replay` 服务单独负责读取挂载的 JSONL 并调用本模块的 `replay()` 发送 Kafka。Dashboard 只调用它的控制接口，不加载 L1 数据文件，也不创建 Kafka Producer。窗口数据仍只通过 `l1-aligned-windows` Topic 流入 L2。

服务接口：`POST /replay`（启动任务，速度 1～10000）、`GET /replay/status`（查看状态和提交/确认数）、`GET /health`（进程健康状态；不代表 Kafka 已连接）。运行回放时若 Kafka 连接或发送失败，状态会变为 `error`。可以不启动 Dashboard，单独运行 `python -m l1_stream_simulator.service`，并通过 `http://localhost:8091` 调用这些接口。

进度中的“已提交”表示 Producer 已接收发送请求；“Kafka 已确认”表示 broker 已确认消息。回放完成会先 flush 等待确认，再报告完成状态。

## 一致性口径

- 均值、最大值、数量和比率只从当前窗口实际事件计算；无 metric/span 时相关数值为 `null` 并写入 `missing_features`。
- 原始日志、Span、告警总数保留，供兼容已有规则和下游输出；同时提供日志/分钟、Span/分钟、失败 Span/分钟、告警/分钟、日志等级比例、Span 失败率、告警等级比例和 ERROR 日志 trace 匹配比例，供模型使用。每分钟数量按窗口长度归一化。
- `relation_key` 是同时出现在日志和 Span 中的非空 `trace_id`。relation 包含该键在当前窗口的全部日志和 Span ID。
- `matched_error_log_count` / `matched_failed_span_count` 分别是关系中 ERROR 日志/失败 Span 总数；`error_failed_trace_overlap_count` 是同时含 ERROR 日志与失败 Span 的 trace_id 数；`error_log_match_ratio` 是已匹配 ERROR 日志数除以全部 ERROR 日志数。
- `quality.sample_counts` 由事件数量得出。指标采样低于窗口长度除以 30 秒时为 incomplete；无日志或 Span 时对应类型为 incomplete；告警允许零条且仍视为 complete。
- 父 Span 总在子 Span 前生成；重叠窗口复用完全相同的原始记录。

配置文件使用 JSON 语法（也是 YAML 1.2 的合法子集），因此无需额外 YAML 依赖。异常频率、强度、类型、实体数、时长、窗口大小、滑动步长、种子与输出路径均可配置。
