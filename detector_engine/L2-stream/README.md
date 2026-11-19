# L2 规则异常检测

项目由 L1 回放器、L2 Flink 作业和 Dashboard 组成，三个模块通过 Kafka 传输窗口与事件数据；Dashboard 只通过 HTTP 控制 L1 回放，不直接产生 Kafka 消息。工作区根目录的 `docker-compose.yml` 编排这些服务。Kafka 流程只使用两个业务 Topic：

- `l1-aligned-windows`：L1 对齐窗口，作为 L2 输入。
- `l2-anomaly-events`：Drools 与 CEP 融合、去重和分级后的 L2 异常事件输出。

L1 生成 JSONL 窗口并由独立回放服务发送到输入 Topic；L2 从输入 Topic 消费，在 Flink 内部完成 Drools、CEP、融合和最终决策，再写入输出 Topic。Dashboard 消费两个 Topic 供监控展示。

Kafka、L1 回放服务、L2 Flink 作业和看板的配置见 `docker-compose.yml`。Kafka 初始化服务只创建以上两个 Topic。L2 规则配置位于 `config/l2/rules.env`。

在工作区根目录 PowerShell 启动全部服务：

```powershell
docker compose up -d --build
```

单独启动 Kafka 与 L2：

```powershell
docker compose up -d kafka kafka-ui kafka-init jobmanager taskmanager
```

在 Dashboard 点击“回放 500 条样本”，或直接请求 L1 控制服务（Compose 内部地址 `http://l1-replay:8091`）。在宿主机使用原有 CLI 时，先进入本目录：

```powershell
Set-Location .\L2-stream
python -m pip install -r .\l1_stream_simulator\requirements.txt
python -m l1_stream_simulator.replay .\L1生成结果\l1_windows.jsonl --speed 600 --kafka-bootstrap localhost:9092
```

Kafka UI：<http://localhost:8080>；Dashboard：<http://localhost:8090>；Flink UI：<http://localhost:8081>。容器内 Kafka 地址为 `kafka:29092`，宿主机为 `localhost:9092`。

## 数据保留与排查

当前 `docker-compose.yml` 以 `cleanup.policy=compact` 创建两个 Topic，且消息 Key 是 `entity_id`。压缩会逐步保留每个 Key 的最新值，不适合依赖完整历史窗口或历史异常记录的回放与审计。`--if-not-exists` 只影响新建 Topic，不会修改已存在 Topic 的配置。先只读检查当前 Topic 配置：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:29092 --entity-type topics --entity-name l1-aligned-windows --describe
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:29092 --entity-type topics --entity-name l2-anomaly-events --describe
```

若需要完整历史，建议评估将两个 Topic 改为按时间保留（`cleanup.policy=delete`），并先确定 `retention.ms`。修改既有 Topic 要通过 Kafka topic 配置命令单独完成；本项目未自动更改线上已有 Topic。Kafka 服务当前未声明数据 Volume，容器重建可能丢失其可写层数据；如需持久化，应先确认 Kafka 实际 `log.dirs` 再规划挂载，避免把空目录挂到已有数据目录。

L2 Sink 把 Flink 记录时间作为 Kafka 消息时间戳传递。对历史窗口回放时，记录时间可能早于当前时间；在 Kafka 使用 `CreateTime` 的默认设置下，需评估时间戳与日志保留策略对历史记录过期的影响。当前改造没有修改 Topic 策略、消息时间戳或数据内容。PostgreSQL 服务和数据 Volume 保持原配置。

常见问题：Dashboard 显示 L1 回放服务不可用时，检查 `docker compose ps l1-replay` 与 `docker compose logs l1-replay`；Kafka 不可用时查看 `docker compose logs kafka`；L2 无作业时查看 `docker compose logs jobmanager taskmanager`，并确认输入 Topic 有消息。
