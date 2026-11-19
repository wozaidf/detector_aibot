# L2.5 IForest Kafka 预测服务

该目录是独立常驻推理服务：消费统一外层格式的 L2 异常事件，按训练时共享的 23 列特征 schema 对每个原窗口单独评分，并将保留上游字段、包含 `l25_result` 的事件写入输出 Topic。不读取本地 L1 文件，也不负责模型训练。

## 目录结构

- `main.py`：加载模型、消费消息、调用预测、发布结果并提交 offset。
- `config.py`：通过 `get_settings()` 统一读取 `.env` 和容器环境变量。
- `kafka_stream/`：Kafka Consumer、Producer 与 offset 提交。
- `inference/predictor.py`：只接收特征表并返回分数和阈值判断。
- `inference/window_adapter.py`：读取 `original_data.window_snapshots`，提取模型特征并构造 L2.5 结果。
- `inference/feature_schema.py`：训练和推理共同使用的特征定义。
- `models/`：已训练模型包及其特征清单。
- `iforest_tool.py`：保留旧模型包序列化引用的兼容类名。

## Kafka 数据流

输入默认是 `l2-anomaly-events`。模块从 `original_data.window_snapshots` 读取窗口，并使用 `l2_result.matched_window_ids` 选择窗口。不检查事件类型或特征 schema 版本。每个窗口独立评分，结果汇总在一个外层事件的 `l25_result.window_results` 数组中。无法评分的窗口 ID 记录在 `unscored_window_ids`；缺失或无效特征由训练好的预处理器按训练中位数处理，并在对应窗口的 `input_warnings` 中说明。

输出保留 L2 的顶层 `event_id`、`original_data`、`l2_result` 等字段；将 `stage` 更新为 `L25`、更新 `updated_at`，写入 `l25_result`，并令 `l4_result` 为 `null`。模型名为 `PYOD_IFOREST`，输入阶段为 `L2`。原始数据和 L2 结果不做修改。

输入范围是 L2 事件关联的窗口，不是所有 L1 窗口；模块不会独立扫描未被 L2 事件引用的正常窗口。

每个有效输入事件按其有效窗口数产生多条逐窗口评分，包括 `anomaly=true` 和 `anomaly=false`。结果含稳定的 `event_id`、`entity_id`、原窗口引用、`anomaly_score`、阈值、输入特征证据和决策理由。IForest 分数是异常评分，不代表故障概率，也不转换为严重度。Producer 继续使用 `entity_id` 作为 Kafka message key。

Consumer 关闭自动提交。服务先等待输出 Topic 确认，再提交该输入消息的 offset；异常退出时未提交的消息会重放，输出 `event_id` 可供下游识别重复。无效 JSON 或缺少必需窗口字段的消息会写入服务日志并提交跳过，不会静默丢失，也不额外创建错误 Topic。

## 配置

复制 `.env.example` 为 `.env`，按部署环境调整。配置由 `get_settings()` 读取：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker 地址 |
| `L25_INPUT_TOPIC` | `l2-anomaly-events` | 单一输入 Topic；可设为 `l2-anomaly-events` 或 `l4-anomaly-decisions` |
| `L25_OUTPUT_TOPIC` | `l25-iforest-events` | 预测结果输出 Topic |
| `L25_GROUP_ID` | `l25-group-1` | 独立消费组 |
| `L25_AUTO_OFFSET_RESET` | `earliest` | 新消费组起始位置 |
| `IFOREST_MODEL_PATH` | `models/iforest_csv_bundle.joblib` | 模型包路径 |

已存在消费组的 offset 优先于 `auto_offset_reset`。要重放历史窗口，可配置新的 `L25_GROUP_ID`，或显式重置该消费组的 offset。

### 选择实验组合

L2.5 输入 Topic 默认固定为 `l2-anomaly-events`，从该 Topic 消费 L2 事件，并将结果写到 `L25_OUTPUT_TOPIC`（通常为 `l25-iforest-events`）。

## Compose 启动

从项目根目录执行：

```powershell
Copy-Item .\L2-iforest\.env.example .\L2-iforest\.env
docker compose build l25-iforest
docker compose up -d l25-iforest
docker compose logs -f l25-iforest
```

Compose 初始化时会创建 `l25-iforest-events`。本地运行需在 `L2-iforest/.env` 中将 broker 地址设为 `localhost:9092`，并安装 `requirements.txt` 中的依赖后执行 `python main.py`。

## 更新模型

完成训练和验证后，从项目根目录运行 `L2-iforest-training/export_model.ps1`，将新模型包复制到 `L2-iforest/models/`。重新构建并重启 `l25-iforest` 容器后加载新模型。
