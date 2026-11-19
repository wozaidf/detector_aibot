# L4 JEV 事件筛选

## Laya 模型文件

`L4-Jev/laya/models/` 已加入项目根目录的 `.gitignore`，其中的模型文件不会上传到 Gitee。运行或构建依赖 Laya 模型的 L4 服务前，需要通过部署环境或人工方式将模型放到以下路径：

```text
L4-Jev/laya/models/model.safetensors
```

仓库不包含该模型文件；缺少模型时，依赖本地 Laya 模型的功能无法正常加载。

L4 从 `L4_CONSUMER_TOPIC` 消费 L2.5 事件，将 `original_data`、`l2_result` 和 `l25_result` 映射为 JEV state，再调用 JEV 获取事件级结果。JEV 响应的 `answers.needs_diagnosis.noul` 用于本阶段的发布筛选。

`L4_DIAGNOSIS_THRESHOLD` 默认 `0.80`，可通过 `L4-Jev/.env` 或部署环境变量修改。分数大于或等于阈值时，L4 保留上游事件字段，设置 `stage=L4`、更新 `updated_at`、将 JEV 响应写入 `l4_result`，然后发布到 `L4_PRODUCER_TOPIC`。分数低于阈值时不发布，但提交输入 offset。JEV 调用失败、分数缺失或非法、Kafka 发布失败时不提交 offset。

`0.80` 是初始筛选值，需要用实际已标注事件的误报和漏报情况继续校准。L2.5 的逐窗口评分仍全部送入 L4；此处只执行 L4 事件级发布筛选。

L4 会先把输入事件包装成 L4 输出结构：保留上游字段，设置 `stage=L4`、更新 `updated_at` 并写入 `l4_result`。之后才执行阈值筛选。未达到阈值的事件以这份**完整 L4 输出 JSON**写入 PostgreSQL 表 `l4_rejected_events.event_payload`，表中的 `stage` 也为 `L4`；同时保存来源 Topic、事件 ID、实体、JEV 分数、阈值和未通过原因。表以 `(source_topic, source_event_id)` 为主键，重复消费会更新为最新完整的 L4 输出。数据库写入成功后才提交 Kafka offset，数据库故障时消息保持未提交并由服务重试。连接变量为 `POSTGRES_HOST`、`POSTGRES_PORT`、`POSTGRES_DB`、`POSTGRES_USER` 和 `POSTGRES_PASSWORD`。
