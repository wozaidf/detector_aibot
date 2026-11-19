# detector-engine 异常检测系统

这个项目用来从连续采集的数据中筛出值得进一步诊断的异常。仓库自带一批测试数据，你可以在浏览器点一次「回放 500 条样本」，观察数据如何经过规则检测、模型评分和诊断筛选。

## 数据是怎么流动的

Kafka 是各个服务之间传递消息的地方。**Topic（主题）**是一个有名字的消息通道。上一步把结果写入一个主题，下一步从同一个主题接收结果。

| 阶段 | 做什么 | 输出主题 |
| --- | --- | --- |
| L1 样本回放 | 读取样本文件，把已经按时间整理好的数据窗口逐条送入 Kafka | `l1-aligned-windows` |
| L2 规则检测 | Flink 运行 Drools 规则和 CEP 时序规则，合并、去重并生成异常事件 | `l2-anomaly-events` |
| L2.5 异常评分 | IForest 用 23 个特征给异常事件关联的数据窗口逐个打分，结果写入 `l25_result.window_results` | `l25-iforest-events` |
| L4 诊断筛选 | 本地 Laya 模型评估事件是否值得进一步诊断。达到阈值的事件带上 `l4_result` 发给下游 | `l4-anomaly-decisions` |

L4 从 `l4_result.answers.needs_diagnosis.noul` 读取诊断分数。Compose 默认阈值是 `0.8`，分数等于或高于阈值才会发往 L4 输出主题。低于阈值的事件会存入 PostgreSQL，供事后查询。

## 第一次启动（Windows PowerShell）

Docker Desktop 负责运行服务，Docker Compose 根据 [docker-compose.yml](docker-compose.yml) 把服务连起来。下面的命令都在 **detector-engine 项目根目录**运行，也就是能看到 `README.md` 和 `docker-compose.yml` 的目录。

### 1. 准备 Docker 和配置文件

打开 Docker Desktop，等它启动后，在 PowerShell 检查 Docker 和 Compose：

```powershell
docker version
docker compose version
```

从模板创建服务配置。下面的命令会跳过已有的 `.env` 文件：

```powershell
if (-not (Test-Path .\L2-iforest\.env)) {
    Copy-Item .\L2-iforest\.env.example .\L2-iforest\.env
}
if (-not (Test-Path .\L4-Jev\.env)) {
    Copy-Item .\L4-Jev\.env.example .\L4-Jev\.env
}
```

### 2. 准备 L4 模型

完整链路使用仓库中的 `out5-multi` 模型。它的权重由 Git LFS 保存。刚从 Git 克隆项目时，在根目录执行以下命令下载权重：

```powershell
git lfs version
git lfs pull
Get-Item .\L4-Jev\laya_predict\models\out5-multi\model.safetensors |
    Select-Object Name, Length
```

如果 `git lfs version` 失败，先安装 Git LFS。完整权重的大小应为 **643835524 字节**（约 614 MiB）。如果文件只有一百多字节，你手里的是 LFS 下载指针，仍需要用有仓库访问权限的账号执行 `git lfs pull`。模型目录中的 tokenizer、encoder 配置和其他 JSON 文件也要一起保留。

打开 `L4-Jev/.env`，把模型名那一行设置成：

```dotenv
LAYA_MODEL_NAME=out5-multi
```

当前入口用本地 Laya 推理，`JEV_API_KEY` 和 `JEV_NAME` 可以留空。其他可用模型名称见 [Laya 模型路径配置](L4-Jev/laya_predict/laya_api.py)。

### 3. 启动并确认状态

第一次跑完整链路：

```powershell
docker compose --progress plain up -d --build
docker compose ps -a
```

`--build` 会构建镜像。首次构建 L4 会下载 PyTorch 和 NVIDIA 依赖，可能需要较长时间并占用数 GB 磁盘。`--progress plain` 保留普通日志，方便看到正在下载或安装什么。`-d` 让容器启动后在后台运行。

先体验 L1 至 L2.5 时，可以跳过 L4 模型下载，启动以下服务：

```powershell
docker compose --progress plain up -d --build dashboard taskmanager l2-iforest kafka-ui pgadmin
docker compose ps -a
```

| 状态 | 怎么看 |
| --- | --- |
| Kafka、PostgreSQL 显示 `healthy` | 基础服务已通过健康检查 |
| `kafka-init` 显示 `Exited (0)` | 四个业务主题创建成功，初始化任务正常结束 |
| 检测服务显示 `Up` | 容器进程已启动。L2 作业还要在 Flink 页面确认 `RUNNING` |
| `Restarting` 或 `Exited` 且退出码非 0 | 查看对应服务日志，找最早出现的错误 |

## 回放样本，看看结果

1. 打开 [Flink UI](http://localhost:8081)，等 L2 作业显示 `RUNNING`。
2. 打开 [Dashboard](http://localhost:8090)，点击「回放 500 条样本」。样本来自 `L2-stream/L1生成结果/l1_windows.jsonl`。Dashboard 可以查看 L1 窗口和 L2 异常事件。
3. 打开 [Kafka UI](http://localhost:8080)，进入 Topics，按上表顺序打开各个主题的消息页。L4 的消息体里可以看到 `stage: "L4"` 和 `l4_result`。
4. 如果 L4 主题为空，先看 `l25-iforest-events` 是否有新消息，再看 L4 日志以及数据库中的低分事件。L2 会去重，重复回放同一批样本时，新增异常数量可能减少。

```powershell
docker compose logs --tail 100 jobmanager taskmanager l2-iforest
docker compose logs --tail 100 l4-jev
```

在 [pgAdmin](http://localhost:5050) 查询审计记录。网页登录使用 Compose 中 `pgadmin` 的 `PGADMIN_DEFAULT_*` 配置。添加数据库服务器时，主机填 `postgres`，端口填 `5432`，数据库名、用户名和密码使用 Compose 中 `postgres` 的 `POSTGRES_*` 配置。

| 表 | 可以查到什么 |
| --- | --- |
| `l2_non_anomaly_windows` | 已由 Drools 评估、尚未关联最终 L2 异常事件的数据窗口 |
| `l4_rejected_events` | 诊断分数低于阈值的事件，`event_payload` 保存完整 L4 输出 JSON |

## 常用操作

### 更新 L4 权重

Compose 把 Windows 上的 `L4-Jev/laya_predict/models/` 挂载为容器内的 `/app/laya_predict/models/`。**只替换同一路径下的模型文件时，重启 L4 即可重新加载，省去镜像构建。**

```powershell
docker compose restart l4-jev
```

如果修改了 `L4-Jev/.env` 里的模型名，需要重新创建容器来读取新配置。已有镜像时执行：

```powershell
docker compose up -d --no-build --force-recreate l4-jev
```

Compose 中服务的 `environment` 优先于该服务的 `.env`。调整 L4 阈值、Kafka 主题或数据库连接时，检查并修改 [docker-compose.yml](docker-compose.yml) 中 `l4-jev` 的对应配置。

### 修改 L2 规则参数

Dashboard 的「规则参数」会保存到 [rules.env](L2-stream/config/l2/rules.env)。保存后重新创建 Flink 容器：

```powershell
docker compose up -d --force-recreate jobmanager taskmanager
```

### 导出新的 IForest 模型

完成训练和评估后，从项目根目录导出模型，再重建推理服务。部署用模型包位于 `L2-iforest/models/iforest_csv_bundle.joblib`。

```powershell
.\L2-iforest-training\export_model.ps1
docker compose up -d --build l2-iforest
```

### 停止或继续运行

`docker compose stop` 停止当前服务并保留容器。下次可以用 `docker compose start` 启动已有容器。

## 下游 L5 如何接 Kafka

当前 Compose 提供到 L4。接入自己的 L5 时，把它配置成 Kafka **消费者**，订阅已经由 `kafka-init` 创建的 `l4-anomaly-decisions`，并给 L5 一个独立的消费者组，例如 `l5-group-1`。消费者组会记录读到哪条消息。

| L5 在哪里运行 | Kafka 地址 |
| --- | --- |
| 和 Kafka 在同一个 Compose 网络中 | `kafka:29092` |
| 直接运行在这台 Windows 主机上 | `localhost:9092` |
| 运行在另一台机器上 | 需要把 Kafka 的对外公布地址从 `localhost:9092` 改为 L5 能访问的主机名或 IP 和端口 |

L5 收到消息后，按 UTF-8 JSON 解析，读取 `l4_result` 并交给自己的业务逻辑。处理成功后再提交消费进度。首次启动、且消费者组没有已保存进度时，`earliest` 从主题中最早仍保留的消息开始，`latest` 等待之后的新消息。若 L5 加入当前 Compose，可以设置依赖条件，让它等 `kafka-init` 成功后再启动。

## 项目目录

| 目录 | 内容 |
| --- | --- |
| [L2-stream/](L2-stream/) | L1 样本与回放、Flink 规则引擎、Dashboard |
| [L2-iforest/](L2-iforest/) | L2.5 推理、Kafka 消费和发布、部署用模型包 |
| [L2-iforest-training/](L2-iforest-training/README.md) | 数据集生成、IForest 训练、阈值校准和评估 |
| [L4-Jev/](L4-Jev/) | Laya 推理、诊断筛选和 PostgreSQL 审计 |
