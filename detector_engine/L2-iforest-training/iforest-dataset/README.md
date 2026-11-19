# IForest 数据集与 L1 特征口径

训练 CSV 由 `generate_dataset.py` 生成。它直接调用当前工作区 `L2-stream/l1_stream_simulator/generator` 中的事件生成器、窗口构造器和 `feature_calculator.calculate()`，因此模型训练和 L1 在线窗口使用相同的特征计算代码。

## 统一的数据口径

- 实体默认沿用 L1 的 `server-001`、`server-002`、`server-003`。
- 默认窗口长度 300 秒、滑动 60 秒、指标采样间隔 30 秒。
- 窗口事件范围是左闭右开 `[window_start, window_end)`；按当前 L1 事件生成频率，正常窗通常有 7 条日志（每分钟请求日志，另有每 150 秒健康检查）和 10 个 Span（每分钟生成一对父子 Span）。异常场景可能改变日志等级、Span 状态和统计值。
- CPU、内存、延迟、丢包、流量、日志、Span、告警特征，全部从 L1 生成的窗口原始事件用同一个 `feature_calculator.calculate()` 计算，不再为 CSV 单独生成特征。
- 模型不再使用日志/Span/告警绝对总数：使用每分钟总日志数、每分钟 Span 数、每分钟失败 Span 数、每分钟告警数；并保留 ERROR/WARN/INFO 日志比例、Span 失败率、告警等级比例、日志与失败 Span 的 trace 匹配比例。每分钟数量 = 窗口内数量 ÷ 窗口分钟数。L1 原特征中的绝对计数仍保留，供已有 L1/L2 输出和规则使用。
- CSV 的 `label` 按 L1 场景与窗口时间区间是否重叠生成；train 禁用异常场景，val/test 使用 L1 场景生成器注入场景。val/test 异常占比由场景覆盖率自然决定，不再强制凑成固定比例。
- train/val/test 使用独立的连续时间段，并留出至少一个完整窗口长度，防止同一实体的滑动窗口跨 split 重叠。

CSV 共 27 列：`window_id`、`entity_id`、`window_end`、`label`，以及 23 个模型特征。模型列名与取值路径由 `L2-iforest/inference/feature_schema.py` 单点定义，训练 CSV 生成和 L1 推理适配器共同引用该映射。`window_start` 可由 `window_end - 300 秒` 得到；元数据和标签不进入模型。

23 个特征按组为：

- Metrics：CPU、内存、延迟均值/最大值、丢包率、网络流量和带宽利用率，共 9 项。
- Logs：日志总量/分钟、ERROR/WARN/INFO 比例、ERROR 日志与失败 Span 的 trace 匹配比例，共 5 项。
- Traces：Span/分钟、失败 Span/分钟、Span 失败率、耗时均值/最大值，共 5 项。
- Alarms：告警/分钟、critical/major/minor 等级比例，共 4 项。

## 生成与校验

在项目目录创建环境并安装依赖后，在工作区根目录运行：

```powershell
uv venv --python 3.12 .\L2-iforest-training\.venv
uv pip install --python .\L2-iforest-training\.venv\Scripts\python.exe -r .\L2-iforest-training\requirements.txt
& .\L2-iforest-training\.venv\Scripts\python.exe .\L2-iforest-training\iforest-dataset\generate_dataset.py
& .\L2-iforest-training\.venv\Scripts\python.exe .\L2-iforest-training\iforest-dataset\validate_dataset.py
```

默认生成 14,000 个正常训练窗口、3,000 个验证窗口、3,000 个测试窗口。验证/测试异常标签完全来自 L1 场景与窗口的真实时间重叠；当前生成结果的异常数量应以 CSV 标签统计为准，不能把旧的“固定 10%”假设用于新数据。

可调整数量、起始时刻、窗口和采样配置：

```powershell
python .\L2-iforest-training\iforest-dataset\generate_dataset.py `
  --train-count 14000 --val-count 3000 --test-count 3000 `
  --start-time 2026-01-01T00:00:00Z --window-size-sec 300 --slide-sec 60 `
  --metric-interval-sec 30 --scenario-frequency 1.0 --seed 42
```

`--val-anomaly-ratio` 和 `--test-anomaly-ratio` 仅为旧命令行兼容保留，不控制标签比例。实体和场景定义沿用 L1 模拟器，防止训练侧单独漂移。`generation_manifest.json` 记录列名、单位、各 split 标签计数和 CSV 哈希。

## IForest 训练和阈值

`train_iforest.py train` 只使用正常 train 拟合模型，在 val 上按已实现的策略选择分数阈值；`evaluate` 锁定该阈值后只评估 test。请在生成 CSV 后重新训练模型，旧模型与旧阈值基于不同统计分布，不应继续用于 L1。

```powershell
& .\L2-iforest-training\.venv\Scripts\python.exe .\L2-iforest-training\train_iforest.py all
& .\L2-iforest-training\.venv\Scripts\python.exe -m pytest .\L2-iforest-training\tests -q
```

IForest 分数是模型异常分数，不是故障概率。验证/测试指标依旧是由同一个 L1 模拟器产生的合成场景评估，不代表真实生产效果；生产使用前仍需目标环境的历史数据验证。
