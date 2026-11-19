# IForest CSV 训练与验证

本项目使用 `iforest-dataset\datasets` 下的 `train.csv`、`val.csv`、`test.csv`。CSV 生成器复用当前 L1 模拟器的事件生成、窗口切片和 `feature_calculator.calculate()`，保证同一特征统计口径。数据仍是合成模拟样本，指标用于本地算法/流程验证，不代表生产环境效果。

## 数据约定

当前数据集有 27 列：4 个元数据列和 23 个模型特征列。

日志、Span 和告警绝对总数不再作为模型特征。模型输入使用总量/分钟、失败率/等级比例和持续时间特征；L1 `features` 中原有的总数仍保留，L2 规则和下游数据消费者不受影响。模型列与 L1 特征路径统一定义在 `L2-iforest/inference/feature_schema.py`，CSV 生成和 L1 推理适配器共用该定义。

- `train.csv`：默认 14,000 条，全部 `label=0`，只用于 IForest 拟合及预处理器拟合。
- `val.csv`：默认 3,000 条，使用 L1 场景覆盖的窗口作为异常，用于选择决策阈值；正负比例由场景覆盖自然决定。
- `test.csv`：默认 3,000 条，同样由 L1 场景标签生成，只用于锁定阈值后的最终评估。
- `window_id`、`entity_id`、`window_end` 只用于追溯和时间线；`label` 只用于阈值校准和评估。以上四列均不进入模型特征。
- 其余 23 个数值列是模型特征。空值/NaN/正负无穷使用 train 每列的中位数补齐；若某列在 train 中全缺失，流程报错，不静默填零。
- 数据按连续时间段划分，校验器会检查不同 split 的窗口时间没有交叠；不做随机切分。

`iforest-dataset\generate_dataset.py` 和 `validate_dataset.py` 是数据集生成/校验工具。生成器直接复用 `L2-stream\l1_stream_simulator\generator` 中的代码；特征列映射集中在 `L2-iforest\inference\feature_schema.py`。

## 模型与阈值

模型是 PyOD `IForest` 无监督异常检测器，仅用正常 train 特征拟合。分数越高表示窗口越偏离训练基线。

当前默认训练参数采用验证集网格搜索得到的最佳配置：`n_estimators=300`、`max_samples=1024`、`max_features=1.0`、`contamination=0.01`、`random_state=42`。`contamination` 是 PyOD 内部参数；业务判定仍由每次训练单独在 val 上选择的分数阈值决定。

最终判定阈值在 val 上选择：默认最大化 F1；多个阈值 F1 并列时优先选择 precision 较高者，再优先较高阈值。锁定的阈值保存进模型包，test 评估不会重新调阈值。`--model-contamination` 仅提供 PyOD 模型内部参数；它不是最终判定阈值，也不代表异常概率或业务异常比例。

test 报告包含混淆矩阵、Precision、Recall、F1、ROC-AUC、PR-AUC Average Precision，以及按实体拆分的结果。`score_distribution_report.json` 和阈值选择报告分别汇总验证/测试集正常与异常窗口的分数分位数、阈值命中率和分数区间重叠。分数不是故障概率。合成数据的效果不能外推到真实生产流量，需要用目标环境的真实历史数据重新验证。

## Windows PowerShell

在项目根目录执行：

```powershell
cd .\L2-iforest-training
uv venv --python 3.12 .venv
uv pip install --python .\.venv\Scripts\python.exe -r requirements.txt
```

### 完整流程

```powershell
.\run_all.ps1
```

或者显式运行 CLI：

```powershell
uv run --python .\.venv\Scripts\python.exe .\train_iforest.py all
```

### 分开运行

```powershell
uv run --python .\.venv\Scripts\python.exe .\train_iforest.py inspect
uv run --python .\.venv\Scripts\python.exe .\train_iforest.py train --n-estimators 300 --max-samples 1024 --max-features 1.0 --model-contamination 0.01 --seed 42
uv run --python .\.venv\Scripts\python.exe .\train_iforest.py evaluate --model .\artifacts\模型验证与测试结果\iforest_csv_bundle.joblib
uv run --python .\.venv\Scripts\python.exe -m pytest -q
```

默认 CSV 路径是 `iforest-dataset\datasets`，训练、验证和测试结果输出到 `artifacts\模型验证与测试结果`。可通过 `--datasets-dir` 和 `--output` 指定数据集和训练结果目录。

## 交付模型给预测项目

本项目只负责数据集生成、训练、阈值校准和测试评估。L1 窗口预测程序位于同级 `L2-iforest` 项目，不依赖本训练目录运行。训练完成后在工作区根目录执行下面命令，将模型包复制到预测项目：

```powershell
.\L2-iforest-training\export_model.ps1
```

模型包含训练所得 IForest、预处理器、精确特征顺序和验证集锁定阈值。不要手动修改阈值；如训练特征或数据生成方式变化，应重新训练、校准和测试，再导出模型。

## 产物目录

`artifacts` 按用途分为两个文件夹，训练/评估输出和 L1 推理输出彼此独立。

### `artifacts\模型验证与测试结果`

- `iforest_csv_bundle.joblib`：模型、预处理器、特征顺序和 val 校准阈值。
- `data_inspection_report.json`：训练/验证 split 数量、标签、实体、时间、缺失值与特征清单；训练命令不读取 test.csv。
- `feature_manifest.json`：训练列顺序、中位数填补策略和训练缺失统计。
- `training_config.json`：数据文件哈希、模型参数和 Python/库版本。
- `threshold_selection_report.json`：val 阈值选择规则、阈值及 val 指标。
- `training_summary.json`：本次训练参数、模型文件大小、训练耗时和 val 指标。
- `val_scores.csv`、`test_scores.csv`：每个窗口的 ID、实体、时间、标签、分数、阈值和预测标签。逐窗口评分只保留 CSV，避免重复导出 JSONL。
- `test_evaluation_report.json`：锁定阈值后的总体和分实体 test 指标。
- `l25_test_candidate_events.jsonl`：test 上超过阈值的候选事件格式样例，不含真值标签。
- `validation_score_distribution.png`、`test_score_distribution.png`、`test_score_timeline.png`、`test_confusion_matrix.png`：阈值与评估图表。


