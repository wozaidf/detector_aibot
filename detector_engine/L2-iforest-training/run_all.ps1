param(
    [string]$DatasetsPath = '.\iforest-dataset\datasets',
    [string]$OutputPath = '.\artifacts\模型验证与测试结果',
    [int]$Trees = 300,
    [int]$MaxSamples = 1024,
    [double]$MaxFeatures = 1.0,
    [int]$Seed = 42,
    [double]$ModelContamination = 0.01
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw '未找到虚拟环境。请先执行: uv venv --python 3.12 .venv' }
Push-Location $projectRoot
try {
    & $python .\train_iforest.py all --datasets-dir $DatasetsPath --output $OutputPath --n-estimators $Trees --max-samples $MaxSamples --max-features $MaxFeatures --seed $Seed --model-contamination $ModelContamination
    if ($LASTEXITCODE -ne 0) { throw "IForest CSV 流程失败，退出码 $LASTEXITCODE" }
}
finally { Pop-Location }
