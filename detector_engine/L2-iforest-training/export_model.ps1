$ErrorActionPreference = 'Stop'
$trainingRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$workspaceRoot = Split-Path -Parent $trainingRoot
$model = Join-Path $trainingRoot 'artifacts\模型验证与测试结果\iforest_csv_bundle.joblib'
$destinationDir = Join-Path $workspaceRoot 'L2-iforest\models'
$destination = Join-Path $destinationDir 'iforest_csv_bundle.joblib'

if (-not (Test-Path -LiteralPath $model -PathType Leaf)) {
    throw "找不到训练模型包：$model。请先完成训练。"
}
New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null
Copy-Item -LiteralPath $model -Destination $destination -Force
Write-Output "预测模型已更新：$destination"
