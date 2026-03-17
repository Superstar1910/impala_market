param(
  [string]$SourceCsv = "C:\Users\user\Documents\Impala Market\Test Data\bou_unified_master_analysis_dataset_v2.csv",
  [string]$TargetCsv = "C:\Users\user\Documents\impala_market\data\latest_unified.csv"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $SourceCsv)) {
  throw "Source file not found: $SourceCsv"
}

New-Item -ItemType Directory -Force -Path (Split-Path $TargetCsv -Parent) | Out-Null
Copy-Item -Path $SourceCsv -Destination $TargetCsv -Force

$rows = (Import-Csv $TargetCsv | Measure-Object).Count
Write-Host "Refresh complete. Rows: $rows"
