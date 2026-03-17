param(
  [string]$CsvPath = "C:\Users\user\Documents\impala_market\data\latest_unified.csv",
  [int]$MaxAgeHours = 48
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $CsvPath)) {
  Write-Error "CSV missing: $CsvPath"
  exit 2
}

$rows = Import-Csv $CsvPath
if ($rows.Count -eq 0) {
  Write-Error "CSV has zero rows"
  exit 3
}

$latest = $rows | ForEach-Object { $_.report_date } | Where-Object { $_ -match '^\d{4}-\d{2}-\d{2}$' } | Sort-Object | Select-Object -Last 1
if (-not $latest) {
  Write-Error "No valid report_date found"
  exit 4
}

$latestDt = [datetime]::ParseExact($latest, "yyyy-MM-dd", $null)
$ageHours = ((Get-Date).ToUniversalTime() - $latestDt.ToUniversalTime()).TotalHours

if ($ageHours -gt $MaxAgeHours) {
  Write-Error "Data stale: latest=$latest age_hours=$([math]::Round($ageHours,2))"
  exit 5
}

Write-Host "Health OK: rows=$($rows.Count) latest=$latest age_hours=$([math]::Round($ageHours,2))"
exit 0
