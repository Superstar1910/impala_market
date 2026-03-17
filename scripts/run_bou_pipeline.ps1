param(
  [string]$StartDate = "2025-01-02",
  [string]$EndDate = "",
  [string]$Root = "data"
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($EndDate)) {
  $EndDate = (Get-Date).ToString("yyyy-MM-dd")
}

python .\scripts\refresh_bou_market_data.py --start-date $StartDate --end-date $EndDate --root $Root
