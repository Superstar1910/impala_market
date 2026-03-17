# Bond Market Intelligence MVP (Streamlit)

Self-contained clickable MVP for African bond market intelligence (Uganda-first), using local CSV data.

## Project Structure
- `app.py`: main Streamlit app and page navigation
- `data_loader.py`: data loading and transformation helpers
- `ops.py`: health checks, logging, webhook notifications
- `docs/API_ROUTES.md`: planned API contract for phase-2 backend split
- `docs/PRODUCTION_CHECKLIST.md`: hardening checklist and next steps
- `docs/DATA_ARCHITECTURE.md`: lake/warehouse/serving design
- `scripts/refresh_data.ps1`: refresh local fallback dataset
- `scripts/health_check.ps1`: freshness preflight check
- `scripts/refresh_bou_market_data.py`: single reusable BoU scraper/parser pipeline
- `requirements.txt`: Python dependencies

## Data Source
Default source path is remote URL (source of truth):
`https://drive.google.com/uc?export=download&id=17DGvu69IpPPSdh1GSSWAiNLurJqu87Gx`

You can override it from the sidebar inside the app.

## Run
1. Open terminal in this folder.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Start app:
   - `streamlit run app.py`

## MVP Modules
- Dashboard (overview cards, turnover, latest curve)
- Auctions
- Secondary Market
- Yield Curve
- Instruments
- Alerts
- Ops (health, freshness, runtime config, webhook test)

## Notes
- This MVP is UI+analytics in one process (no separate backend service).
- `docs/API_ROUTES.md` shows how to split into API architecture later.
- Exports are available in each module via CSV download.
- Remote CSV has retry with local fallback support (`LOCAL_FALLBACK_CSV`, default `data/latest_unified.csv`).

## Production Controls
Configure via Streamlit secrets or environment variables:
- `APP_AUTH_REQUIRED` (`true/false`, default `false` for public pilot)
- `APP_PASSCODE` (basic passcode auth when enabled)
- `CACHE_TTL_SECONDS` (default `900`)
- `STALE_DATA_HOURS` (default `48`)
- `WEBHOOK_URL` (optional)

## Ops scripts
- Refresh fallback data:
  - `powershell -ExecutionPolicy Bypass -File .\scripts\refresh_data.ps1`
- Run health check:
  - `powershell -ExecutionPolicy Bypass -File .\scripts\health_check.ps1`

## BoU single-pipeline run
Run one command to scrape, parse, normalize, and publish serving extracts:

```powershell
python .\scripts\refresh_bou_market_data.py --start-date 2025-01-02 --end-date 2026-03-18 --root data
```

PowerShell wrapper:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_bou_pipeline.ps1 -StartDate 2025-01-02
```

Outputs:
- `data/lake/raw` (downloaded source files)
- `data/lake/normalized` (partitioned parquet)
- `data/lake/curated` (canonical merged dataset)
- `data/warehouse/impala_market.duckdb`
- `data/serving/*.csv`
- `data/logs/parse_log_*.csv` and summary json
