# Production Hardening Checklist

## Implemented
- Public-by-default mode (`auth_required=false`).
- Optional passcode auth via secrets/env (`APP_AUTH_REQUIRED`, `APP_PASSCODE`).
- Remote source-of-truth CSV with retry and local fallback support.
- Cache TTL control (`CACHE_TTL_SECONDS`).
- Data freshness check (`STALE_DATA_HOURS`).
- Ops page with health status, missing-column checks, stale warning.
- File logging to `logs/app.log`.
- Optional webhook push from Ops page (`WEBHOOK_URL`).
- Scheduled dataset refresh (`.github/workflows/refresh-bou-data.yml`): runs
  `scripts/refresh_bou_market_data.py` daily and commits the refreshed
  `data/serving/*.csv` extracts back to the repo, so `LOCAL_FALLBACK_CSV`
  and any external consumer reading `data/serving/` always has a current
  file without anyone running the pipeline by hand.

## Before Push/Deploy
1. Confirm Streamlit Cloud secrets are set as needed.
2. Keep `auth_required=false` for pilot if public access is intended.
3. Verify fallback file path exists if using `LOCAL_FALLBACK_CSV`.
4. Validate latest `report_date` is within freshness threshold.
5. Test webhook endpoint from Ops page (optional).
6. Confirm Actions are enabled for this repo and the default `GITHUB_TOKEN`
   has `contents: write` (Settings -> Actions -> General -> Workflow
   permissions) so `refresh-bou-data.yml` can push its commits.

## Next Steps
1. Add uptime monitoring ping on Streamlit URL.
2. ~~Schedule dataset refresh to local fallback path.~~ Done -
   `.github/workflows/refresh-bou-data.yml`.
3. Add row-level QA metrics trend chart in Ops page.
4. Consider a `repository_dispatch` step in `refresh-bou-data.yml` (needs a
   PAT with cross-repo `repo` scope stored as a secret - the default
   `GITHUB_TOKEN` can't do this) to notify downstream consumers like
   `oakstone-allocation-os` the moment new data is published, instead of
   them polling or waiting for their own schedule.
