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

## Before Push/Deploy
1. Confirm Streamlit Cloud secrets are set as needed.
2. Keep `auth_required=false` for pilot if public access is intended.
3. Verify fallback file path exists if using `LOCAL_FALLBACK_CSV`.
4. Validate latest `report_date` is within freshness threshold.
5. Test webhook endpoint from Ops page (optional).

## Next Steps
1. Add uptime monitoring ping on Streamlit URL.
2. Schedule dataset refresh to local fallback path.
3. Add row-level QA metrics trend chart in Ops page.
