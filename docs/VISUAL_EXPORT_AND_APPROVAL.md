# Impala Market — Reproducible Visual Export and Publication Protocol

Status: **operational specification, not an image approval**. Owner: Impala Market research/editorial. Downstream destination: `Superstar1910/Superstar1910.github.io` Legesi Visual Library.

## Verified starting point (2026-11-01)
- `app.py` renders Plotly charts for the Dashboard, Auctions, Secondary and Yield Curve pages. There are data-visualisation controls that change output, including date range, instrument type, clipping of turnover outliers, currency scaling, and logarithmic axes.
- `data_loader.py` uses an external CSV with a local fallback; source selection and freshness can change results. Financial illustrations must capture **actual source used**, whether fallback was used and the dataset SHA-256, not simply today's screenshot.
- The dedicated ChatGPT Library folder `/Impala Market` held no recoverable files in the accessible listing; no source-approved image exports were found.
- Historic Project discussions specify figures but are not original PNG/SVG evidence.

## Chart families to produce **only after editorial approval**
| Chart family | Existing implementation reference | Essential data verification |
|---|---|---|
| Daily secondary-market turnover | `app.py`: `daily_turnover`, `prepare_turnover_for_plot` | UGX vs UGX bn; amount-cost versus face-value; positive-value filters; whether quantile clipping is enabled; seven-observation moving average |
| Yield curve snapshot | `app.py`: `latest_curve`, `aggregate_curve` | Auction vs traded yields; yield metric; security tenor vs residual maturity; tenor bin size; weighted mean vs median; snapshot date |
| Auction results | `app.py`: `get_auctions`, `compute_auction_surprise` | BoU issue/auction date; 91/182/364-day tenor; offered/tendered/accepted; bid-to-cover; accepted yield; ISIN |
| Auction window liquidity | `app.py`: `auction_window_liquidity` | Date window including/excluding auction date; missing observations; trading-day basis; comparison benchmark |
| IPS allocation tools | Historical Project specification only | No personalised accounts; scenario inputs, asset universe, return/volatility sources and appropriateness disclaimers |

## Non-negotiable provenance manifest
For each exported visual save a sidecar JSON with:
`visual_id`, `project`, `title`, `caption`, `alt`, `source_conversation_or_commit`, `code_commit_sha`, `source_uri_or_file`, `dataset_sha256`, `data_observation_start`, `data_observation_end`, `report_date_as_of`, `retrieved_at_utc`, `used_fallback`, `filters`, `unit`, `chart_transformations`, `source_licence`, `rights_status`, `confidentiality_status`, `reviewer`, `approved_at_utc`, `original_file_sha256`, `publication_status`.

If any source/approval field is unknown, mark `pending`; never populate it with a fabricated value. Store raw data privately where required and retain a verifiable source hash. DO NOT publish personal holdings or nonpublic order flow.

## Export procedure
1. Freeze a known source file (including source path, dataset SHA-256, BoU press release URL/reference and report date), and record code commit SHA. Confirm the app has not silently switched to the fallback CSV.
2. Recalculate selected observations against official BoU source rows; record reconciliations and known gaps. Separate auction yields from secondary traded yields. For turnovers, record whether raw, trimmed or winsorised data are displayed. Never label quantile-clipped values as unadjusted turnover.
3. Reproduce chart with documented filters. Export a source-friendly vector original if available and a lossless PNG; preserve output with a sidecar manifest. Any version change requires a new hash.
4. Compare candidate against historical revisions and obtain an explicit editorial final-version approval and rights clearance. A later filename does not establish approval.
5. Prepare web derivatives without changing values or annotations. Strip sensitive EXIF. Name image files deterministically under `static/img/visuals/impala-market/` in the Legesi repo.
6. Archive using a feature branch/PR with image and thumbnail, catalogue record in `static/data/visual-library.json`, original/source-hash evidence and editor approval. Run `python scripts/validate_visual_library.py`, Site Quality and responsive visual tests.
7. After authorised merge, verify live rendering at `https://legesi.xyz/visuals/`. Never report "published" based only on a commit, CI run or generated image.

## Exclusions and status
Cross-project Oakstone FX Pressure Watch images are not Impala originals and remain blocked pending separate approval and data QA. Charts/screenshots generated from sample or stale CSV may serve as labelled internal previews only and cannot be published as market research.

Related Visual Library audit: `Superstar1910/Superstar1910.github.io/docs/IMPALA_MARKET_VISUAL_AUDIT_2026-11-01.md`.
