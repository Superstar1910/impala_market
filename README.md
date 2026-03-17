# Bond Market Intelligence MVP (Streamlit)

Self-contained clickable MVP for African bond market intelligence (Uganda-first), using local CSV data.

## Project Structure
- `app.py`: main Streamlit app and page navigation
- `data_loader.py`: data loading and transformation helpers
- `docs/API_ROUTES.md`: planned API contract for phase-2 backend split
- `requirements.txt`: Python dependencies

## Data Source
Default source path is:
`C:\Users\user\Documents\Impala Market\Test Data\bou_unified_master_analysis_dataset_v2.csv`

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

## Notes
- This MVP is UI+analytics in one process (no separate backend service).
- `docs/API_ROUTES.md` shows how to split into API architecture later.
- Exports are available in each module via CSV download.
