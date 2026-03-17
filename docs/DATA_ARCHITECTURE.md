# Data Architecture (Lake -> Warehouse -> Serving)

## Paths
- Lake root: `data/lake`
- Warehouse: `data/warehouse/impala_market.duckdb`
- Serving extracts: `data/serving`

## Lake layout
- `data/lake/raw/`
  - downloaded source files (PDF/XLS/XLSX/CSV) partitioned by market and year
- `data/lake/normalized/`
  - parquet partitions:
    - `market_type=<...>/report_year=<...>/data.parquet`
- `data/lake/curated/`
  - canonical merged dataset:
    - `bou_market_curated.parquet`
    - `bou_market_curated.csv`

## Warehouse layer
- `impala_market.duckdb` tables:
  - `bou_market_raw`
  - `bou_market_curated`

## Serving layer
- `daily_turnover.csv`
- `latest_curve_points.csv`
- `bou_unified_master_analysis_dataset_v2.csv` (app fallback copy)

## Single pipeline script
- `scripts/refresh_bou_market_data.py`
  - scrapes BoU pages
  - downloads sources
  - parses secondary + auction reports
  - writes lake partitions
  - builds DuckDB warehouse
  - exports serving files
  - writes run logs in `data/logs/`
