# Planned API Routes (Phase 2)

## Health
- `GET /health`

## Auctions
- `GET /auctions?start_date=&end_date=&instrument_type=`
- `GET /auctions/latest`

## Secondary
- `GET /secondary/trades?start_date=&end_date=&instrument_type=`
- `GET /secondary/turnover/daily?start_date=&end_date=`
- `GET /secondary/liquidity/auction-window?window_days=2`

## Curve
- `GET /curve/latest?instrument_type=`
- `GET /curve/snapshot?date=`
- `GET /curve/compare?date_a=&date_b=`

## Instruments
- `GET /instruments?query=&instrument_type=`
- `GET /instruments/{security_key}`
- `GET /instruments/{security_key}/history`

## Alerts
- `GET /alerts?severity=&start_date=&end_date=`
- `POST /alerts/rules/recompute`
