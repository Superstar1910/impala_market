# Contributing

## Add or refresh coverage

1. Open a **Country coverage** issue and identify the currency, cut-off date and primary sources.
2. Copy `data/countries/_template.json` and complete every required field.
3. Add sources to `sources/source-register.csv`.
4. Label unverified drivers as hypotheses; do not convert market commentary into fact.
5. Run `python scripts/validate_data.py`.
6. Submit a pull request describing what changed, why, and any unresolved evidence gaps.

## Evidence hierarchy

Prefer central banks, statistics agencies, finance ministries, multilateral datasets, exchanges and official energy/commodity sources. Use credible market reporting for intraday colour only, and identify it as such.

## Dating rule

Every numeric observation must include its observation date or period and retrieval date. Never describe a stale observation as current.

