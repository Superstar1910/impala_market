# Africa FX Pressure Watch

An open-source launch pad for monitoring foreign-exchange pressure across African currencies with a consistent, evidence-led framework.

The project separates:

- observable currency outcomes;
- near-term USD supply and demand pressures;
- structural balance-of-payments conditions;
- reserve and policy buffers; and
- verified facts from hypotheses that still require evidence.

## Current coverage

| Currency | Country | Status | Latest research cut-off |
|---|---|---|---|
| UGX | Uganda | Reference implementation | 21 September 2026 |
| KES, TZS, RWF, GHS, NGN, ZAR, EGP, ETB, ZMW, XOF, XAF | Selected African markets | Coverage planned | Not yet published |

No planned-market tile is a market call. A currency becomes `active` only after its source register and country file pass review.

## Use the dashboard

Open [`index.html`](index.html) or enable GitHub Pages from the repository's `main` branch. Uganda's detailed dashboard is at [`countries/uganda/index.html`](countries/uganda/index.html).

## Repository map

```text
countries/               Country dashboards and notes
data/countries/          Machine-readable country snapshots
docs/                    Methodology, governance and contribution rules
sources/                 Evidence registers
.github/                 Validation workflow and issue templates
index.html               Pan-African launch pad
```

## Method in one line

**FX outcome = near-term flow pressure + structural external balance + buffer/policy capacity + market positioning**, assessed with explicit dates and evidence confidence.

The pressure score is a triage device, not a forecast or trading signal. See [methodology](docs/METHODOLOGY.md).

## Contributing a currency

Start with the country coverage issue template. Every factual datapoint needs a date, unit, source URL, publisher and confidence classification. See [contributing](CONTRIBUTING.md).

## Important notice

This is research infrastructure, not investment advice. Data can be delayed, revised or incomparable across countries. Verify primary sources before acting.

