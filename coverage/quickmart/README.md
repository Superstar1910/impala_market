# Quickmart IPO investment analysis

Independent investor research by **Kenneth S. Legesi, CFA | Legesi Investment Research**. Research snapshot: **12 October 2026**. This is a public research package, not an issuer-endorsed publication or personalised investment advice.

## Start here

- [Current investment assessment and free article](research/quickmart-ipo-investment-assessment.md)
- [Revised enterprise DCF workbook](valuation/revised-enterprise-dcf.xlsx)
- [Revised valuation and audit note](valuation/valuation-audit-note.pdf)
- [Methodology and limitations](research/valuation-methodology.md)
- [Sources and references](sources/official-document-links.md)
- [Publication inventory](sources/publication-inventory.json)

## Valuation status — provisional, not validated

**WATCH / DEFER at KES 7.50** remains the research assessment. It is not a forecast of the listing-day share price.

| Reference | KES/share | What it means |
|---|---:|---|
| Illustrative 13x FY2026 P/E | 6.83 | Judgmental earnings screen |
| Offer price | 7.50 | Price in the IPO documentation |
| Revised enterprise DCF | 8.65 | Provisional 16% WACC / 4% growth case |
| Rental-based cross-check | 6.75 | Unverified rental proxies; unresolved reconciliation |

The DCF is anchored to **end-2026**, not precisely 12 October 2026. Its WACC, lease roll-forward, working capital and terminal reinvestment are not fully validated. Do not average these estimates into a fair-value target. See [limitations](valuation/assumptions-and-limitations.md).

## Reproduce the calculations

```bash
python3 scripts/reproduce_valuation.py
```

Python 3 standard library only. The script uses a transparent numeric input extract from the revised workbook and writes the DCF bridge, cash flows, sensitivity CSVs and exact SVG dashboard. It reproduces the workbook arithmetic; it does not independently validate the economic assumptions or generate a new investment recommendation.

## Graphics

1. [IPO at a glance](graphics/ipo-at-a-glance.png)
2. [Five-finger investor test](graphics/five-finger-investor-test.png)
3. [International comparables](graphics/international-comparables.png)
4. [P/E sensitivity](graphics/pe-sensitivity.png)
5. [Exact DCF dashboard](graphics/dcf-valuation-dashboard.svg)

The handwritten comparables graphic uses rounded KES 2.10bn earnings in its side-panel examples; the article uses KES 2.103bn. This explains KES 9.29 versus KES 9.31, for example. The original handwritten DCF dashboard had sensitivity errors and is excluded; the SVG replacement contains calculated values.

## Historical research

Earlier reports and models are retained under [archive](archive/README.md). They are **superseded historical material**, not the current valuation or conclusion. Read the current assessment first.

## Copyright and publication

Third-party prospectuses, audited accounts, EY and Stanbic reports are **linked, not redistributed**. Source ownership and any data-provider restrictions remain with their owners. Analytical tables and illustrations are attributed; no licence is granted over third-party material. See [copyright policy](COPYRIGHT.md) and [excluded materials](sources/excluded-material.md).

No public Substack article URL was verified during packaging, so no draft-editor link is presented as a published article. The [Legesi publication](https://legesi.substack.com/) is the publication home.

Please use GitHub issues for calculation corrections and suggestions for the next stock assessment. Include the company, ticker, exchange and question you want answered.
