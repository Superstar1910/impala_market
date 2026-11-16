# Umeme stock coverage
**Kenneth S. Legesi | Public research repository | Initial release: 13 October 2026**

A research hub for Umeme's concession exit, buyout recovery, contested liabilities and shareholder distributions. The central question is **what remaining cash can reach shareholders, when, and with what uncertainty?**

## Published articles
- [Part 1 — Optionality and the Investor’s Mindset: Lessons from Umeme](https://legesi.substack.com/p/optionality-and-the-investors-mindset) — 19 June 2025.
- [Part 2 — What's Umeme's Probability of Winning the Arbitration?](https://legesi.substack.com/p/optionality-and-the-investors-mindset-7f5) — 1 July 2025.

Substack remains the canonical publication. [Article register](research/articles.md) records verified publications and recovered chat research. This repository adds reproducible methods, historical inputs and a corrections ledger.

## Start here
- [Coverage note](research/coverage.md)
- [Valuation methodology](docs/methodology.md)
- [Historical reconciliation](research/corrections.md)
- [Source register](data/source-register.csv)
- [Recovery model](models/recovery.py)
- [Publication governance](governance/publication.md)
- [HTML research hub](docs/index.html) — downloadable HTML; Pages deployment is separate.

## Evidence posture
This initial release preserves dated research. It does **not** supply a verified October 2026 price, target, recommendation or arbitration-status update. The illustrative model deliberately separates assumptions from reported facts. Original project attachments have not all been recovered; see [migration register](data/project-file-manifest.csv).

## Run
Requires Python 3.10+; standard library only.
```sh
python models/recovery.py data/assumptions/illustrative.json
python -m unittest discover -s tests -v
```
The example is hypothetical and is not calibrated to current Umeme accounts. Results must not be used as a current price target.

Research is informational. Positions/conflicts: not supplied; readers should not infer absence. Authored research is copyright Kenneth S. Legesi; code licence is in LICENSE. Third-party materials retain their rights.
