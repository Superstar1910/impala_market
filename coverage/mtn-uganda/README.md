# MTN Uganda stock coverage — HTML output package

Kenneth S. Legesi, CFA | Legesi Investment Research

Prepared 13 October 2026. Public HTML research output package. Full project-file consolidation and a refreshed numerical valuation remain outstanding.

## Read the analysis

- [Coverage synthesis](docs/index.html)
- [Valuation methodology](docs/valuation.html)
- [Shareholder rights analysis](docs/ownership.html)
- [Substack article index](docs/substack.html)

**[Read the rendered HTML analysis](https://legesi.xyz/mtn-uganda-stock-coverage/)**. GitHub Pages is enabled and the deployment is verified. The included Pages workflow publishes main-branch changes.

## Published foundations

- [The Untold Story behind Africa’s Most Valuable Fintech Spinout](https://legesi.substack.com/p/the-untold-story-behind-africas-most)
- [The Illusion of Ownership: What MTN Uganda’s MoMo Restructure Means for Investors](https://legesi.substack.com/p/the-illusion-of-ownership-what-mtn)
- [The Gem Inside the Rump](https://legesi.substack.com/p/the-gem-inside-the-rump)
- [Publication RSS](https://legesi.substack.com/feed)

Substack retains the canonical articles. HTML outputs synthesize the analysis and link to those articles. They do not assert a refreshed price, target or recommendation.

## Rebuild and refresh

```bash
python3 scripts/build_reports.py
python3 scripts/refresh_substack.py
python3 scripts/build_reports.py
```

Python standard library only. RSS matching searches titles and descriptions for MTN/MoMo and validates canonical Substack links. Older articles remain in the curated register because feeds may only include recent posts. Browser-side cross-origin fetching is avoided.

The Monday refresh produces an artifact for review. It does not commit changes or alter an investment conclusion. Download and review the artifact before incorporating metadata into main. The live RSS fetch could not be validated in the preparation environment; an HTML access-error page was returned. The parser rejects that response and preserves the cache. Offline parser and link checks were completed.

## Complete coverage release

Add the full project source inventory, normalized financials, source-to-model references, audited valuation calculations, dated prices/dividends, and verified current separation/entitlement documents. Confirm the date of the Gem article. Replace historical graphics only after their inputs tie to the models. Preserve reported facts, historical claims and forecasts as separate fields.

## Publication terms

General information, not personalized investment advice or an issuer-endorsed report. Author research and publication rights remain with Kenneth Legesi; third-party source rights remain with their owners. No third-party filings are redistributed in this package.

## Canonical public URLs

Reader-facing coverage is published at https://legesi.xyz/work/mtn-uganda-stock-coverage/
by the main website repository. This repository remains the research source.
`python scripts/build_reports.py` exports the four full HTML reports to `canonical/`
for review/import and writes compatibility redirects to `docs/`. The export is
ignored by git and never uploaded as the Pages root.

After reviewing changes, import the export into the main-site repository using
`python scripts/import_mtn_coverage.py /path/to/this-checkout`, build and deploy
the main site first, then deploy this repository's redirect update. Existing
`.html` links continue to the corresponding directory-style canonical page.
Shared redirect JavaScript preserves query strings and anchors. GitHub Pages
serves page-based redirects, not custom HTTP 301/308 rules.
