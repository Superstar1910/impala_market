# Valuation methodology

All enterprise cash-flow amounts are in KES billion; shares are in billions. Outputs are nominal KES. The model discounts FY2027–2032 year-end cash flows to end-2026.

## Earnings screen

Forecast FY2026 PAT of KES 2.103bn / 4bn shares = KES 0.52575 EPS. Applying 13x gives KES 6.83475 per share. The multiple is judgmental; its 26.55% discount to 17.7x is not a statistically validated risk adjustment.

## Enterprise DCF

FCFF = EBIT × (1 − tax) + PPE/intangible depreciation + ROU depreciation − conventional capex − NWC cash outflow.

PV explicit cash flows = sum(FCFF_t / (1 + WACC)^t).

PV terminal value = [FCFF_2032 × (1 + g) / (WACC − g)] / (1 + WACC)^6.

EV = PV explicit + PV terminal. Equity = EV − net financial obligations including leases. Per-share value = equity / shares outstanding.

At 16% WACC and 4% growth: PV explicit 19.023944; PV terminal 22.857973; EV 41.881917; net obligations 7.272728; equity 34.609189; share value 8.652297.

## Rental-proxy diagnostic

Rental FCFF = post-IFRS16 FCFF − tax × ROU depreciation − (1 − tax) × rental proxy. This replaces the tax-effected ROU depreciation with tax-effected rent in operating economics. The diagnostic excludes lease liabilities from net debt, giving net debt excluding leases of −0.627272bn.

Diagnostic EV 26.375041; equity 27.002313; share value 6.750578. This is an unverified proxy method, not a completed lease-adjusted valuation. See the limitations file for unmatched lease additions, cost-of-capital and reinvestment assumptions.

## Reproducibility boundary

The workbook remains the working source model. `forecast-inputs.json` is an explicit numeric extract for independent arithmetic checks. The reproduction script uses those disclosed inputs and recalculates all outputs; it does not import cached spreadsheet results.

Forecast revenue and EBIT after FY2027 are hardcoded analyst projections in the supplied workbook, not a fully driver-based store forecast. They are published transparently, without describing them as independently verified company guidance.
