# Recovery valuation methodology
Use **unrestricted cash at cutoff**, already net of historic paid distributions and paid liabilities. Do not add old buyout receipts again.

For scenario s:
- future UGX cash = opening unrestricted UGX cash + incremental USD recovery × settlement FX − future USD costs × FX − future UGX liabilities − reserves.
- future distributable cash = max(0, future cash).
- PV/share = distributable cash / outstanding shares / (1 + discount rate)^years.
- expected PV/share = sum(probability × scenario PV/share).

This simple model assumes a single distribution date per scenario. Use a dated multi-cash-flow model where interim payments materially affect value. It shows company-level gross distributions before investor-specific taxes. Insolvency is flagged rather than implying creditors disappear when equity is floored at zero.

For a binary full-or-zero recovery with otherwise fixed inputs, an implied probability can be solved only after subtracting the verified base value and discounting net incremental recovery. In a multi-scenario setting a single market price cannot uniquely identify legal-success probability.

Tests verify cash arithmetic, probability sum, nonnegative equity and rejection of invalid inputs. Source-backed accounting tie-outs remain a prerequisite to a current target.
