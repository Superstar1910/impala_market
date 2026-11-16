"""Single-date recovery model. Input opening cash must be net of past payments."""
import json
import math
import sys

def number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite numeric")
    return value

def calculate(data):
    shares = number(data["shares"], "shares")
    cash = number(data["opening_cash_ugx"], "opening_cash_ugx")
    rate = number(data["discount_rate"], "discount_rate")
    if shares <= 0 or cash < 0 or rate < 0:
        raise ValueError("shares positive; opening cash and discount rate nonnegative")
    scenarios = data["scenarios"]
    if not scenarios:
        raise ValueError("scenarios required")
    rows = []
    for s in scenarios:
        values = {key: number(s[key], key) for key in
                  ("probability", "recovery_usd", "fx", "costs_usd", "liabilities_ugx", "reserve_ugx", "years")}
        if any(v < 0 for v in values.values()) or values["fx"] <= 0 or values["probability"] > 1:
            raise ValueError("invalid scenario values")
        net = cash + (values["recovery_usd"] - values["costs_usd"]) * values["fx"] - values["liabilities_ugx"] - values["reserve_ugx"]
        pv = max(0, net) / shares / (1 + rate) ** values["years"]
        rows.append({"name": s["name"], "probability": values["probability"],
                     "cash_before_equity_floor_ugx": net, "shortfall_ugx": max(0, -net),
                     "future_gross_distribution_per_share_ugx": max(0, net) / shares,
                     "pv_per_share_ugx": pv})
    if not math.isclose(sum(r["probability"] for r in rows), 1, abs_tol=1e-9):
        raise ValueError("probabilities must sum to one")
    return {"label": data.get("label", "Unlabelled assumptions"), "scenarios": rows,
            "expected_pv_per_share_ugx": sum(r["probability"] * r["pv_per_share_ugx"] for r in rows)}

if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as f:
        print(json.dumps(calculate(json.load(f)), indent=2, allow_nan=False))
