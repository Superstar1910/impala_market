import copy
import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("recovery", Path(__file__).resolve().parents[1] / "models/recovery.py")
model = importlib.util.module_from_spec(spec)
spec.loader.exec_module(model)

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.data = {"shares": 10, "opening_cash_ugx": 100, "discount_rate": 0.1,
                     "scenarios": [{"name": "one", "probability": 1, "recovery_usd": 10,
                                    "fx": 2, "costs_usd": 1, "liabilities_ugx": 8,
                                    "reserve_ugx": 0, "years": 1}]}
    def test_cash_bridge_and_discount(self):
        result = model.calculate(self.data)
        self.assertAlmostEqual(result["expected_pv_per_share_ugx"], 10)
        self.assertEqual(result["scenarios"][0]["cash_before_equity_floor_ugx"], 110)
    def test_paid_dividend_not_readded(self):
        self.data["opening_cash_ugx"] = 50
        result = model.calculate(self.data)
        self.assertEqual(result["scenarios"][0]["cash_before_equity_floor_ugx"], 60)
    def test_insolvency_flag(self):
        self.data["scenarios"][0]["liabilities_ugx"] = 200
        r = model.calculate(self.data)["scenarios"][0]
        self.assertEqual(r["pv_per_share_ugx"], 0)
        self.assertEqual(r["shortfall_ugx"], 82)
    def test_probability_rejected(self):
        self.data["scenarios"][0]["probability"] = 0.5
        with self.assertRaises(ValueError): model.calculate(self.data)
    def test_invalid_inputs(self):
        for key, value in [("shares", 0), ("discount_rate", -1), ("opening_cash_ugx", float("nan"))]:
            d = copy.deepcopy(self.data); d[key] = value
            with self.assertRaises(ValueError): model.calculate(d)
if __name__ == "__main__": unittest.main()
