import os
import time

import numpy as np
import pandas as pd

DEFAULT_DATA_PATH = "https://drive.google.com/uc?export=download&id=17DGvu69IpPPSdh1GSSWAiNLurJqu87Gx"
DEFAULT_FALLBACK_PATH = os.getenv("LOCAL_FALLBACK_CSV", "data/serving/bou_unified_master_analysis_dataset_v2.csv")


def _to_datetime_safe(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _to_numeric_safe(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _read_csv_with_retry(path: str, retries: int = 3, backoff_seconds: float = 1.5) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pd.read_csv(path)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)
    raise last_error


def load_data(path: str, fallback_path: str = DEFAULT_FALLBACK_PATH) -> pd.DataFrame:
    try:
        df = _read_csv_with_retry(path)
    except Exception:
        if fallback_path and os.path.exists(fallback_path):
            df = _read_csv_with_retry(fallback_path)
        else:
            raise

    for c in ["report_date", "auction_date", "value_date", "maturity_date", "settlement_date"]:
        if c in df.columns:
            df[c] = _to_datetime_safe(df[c])

    for c in ["amount_cost_ugx", "amount_fv_ugx", "amount_offered_ugx", "amount_tendered_ugx", "amount_accepted_ugx", "yield_pct", "ytm_pct", "coupon_pct", "price_per_100"]:
        if c in df.columns:
            df[c] = _to_numeric_safe(df[c])

    if "yield_pct" not in df.columns and "ytm_pct" in df.columns:
        df["yield_pct"] = df["ytm_pct"]

    if "turnover_ugx" not in df.columns:
        if "amount_cost_ugx" in df.columns:
            df["turnover_ugx"] = df["amount_cost_ugx"]
        elif "amount_fv_ugx" in df.columns:
            df["turnover_ugx"] = df["amount_fv_ugx"]
        else:
            df["turnover_ugx"] = np.nan

    if "record_type" in df.columns:
        df["record_type"] = df["record_type"].fillna("")
    if "market_segment" in df.columns:
        df["market_segment"] = df["market_segment"].fillna("")

    df["is_auction"] = (
        (df.get("record_type", "") == "auction_result") |
        (df.get("market_segment", "") == "primary_auction")
    )
    df["is_secondary"] = (
        (df.get("record_type", "") == "secondary_trade") |
        (df.get("market_segment", "") == "secondary_market")
    )

    if "security_key" not in df.columns:
        df["security_key"] = ""
    if "security_isin" not in df.columns:
        df["security_isin"] = ""

    return df


def get_auctions(df: pd.DataFrame) -> pd.DataFrame:
    auctions = df[df["is_auction"]].copy()
    key_cols = [
        "report_date", "auction_date", "value_date", "instrument_type", "instrument_label",
        "tenor_bucket", "security_key", "security_isin", "yield_pct", "price_per_100",
        "amount_offered_ugx", "amount_tendered_ugx", "amount_accepted_ugx", "bid_to_cover"
    ]
    cols = [c for c in key_cols if c in auctions.columns]
    return auctions[cols].sort_values(by="report_date", ascending=False)


def get_secondary(df: pd.DataFrame) -> pd.DataFrame:
    sec = df[df["is_secondary"]].copy()
    key_cols = [
        "report_date", "value_date", "instrument_type", "instrument_label", "tenor_bucket",
        "security_key", "security_isin", "yield_pct", "price_per_100", "turnover_ugx", "amount_fv_ugx"
    ]
    cols = [c for c in key_cols if c in sec.columns]
    return sec[cols].sort_values(by="report_date", ascending=False)


def daily_turnover(secondary: pd.DataFrame) -> pd.DataFrame:
    if secondary.empty:
        return pd.DataFrame(columns=["report_date", "turnover_ugx", "trade_count", "avg_trade_size_ugx"])
    g = secondary.groupby("report_date", dropna=True).agg(
        turnover_ugx=("turnover_ugx", "sum"),
        trade_count=("turnover_ugx", "count"),
    ).reset_index().sort_values("report_date")
    g["avg_trade_size_ugx"] = g["turnover_ugx"] / g["trade_count"].replace(0, np.nan)
    return g


def auction_window_liquidity(secondary: pd.DataFrame, auctions: pd.DataFrame, window_days: int = 2) -> pd.DataFrame:
    if secondary.empty or auctions.empty:
        return pd.DataFrame(columns=["auction_date", "offset", "turnover_ugx", "trade_count"])

    auction_dates = auctions["report_date"].dropna().drop_duplicates().sort_values()
    sec = secondary.copy().dropna(subset=["report_date"])
    sec_idx = sec.groupby("report_date", dropna=True).agg(
        turnover_ugx=("turnover_ugx", "sum"),
        trade_count=("turnover_ugx", "count"),
    )

    rows = []
    for ad in auction_dates:
        for offset in range(0, window_days + 1):
            day = ad + pd.Timedelta(days=offset)
            if day in sec_idx.index:
                row = sec_idx.loc[day]
                turnover_ugx = float(row["turnover_ugx"])
                trade_count = int(row["trade_count"])
            else:
                turnover_ugx = 0.0
                trade_count = 0
            rows.append(
                {
                    "auction_date": ad,
                    "offset": f"D+{offset}",
                    "turnover_ugx": turnover_ugx,
                    "trade_count": trade_count,
                }
            )
    return pd.DataFrame(rows)


def latest_curve(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d = d.dropna(subset=["report_date", "maturity_date", "yield_pct"])
    if d.empty:
        return pd.DataFrame(columns=["report_date", "tenor_years", "yield_pct", "security_key", "instrument_type"])

    latest_date = d["report_date"].max()
    snap = d[d["report_date"] == latest_date].copy()
    snap["tenor_years"] = (snap["maturity_date"] - snap["report_date"]).dt.days / 365.25
    snap = snap[(snap["tenor_years"] > 0) & (snap["tenor_years"] < 60)]
    return snap[["report_date", "tenor_years", "yield_pct", "security_key", "instrument_type"]].sort_values("tenor_years")


def curve_snapshot(df: pd.DataFrame, date_value: pd.Timestamp) -> pd.DataFrame:
    d = df.copy()
    d = d.dropna(subset=["report_date", "maturity_date", "yield_pct"])
    d = d[d["report_date"] == date_value].copy()
    d["tenor_years"] = (d["maturity_date"] - d["report_date"]).dt.days / 365.25
    d = d[(d["tenor_years"] > 0) & (d["tenor_years"] < 60)]
    return d[["report_date", "tenor_years", "yield_pct", "security_key", "instrument_type"]].sort_values("tenor_years")


def build_alerts(auctions: pd.DataFrame, turnover_daily: pd.DataFrame, curve_latest_df: pd.DataFrame) -> pd.DataFrame:
    alerts = []

    if not auctions.empty and "yield_pct" in auctions.columns:
        recent = auctions.dropna(subset=["yield_pct"]).sort_values("report_date").tail(25)
        if len(recent) >= 6:
            last = recent.iloc[-1]
            avg = recent.iloc[:-1]["yield_pct"].mean()
            shock = float(last["yield_pct"] - avg)
            if abs(shock) >= 1.0:
                alerts.append(
                    {
                        "timestamp": pd.Timestamp.utcnow(),
                        "module": "Auction",
                        "severity": "high" if abs(shock) >= 2.0 else "medium",
                        "message": f"Auction yield shock: {shock:+.2f}pp vs trailing mean",
                    }
                )

    if not turnover_daily.empty:
        td = turnover_daily.dropna(subset=["turnover_ugx"]).sort_values("report_date")
        if len(td) >= 15:
            base = td.iloc[:-1]["turnover_ugx"].median()
            last = td.iloc[-1]["turnover_ugx"]
            if base > 0 and last >= 1.8 * base:
                alerts.append(
                    {
                        "timestamp": pd.Timestamp.utcnow(),
                        "module": "Secondary",
                        "severity": "medium",
                        "message": f"Turnover spike: {last:,.0f} UGX vs median {base:,.0f} UGX",
                    }
                )

    if not curve_latest_df.empty:
        c = curve_latest_df.sort_values("tenor_years")
        if len(c) >= 2:
            ychg = c["yield_pct"].max() - c["yield_pct"].min()
            if ychg >= 5:
                alerts.append(
                    {
                        "timestamp": pd.Timestamp.utcnow(),
                        "module": "Curve",
                        "severity": "low",
                        "message": f"Wide curve dispersion detected: {ychg:.2f}pp",
                    }
                )

    if not alerts:
        alerts.append(
            {
                "timestamp": pd.Timestamp.utcnow(),
                "module": "System",
                "severity": "low",
                "message": "No critical alerts. Data pipeline appears stable.",
            }
        )

    return pd.DataFrame(alerts).sort_values("timestamp", ascending=False)
