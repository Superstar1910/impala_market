import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from data_loader import (
    DEFAULT_DATA_PATH,
    auction_window_liquidity,
    build_alerts,
    curve_snapshot,
    daily_turnover,
    get_auctions,
    get_secondary,
    latest_curve,
    load_data,
)
from ops import build_health_report, ensure_logger, maybe_send_webhook

st.set_page_config(page_title="Bond Market Intelligence MVP", layout="wide")
logger = ensure_logger(Path(__file__).resolve().parent / "logs")


def _bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _secrets():
    try:
        if "app" in st.secrets:
            return st.secrets["app"]
    except Exception:
        pass
    return {}


APP = _secrets()
AUTH_REQUIRED = _bool(APP.get("auth_required", os.getenv("APP_AUTH_REQUIRED", "false")))
PASSCODE = str(APP.get("passcode", os.getenv("APP_PASSCODE", ""))).strip()
CACHE_TTL_SECONDS = int(APP.get("cache_ttl_seconds", os.getenv("CACHE_TTL_SECONDS", "900")))
STALE_DATA_HOURS = int(APP.get("stale_data_hours", os.getenv("STALE_DATA_HOURS", "48")))
WEBHOOK_URL = str(APP.get("webhook_url", os.getenv("WEBHOOK_URL", ""))).strip()


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def get_data(path: str):
    df = load_data(path)
    auctions = get_auctions(df)
    secondary = get_secondary(df)
    turn = daily_turnover(secondary)
    aw = auction_window_liquidity(secondary, auctions, window_days=2)
    lcurve = latest_curve(df)
    alerts = build_alerts(auctions, turn, lcurve)
    return df, auctions, secondary, turn, aw, lcurve, alerts


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def enforce_auth():
    if not AUTH_REQUIRED:
        return
    if not PASSCODE:
        st.error("Auth is enabled but no passcode configured. Set APP_PASSCODE or secrets.app.passcode.")
        st.stop()

    with st.sidebar:
        st.subheader("Access")
        user_code = st.text_input("Passcode", type="password")
        if user_code != PASSCODE:
            st.warning("Enter valid passcode to continue.")
            st.stop()


st.title("African Bond Market Intelligence - Clickable MVP")
st.caption("Uganda-first prototype (auctions, secondary liquidity, curves, alerts)")
enforce_auth()

with st.sidebar:
    st.header("Controls")
    data_path = st.text_input("Unified dataset CSV path", value=DEFAULT_DATA_PATH)
    page = st.radio(
        "Page",
        ["Dashboard", "Auctions", "Secondary", "Yield Curve", "Instruments", "Alerts", "Ops"],
        index=0,
    )

try:
    df, auctions, secondary, turnover, auction_window, lcurve, alerts = get_data(data_path)
except Exception as ex:
    logger.exception("Data load failure: %s", ex)
    st.error(f"Failed to load data: {ex}")
    st.stop()

health = build_health_report(
    df=df,
    source_path=data_path,
    stale_hours=STALE_DATA_HOURS,
    expected_columns=("report_date", "instrument_type", "record_type", "market_segment", "security_key"),
)

with st.sidebar:
    st.subheader("Filters")
    if "instrument_type" in df.columns:
        itypes = sorted([x for x in df["instrument_type"].dropna().unique().tolist() if str(x).strip()])
        selected_itypes = st.multiselect("Instrument Type", options=itypes, default=itypes)
    else:
        selected_itypes = []

    min_date = pd.to_datetime(df["report_date"].min())
    max_date = pd.to_datetime(df["report_date"].max())
    date_range = st.date_input("Report Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    if st.button("Clear cache"):
        st.cache_data.clear()
        st.success("Cache cleared.")

f_df = df.copy()
if selected_itypes:
    f_df = f_df[f_df["instrument_type"].isin(selected_itypes)]
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_dt = pd.to_datetime(date_range[0])
    end_dt = pd.to_datetime(date_range[1])
    f_df = f_df[(f_df["report_date"] >= start_dt) & (f_df["report_date"] <= end_dt)]

f_auctions = auctions.copy()
f_secondary = secondary.copy()
if selected_itypes:
    if "instrument_type" in f_auctions.columns:
        f_auctions = f_auctions[f_auctions["instrument_type"].isin(selected_itypes)]
    if "instrument_type" in f_secondary.columns:
        f_secondary = f_secondary[f_secondary["instrument_type"].isin(selected_itypes)]
if isinstance(date_range, tuple) and len(date_range) == 2:
    f_auctions = f_auctions[(f_auctions["report_date"] >= start_dt) & (f_auctions["report_date"] <= end_dt)]
    f_secondary = f_secondary[(f_secondary["report_date"] >= start_dt) & (f_secondary["report_date"] <= end_dt)]

if page == "Dashboard":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows (filtered)", f"{len(f_df):,}")
    c2.metric("Auction Rows", f"{len(f_auctions):,}")
    c3.metric("Secondary Rows", f"{len(f_secondary):,}")
    c4.metric(
        "Unique ISIN",
        f"{f_df['security_isin'].dropna().astype(str).str.strip().replace('', pd.NA).dropna().nunique():,}"
        if "security_isin" in f_df.columns
        else "n/a",
    )

    if health["is_stale"]:
        st.warning(f"Data stale warning: latest report date is {health['latest_report_date']}.")

    st.subheader("Daily Secondary Turnover")
    turn_f = daily_turnover(f_secondary)
    if not turn_f.empty:
        fig = px.line(turn_f, x="report_date", y="turnover_ugx", title="Turnover (UGX)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No turnover data for selected filters.")

    st.subheader("Latest Yield Curve Snapshot")
    lc = latest_curve(f_df)
    if not lc.empty:
        fig2 = px.line(lc, x="tenor_years", y="yield_pct", color="instrument_type", markers=True, title="Latest Curve")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No curve data available.")

    st.download_button("Download filtered dataset CSV", to_csv_bytes(f_df), file_name="filtered_dataset.csv", mime="text/csv")

elif page == "Auctions":
    st.subheader("Auction Monitor")
    st.dataframe(f_auctions.head(200), use_container_width=True)

    if not f_auctions.empty and "yield_pct" in f_auctions.columns:
        y = f_auctions.dropna(subset=["yield_pct"]).sort_values("report_date")
        if not y.empty:
            fig = px.line(y, x="report_date", y="yield_pct", color="instrument_type", title="Auction Yield Trend")
            st.plotly_chart(fig, use_container_width=True)

            recent = y.tail(15)
            if len(recent) >= 2:
                actual = recent.iloc[-1]["yield_pct"]
                expected = recent.iloc[:-1]["yield_pct"].mean()
                st.metric("Auction Surprise (pp)", f"{actual - expected:+.2f}", help="Actual latest yield minus trailing average")

    st.download_button("Download auctions CSV", to_csv_bytes(f_auctions), file_name="auctions_filtered.csv", mime="text/csv")

elif page == "Secondary":
    st.subheader("Secondary Market Liquidity")
    turn_f = daily_turnover(f_secondary)

    c1, c2 = st.columns(2)
    with c1:
        if not turn_f.empty:
            fig = px.bar(turn_f.tail(60), x="report_date", y="turnover_ugx", title="Daily Turnover (Last 60 Obs)")
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        if not turn_f.empty:
            fig2 = px.line(turn_f.tail(120), x="report_date", y="avg_trade_size_ugx", title="Avg Trade Size")
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Liquidity Around Auction Dates (D0-D+2)")
    awf = auction_window_liquidity(f_secondary, f_auctions, window_days=2)
    if not awf.empty:
        agg = awf.groupby("offset", as_index=False)["turnover_ugx"].mean()
        fig3 = px.bar(agg, x="offset", y="turnover_ugx", title="Average Turnover by Offset")
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(awf.tail(100), use_container_width=True)
    else:
        st.info("No auction-window liquidity data available.")

    st.download_button("Download secondary CSV", to_csv_bytes(f_secondary), file_name="secondary_filtered.csv", mime="text/csv")

elif page == "Yield Curve":
    st.subheader("Yield Curve Monitor")
    dvals = sorted(f_df["report_date"].dropna().unique())
    if len(dvals) < 2:
        st.info("Not enough dated observations for curve comparison.")
    else:
        d1 = st.selectbox("Date A", options=dvals, index=max(0, len(dvals) - 2), format_func=lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"))
        d2 = st.selectbox("Date B", options=dvals, index=len(dvals) - 1, format_func=lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"))

        c1 = curve_snapshot(f_df, pd.to_datetime(d1))
        c2 = curve_snapshot(f_df, pd.to_datetime(d2))

        if not c1.empty:
            c1 = c1.assign(snapshot="Date A")
        if not c2.empty:
            c2 = c2.assign(snapshot="Date B")
        curve_cmp = pd.concat([c1, c2], ignore_index=True)

        if not curve_cmp.empty:
            fig = px.line(curve_cmp, x="tenor_years", y="yield_pct", color="snapshot", markers=True, title="Curve Comparison")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(curve_cmp.sort_values(["snapshot", "tenor_years"]), use_container_width=True)
            st.download_button("Download curve comparison CSV", to_csv_bytes(curve_cmp), file_name="curve_comparison.csv", mime="text/csv")
        else:
            st.info("No curve points available for chosen dates.")

elif page == "Instruments":
    st.subheader("Instrument Explorer")

    query = st.text_input("Search by security_key / ISIN / label")
    inst = f_df.copy()
    keep_cols = [c for c in ["security_key", "security_isin", "instrument_label", "instrument_type", "maturity_date", "coupon_pct", "yield_pct", "price_per_100", "report_date", "turnover_ugx"] if c in inst.columns]
    inst = inst[keep_cols]

    if query.strip():
        q = query.lower().strip()
        mask = pd.Series(False, index=inst.index)
        for c in ["security_key", "security_isin", "instrument_label"]:
            if c in inst.columns:
                mask = mask | inst[c].astype(str).str.lower().str.contains(q, na=False)
        inst = inst[mask]

    st.dataframe(inst.head(300), use_container_width=True)
    st.download_button("Download instrument view CSV", to_csv_bytes(inst), file_name="instrument_view.csv", mime="text/csv")

elif page == "Alerts":
    st.subheader("Alerts Center")
    sev = st.multiselect("Severity", options=["high", "medium", "low"], default=["high", "medium", "low"])
    alerts_f = alerts[alerts["severity"].isin(sev)] if sev else alerts
    st.dataframe(alerts_f, use_container_width=True)
    st.download_button("Download alerts CSV", to_csv_bytes(alerts_f), file_name="alerts.csv", mime="text/csv")

else:
    st.subheader("Ops and Monitoring")
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", f"{health['rows']:,}")
    c2.metric("Latest report_date", health["latest_report_date"] or "n/a")
    c3.metric("Data age (hours)", f"{health['data_age_hours']:.1f}" if health["data_age_hours"] is not None else "n/a")

    st.write("**Health summary**")
    st.json(health)

    if health["missing_columns"]:
        st.error(f"Missing expected columns: {', '.join(health['missing_columns'])}")
    if health["is_stale"]:
        st.warning(f"Staleness threshold breached (> {STALE_DATA_HOURS}h).")

    st.write("**Runtime settings**")
    st.code(
        f"CACHE_TTL_SECONDS={CACHE_TTL_SECONDS}\n"
        f"AUTH_REQUIRED={AUTH_REQUIRED}\n"
        f"WEBHOOK_CONFIGURED={'yes' if WEBHOOK_URL else 'no'}\n"
        f"STALE_DATA_HOURS={STALE_DATA_HOURS}",
        language="bash",
    )

    if st.button("Send health status to webhook"):
        ok, msg = maybe_send_webhook(WEBHOOK_URL, "Bond MVP Health", health)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

logger.info("Rendered page=%s rows=%s", page, len(f_df))
