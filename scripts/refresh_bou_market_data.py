#!/usr/bin/env python
"""
Single reusable BoU scraper/parser pipeline.

Flow:
1) Discover reports from BoU pages
2) Download source files into lake/raw
3) Parse secondary-market + auction reports into normalized rows
4) Persist parquet partitions in lake/normalized
5) Build DuckDB warehouse tables + serving extracts

Usage:
  python scripts/refresh_bou_market_data.py --start-date 2025-01-02 --end-date 2026-03-18
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import duckdb
import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

SECONDARY_URL = "https://www.bou.or.ug/bouwebsite/FinancialMarkets/DailySecondaryMarket.html"
TBILLS_URL = "https://www.bou.or.ug/bouwebsite/FinancialMarkets/tbillsauctionresults.html"
TBONDS_URL = "https://www.bou.or.ug/bouwebsite/FinancialMarkets/tbondsyieldcurve.html"


@dataclass
class LinkItem:
    market_type: str
    source_url: str
    filename: str
    report_date: Optional[date]
    ext: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", type=str, default="2025-01-01")
    p.add_argument("--end-date", type=str, default=date.today().isoformat())
    p.add_argument("--root", type=str, default="data")
    p.add_argument("--timeout", type=int, default=45)
    p.add_argument("--user-agent", type=str, default="impala-market-bou-pipeline/1.0")
    return p.parse_args()


def ensure_dirs(root: Path) -> Dict[str, Path]:
    d = {
        "lake_raw": root / "lake" / "raw",
        "lake_normalized": root / "lake" / "normalized",
        "lake_curated": root / "lake" / "curated",
        "warehouse": root / "warehouse",
        "serving": root / "serving",
        "logs": root / "logs",
    }
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    return d


def to_date(s: str) -> Optional[date]:
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%d-%b-%y", "%d-%B-%y", "%d/%m/%Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def infer_report_date_from_text(text: str) -> Optional[date]:
    pats = [
        r"(\d{1,2})[-_ ]([A-Za-z]{3,9})[-_ ](\d{4})",
        r"(\d{1,2})[-_ ]([A-Za-z]{3,9})[-_ ](\d{2})",
    ]
    for pat in pats:
        m = re.search(pat, text)
        if m:
            d, mon, y = m.group(1), m.group(2), m.group(3)
            if len(y) == 2:
                y = "20" + y
            return to_date(f"{int(d):02d}-{mon[:3]}-{y}")
    return None


def discover_links(session: requests.Session, page_url: str, market_type: str, timeout: int) -> List[LinkItem]:
    r = session.get(page_url, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    out: List[LinkItem] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        ext = Path(urlparse(full).path).suffix.lower()
        if ext not in (".pdf", ".xls", ".xlsx", ".csv"):
            continue
        filename = Path(urlparse(full).path).name
        rpt = infer_report_date_from_text(filename) or infer_report_date_from_text(a.get_text(" ", strip=True))
        out.append(LinkItem(market_type=market_type, source_url=full, filename=filename, report_date=rpt, ext=ext))
    # Deduplicate by URL
    uniq = {}
    for x in out:
        uniq[x.source_url] = x
    return list(uniq.values())


def in_range(d: Optional[date], start: date, end: date) -> bool:
    if d is None:
        return False
    return start <= d <= end


def download_file(session: requests.Session, url: str, path: Path, timeout: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    path.write_bytes(r.content)


def normalize_num(val: str) -> Optional[float]:
    if val is None:
        return None
    t = str(val).strip().replace(",", "").replace("%", "")
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_secondary_pdf(pdf_path: Path, report_date: date, source_url: str) -> List[dict]:
    rows: List[dict] = []
    txt_parts: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p in pdf.pages:
            txt_parts.append(p.extract_text() or "")
    text = "\n".join(txt_parts)
    # Typical row:
    # 10-Mar-2026 0.000% 08-Jun-2026 3M 10.737 97.323 25,000,000,000 24,330,750,000
    line_re = re.compile(
        r"(?P<value_date>\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{2,4})\s+"
        r"(?P<coupon>\d+(?:\.\d+)?%?)\s+"
        r"(?P<maturity_date>\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{2,4})\s+"
        r"(?P<mmy>[A-Za-z0-9\.-]+)\s+"
        r"(?P<ytm>\d+(?:\.\d+)?)\s+"
        r"(?P<price>\d+(?:\.\d+)?)\s+"
        r"(?P<amount_fv>[\d,]+(?:\.\d+)?)\s+"
        r"(?P<amount_cost>[\d,]+(?:\.\d+)?)"
    )
    for ln in text.splitlines():
        m = line_re.search(ln)
        if not m:
            continue
        coupon_num = normalize_num(m.group("coupon"))
        instrument_type = "T-Bill" if coupon_num is not None and abs(coupon_num) < 1e-9 else "T-Bond"
        rows.append(
            {
                "market_type": "secondary",
                "record_type": "secondary_trade",
                "market_segment": "secondary_market",
                "report_date": report_date.isoformat(),
                "auction_date": None,
                "value_date": (to_date(m.group("value_date")) or report_date).isoformat(),
                "maturity_date": (to_date(m.group("maturity_date")) or report_date).isoformat(),
                "instrument_type": instrument_type,
                "instrument_label": m.group("mmy"),
                "coupon_pct": coupon_num,
                "yield_pct": normalize_num(m.group("ytm")),
                "ytm_pct": normalize_num(m.group("ytm")),
                "price_per_100": normalize_num(m.group("price")),
                "amount_fv_ugx": normalize_num(m.group("amount_fv")),
                "amount_cost_ugx": normalize_num(m.group("amount_cost")),
                "amount_offered_ugx": None,
                "amount_tendered_ugx": None,
                "amount_accepted_ugx": None,
                "bid_to_cover": None,
                "security_isin": None,
                "security_key": None,
                "tenor_bucket": m.group("mmy"),
                "source_url": source_url,
                "source_file": str(pdf_path),
                "parse_method": "pdf_regex_secondary",
                "data_confidence_score": 0.70,
            }
        )
    return rows


def parse_tbill_pdf(pdf_path: Path, report_date: date, source_url: str) -> List[dict]:
    rows: List[dict] = []
    txt_parts: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p in pdf.pages:
            txt_parts.append(p.extract_text() or "")
    text = "\n".join(txt_parts)
    # Example:
    # 91-Day T-Bill 28 May 2026 97.323 10.737 11.498 25,000,000,000 47,436,600,000 16,181,600,000 2.93
    line_re = re.compile(
        r"(?P<label>(?:91|182|364)[-\s]Day\s+T[-\s]?Bill)\s+"
        r"(?P<maturity>\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\s+"
        r"(?P<price>\d+(?:\.\d+)?)\s+"
        r"(?P<disc>\d+(?:\.\d+)?)\s+"
        r"(?P<eff>\d+(?:\.\d+)?)\s+"
        r"(?P<offered>[\d,]+)\s+"
        r"(?P<tendered>[\d,]+)\s+"
        r"(?P<accepted>[\d,]+)\s+"
        r"(?P<btc>\d+(?:\.\d+)?)"
    )
    for ln in text.splitlines():
        m = line_re.search(ln)
        if not m:
            continue
        rows.append(
            {
                "market_type": "auction_tbill",
                "record_type": "auction_result",
                "market_segment": "primary_auction",
                "report_date": report_date.isoformat(),
                "auction_date": report_date.isoformat(),
                "value_date": None,
                "maturity_date": (to_date(m.group("maturity")) or report_date).isoformat(),
                "instrument_type": "T-Bill",
                "instrument_label": m.group("label"),
                "coupon_pct": 0.0,
                "yield_pct": normalize_num(m.group("eff")),
                "ytm_pct": normalize_num(m.group("eff")),
                "price_per_100": normalize_num(m.group("price")),
                "amount_fv_ugx": None,
                "amount_cost_ugx": None,
                "amount_offered_ugx": normalize_num(m.group("offered")),
                "amount_tendered_ugx": normalize_num(m.group("tendered")),
                "amount_accepted_ugx": normalize_num(m.group("accepted")),
                "bid_to_cover": normalize_num(m.group("btc")),
                "security_isin": None,
                "security_key": None,
                "tenor_bucket": m.group("label"),
                "source_url": source_url,
                "source_file": str(pdf_path),
                "parse_method": "pdf_regex_tbill",
                "data_confidence_score": 0.72,
            }
        )
    return rows


def parse_tbond_pdf(pdf_path: Path, report_date: date, source_url: str) -> List[dict]:
    rows: List[dict] = []
    txt_parts: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for p in pdf.pages:
            txt_parts.append(p.extract_text() or "")
    text = "\n".join(txt_parts)
    # Generic bond auction line (best effort)
    # 10-Year 14.250% 08-Nov-2035 13.850 98.123 300,000,000,000 650,000,000,000 280,000,000,000
    line_re = re.compile(
        r"(?P<label>\d{1,2}[-\s]Year(?:\s+Re-?Opening)?)\s+"
        r"(?P<coupon>\d+(?:\.\d+)?%?)\s+"
        r"(?P<maturity>\d{1,2}[-/ ]?[A-Za-z]{3,9}[-/ ]?\d{2,4})\s+"
        r"(?P<yield>\d+(?:\.\d+)?)\s+"
        r"(?P<price>\d+(?:\.\d+)?)\s+"
        r"(?P<offered>[\d,]+)\s+"
        r"(?P<tendered>[\d,]+)\s+"
        r"(?P<accepted>[\d,]+)"
    )
    for ln in text.splitlines():
        m = line_re.search(ln)
        if not m:
            continue
        rows.append(
            {
                "market_type": "auction_tbond",
                "record_type": "auction_result",
                "market_segment": "primary_auction",
                "report_date": report_date.isoformat(),
                "auction_date": report_date.isoformat(),
                "value_date": None,
                "maturity_date": (to_date(m.group("maturity")) or report_date).isoformat(),
                "instrument_type": "T-Bond",
                "instrument_label": m.group("label"),
                "coupon_pct": normalize_num(m.group("coupon")),
                "yield_pct": normalize_num(m.group("yield")),
                "ytm_pct": normalize_num(m.group("yield")),
                "price_per_100": normalize_num(m.group("price")),
                "amount_fv_ugx": None,
                "amount_cost_ugx": None,
                "amount_offered_ugx": normalize_num(m.group("offered")),
                "amount_tendered_ugx": normalize_num(m.group("tendered")),
                "amount_accepted_ugx": normalize_num(m.group("accepted")),
                "bid_to_cover": (
                    normalize_num(m.group("tendered")) / normalize_num(m.group("accepted"))
                    if normalize_num(m.group("accepted"))
                    else None
                ),
                "security_isin": None,
                "security_key": None,
                "tenor_bucket": m.group("label"),
                "source_url": source_url,
                "source_file": str(pdf_path),
                "parse_method": "pdf_regex_tbond",
                "data_confidence_score": 0.66,
            }
        )
    return rows


def build_security_key(df: pd.DataFrame) -> pd.Series:
    sec = (
        df["instrument_type"].fillna("")
        + "|"
        + df["instrument_label"].fillna("")
        + "|"
        + df["maturity_date"].fillna("")
        + "|"
        + df["coupon_pct"].fillna(0).astype(str)
    )
    return sec.str.replace(r"\s+", "", regex=True)


def save_parquet_partitions(df: pd.DataFrame, out_base: Path) -> None:
    if df.empty:
        return
    df = df.copy()
    df["report_year"] = pd.to_datetime(df["report_date"], errors="coerce").dt.year.fillna(0).astype(int)
    for (mkt, yr), g in df.groupby(["market_type", "report_year"], dropna=False):
        out_dir = out_base / f"market_type={mkt}" / f"report_year={yr}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        g.to_parquet(out_path, index=False)


def write_log(log_path: Path, rows: List[dict]) -> None:
    if not rows:
        return
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with log_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def build_duckdb(df: pd.DataFrame, db_path: Path, serving_dir: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("CREATE OR REPLACE TABLE bou_market_raw AS SELECT * FROM df")
    con.execute(
        """
        CREATE OR REPLACE TABLE bou_market_curated AS
        SELECT
          *,
          COALESCE(security_key, '') AS security_key_norm,
          CASE
            WHEN amount_cost_ugx IS NOT NULL THEN amount_cost_ugx
            WHEN amount_fv_ugx IS NOT NULL THEN amount_fv_ugx
            ELSE NULL
          END AS turnover_ugx
        FROM bou_market_raw
        """
    )
    # Serving extracts
    daily_turnover = con.execute(
        """
        SELECT report_date, SUM(turnover_ugx) AS turnover_ugx, COUNT(*) AS trade_count
        FROM bou_market_curated
        WHERE market_segment = 'secondary_market'
        GROUP BY report_date
        ORDER BY report_date
        """
    ).df()
    latest_curve = con.execute(
        """
        SELECT report_date, instrument_type, instrument_label, maturity_date, yield_pct, security_key_norm AS security_key
        FROM bou_market_curated
        WHERE yield_pct IS NOT NULL
        ORDER BY report_date DESC
        LIMIT 1000
        """
    ).df()
    serving_dir.mkdir(parents=True, exist_ok=True)
    daily_turnover.to_csv(serving_dir / "daily_turnover.csv", index=False)
    latest_curve.to_csv(serving_dir / "latest_curve_points.csv", index=False)
    df.to_csv(serving_dir / "bou_unified_master_analysis_dataset_v2.csv", index=False)
    con.close()


def main() -> int:
    args = parse_args()
    start = to_date(args.start_date)
    end = to_date(args.end_date)
    if start is None or end is None:
        print("Invalid start/end date. Use YYYY-MM-DD.")
        return 2

    root = Path(args.root)
    dirs = ensure_dirs(root)
    log_rows: List[dict] = []
    parsed_rows: List[dict] = []

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})

    pages = [
        ("secondary", SECONDARY_URL),
        ("auction_tbill", TBILLS_URL),
        ("auction_tbond", TBONDS_URL),
    ]

    discovered: List[LinkItem] = []
    for mkt, page in pages:
        try:
            discovered.extend(discover_links(session, page, mkt, args.timeout))
        except Exception as exc:
            log_rows.append(
                {
                    "status": "failed",
                    "market_type": mkt,
                    "source_url": page,
                    "report_date": None,
                    "local_path": None,
                    "parsed_rows": 0,
                    "error_message": f"discover_failed: {exc}",
                }
            )

    filtered = [x for x in discovered if in_range(x.report_date, start, end)]

    for item in filtered:
        year = item.report_date.year if item.report_date else 0
        local_path = dirs["lake_raw"] / item.market_type / str(year) / item.filename
        try:
            download_file(session, item.source_url, local_path, args.timeout)
            extracted: List[dict] = []
            if item.ext == ".pdf":
                if item.market_type == "secondary":
                    extracted = parse_secondary_pdf(local_path, item.report_date, item.source_url)
                elif item.market_type == "auction_tbill":
                    extracted = parse_tbill_pdf(local_path, item.report_date, item.source_url)
                elif item.market_type == "auction_tbond":
                    extracted = parse_tbond_pdf(local_path, item.report_date, item.source_url)
            elif item.ext in (".xls", ".xlsx", ".csv"):
                # Fallback simple ingest for tabular files
                if item.ext == ".csv":
                    xdf = pd.read_csv(local_path)
                else:
                    xdf = pd.read_excel(local_path)
                xdf = xdf.copy()
                xdf["market_type"] = item.market_type
                xdf["report_date"] = item.report_date.isoformat() if item.report_date else None
                xdf["source_url"] = item.source_url
                xdf["source_file"] = str(local_path)
                xdf["parse_method"] = "tabular_ingest"
                extracted = xdf.to_dict(orient="records")

            parsed_rows.extend(extracted)
            log_rows.append(
                {
                    "status": "success",
                    "market_type": item.market_type,
                    "source_url": item.source_url,
                    "report_date": item.report_date.isoformat() if item.report_date else None,
                    "local_path": str(local_path),
                    "parsed_rows": len(extracted),
                    "error_message": "",
                }
            )
        except Exception as exc:
            log_rows.append(
                {
                    "status": "failed",
                    "market_type": item.market_type,
                    "source_url": item.source_url,
                    "report_date": item.report_date.isoformat() if item.report_date else None,
                    "local_path": str(local_path),
                    "parsed_rows": 0,
                    "error_message": str(exc),
                }
            )

    df = pd.DataFrame(parsed_rows)
    if not df.empty:
        # Ensure canonical columns
        canonical = [
            "market_type",
            "record_type",
            "market_segment",
            "report_date",
            "auction_date",
            "value_date",
            "maturity_date",
            "instrument_type",
            "instrument_label",
            "coupon_pct",
            "yield_pct",
            "ytm_pct",
            "price_per_100",
            "amount_fv_ugx",
            "amount_cost_ugx",
            "amount_offered_ugx",
            "amount_tendered_ugx",
            "amount_accepted_ugx",
            "bid_to_cover",
            "security_isin",
            "security_key",
            "tenor_bucket",
            "source_url",
            "source_file",
            "parse_method",
            "data_confidence_score",
        ]
        for c in canonical:
            if c not in df.columns:
                df[c] = None
        df = df[canonical].copy()
        df["security_key"] = df["security_key"].fillna("")
        missing = df["security_key"].eq("") | df["security_key"].isna()
        df.loc[missing, "security_key"] = build_security_key(df.loc[missing])
        df["data_confidence_band"] = pd.cut(
            df["data_confidence_score"].astype(float),
            bins=[-1, 0.5, 0.75, 1.0],
            labels=["low", "medium", "high"],
        ).astype(str)

        # Save normalized and curated
        save_parquet_partitions(df, dirs["lake_normalized"])
        df.to_parquet(dirs["lake_curated"] / "bou_market_curated.parquet", index=False)
        df.to_csv(dirs["lake_curated"] / "bou_market_curated.csv", index=False)

        # Build warehouse + serving
        build_duckdb(df, dirs["warehouse"] / "impala_market.duckdb", dirs["serving"])

    # Write run logs
    write_log(dirs["logs"] / f"parse_log_{start.isoformat()}_to_{end.isoformat()}.csv", log_rows)
    summary = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "links_discovered": len(discovered),
        "links_in_range": len(filtered),
        "rows_parsed": int(len(parsed_rows)),
        "success_files": sum(1 for r in log_rows if r["status"] == "success"),
        "failed_files": sum(1 for r in log_rows if r["status"] == "failed"),
        "output_root": str(root.resolve()),
    }
    (dirs["logs"] / f"summary_{start.isoformat()}_to_{end.isoformat()}.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
