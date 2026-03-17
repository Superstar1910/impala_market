import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import requests


def ensure_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("impala_bond_mvp")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(log_dir / "app.log", encoding="utf-8")
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)

    return logger


def build_health_report(df: pd.DataFrame, source_path: str, stale_hours: int, expected_columns: Iterable[str]) -> dict:
    latest = None
    data_age_hours = None

    if "report_date" in df.columns:
        latest_ts = pd.to_datetime(df["report_date"], errors="coerce").max()
        if pd.notna(latest_ts):
            latest = latest_ts.strftime("%Y-%m-%d")
            now = datetime.now(timezone.utc)
            latest_utc = latest_ts.to_pydatetime().replace(tzinfo=timezone.utc)
            data_age_hours = max((now - latest_utc).total_seconds() / 3600.0, 0.0)

    missing_cols = [c for c in expected_columns if c not in df.columns]

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source_path": source_path,
        "rows": int(len(df)),
        "latest_report_date": latest,
        "data_age_hours": data_age_hours,
        "stale_threshold_hours": stale_hours,
        "is_stale": bool(data_age_hours is not None and data_age_hours > stale_hours),
        "missing_columns": missing_cols,
    }


def maybe_send_webhook(webhook_url: str, title: str, payload: dict) -> Tuple[bool, str]:
    if not webhook_url:
        return False, "Webhook URL not configured."

    body = {
        "title": title,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if 200 <= resp.status_code < 300:
            return True, f"Webhook sent ({resp.status_code})."
        return False, f"Webhook failed ({resp.status_code}): {resp.text[:180]}"
    except Exception as exc:
        return False, f"Webhook error: {exc}"

