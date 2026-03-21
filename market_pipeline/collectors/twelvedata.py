"""
collectors/twelvedata.py
Twelve Data → fact_market_timeseries  (OHLCV)
"""

import os
import time
import requests
from datetime import datetime, timezone
from psycopg2.extras import Json

from db.connection import (
    get_connection, get_source_id, get_symbol_id, get_interval_id
)

KEY  = os.getenv("TWELVEDATA_API_KEY", "")
BASE = "https://api.twelvedata.com"


def _get(endpoint: str, params: dict) -> dict:
    params["apikey"] = KEY
    r = requests.get(f"{BASE}{endpoint}", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") == "error":
        raise ValueError(data.get("message", "Twelve Data error"))
    return data


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"bad dt: {s}")


def _fetch(symbol: str, interval: str, outputsize: int) -> dict:
    return _get("/time_series", {
        "symbol":     symbol,
        "interval":   interval,
        "outputsize": outputsize,
        "timezone":   "UTC",
    })


def _save(symbol: str, data: dict, interval: str):
    rows = []
    for item in data.get("values", []):
        try:
            ts = _parse_dt(item["datetime"])
        except ValueError:
            continue
        rows.append((
            ts,
            float(item["open"])   if item.get("open")   else None,
            float(item["high"])   if item.get("high")   else None,
            float(item["low"])    if item.get("low")    else None,
            float(item["close"])  if item.get("close")  else None,
            float(item["volume"]) if item.get("volume") else None,
            Json(item),
        ))

    if not rows:
        return 0

    sql = """
    INSERT INTO fact_market_timeseries
        (symbol_id, source_id, interval_id, candle_time_utc,
         open, high, low, close, volume, raw_payload)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (symbol_id, interval_id, candle_time_utc) DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        volume      = EXCLUDED.volume,
        raw_payload = EXCLUDED.raw_payload
    """
    with get_connection() as conn:
        src_id = get_source_id(conn, "twelvedata")
        sym_id = get_symbol_id(conn, symbol)
        int_id = get_interval_id(conn, interval)
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, (sym_id, src_id, int_id) + row)
        conn.commit()
    return len(rows)


# ─── ENTRY POINT ──────────────────────────────────────────────
def run(symbol: str, outputsize: int = 30):
    for interval, size in [("1day", outputsize), ("1min", min(outputsize, 30))]:
        try:
            data  = _fetch(symbol, interval, size)
            saved = _save(symbol, data, interval)
            print(f"  [TwelveData] {interval} → {symbol} ({saved} candles)")
        except Exception as e:
            print(f"  [TwelveData] ERROR {interval} {symbol}: {e}")
        time.sleep(8)
