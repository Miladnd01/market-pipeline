"""
collectors/alphavantage.py
Alpha Vantage → fact_market_indicator  (RSI, MACD, EMA, SMA)
"""

import os
import time
import requests
from datetime import datetime, timezone
from psycopg2.extras import Json

from db.connection import (
    get_connection, get_source_id, get_symbol_id,
    get_interval_id, get_indicator_id
)

KEY  = os.getenv("ALPHAVANTAGE_API_KEY", "")
BASE = "https://www.alphavantage.co/query"


def _get(params: dict) -> dict:
    params["apikey"] = KEY
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "Error Message" in data:
        raise ValueError(data["Error Message"])
    if "Note" in data:
        print(f"  [AlphaVantage] RATE LIMIT: {data['Note']}")
    return data


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"bad dt: {s}")


def _tech_key(data: dict, name: str):
    return next((k for k in data if name in k and "Technical" in k), None)


# ─── FETCH FUNCTIONS ──────────────────────────────────────────
def _fetch_rsi(symbol, interval, period=14):
    return _get({"function": "RSI", "symbol": symbol,
                 "interval": interval, "time_period": period,
                 "series_type": "close"})

def _fetch_macd(symbol, interval):
    return _get({"function": "MACD", "symbol": symbol,
                 "interval": interval, "series_type": "close",
                 "fastperiod": 12, "slowperiod": 26, "signalperiod": 9})

def _fetch_ema(symbol, interval, period=20):
    return _get({"function": "EMA", "symbol": symbol,
                 "interval": interval, "time_period": period,
                 "series_type": "close"})

def _fetch_sma(symbol, interval, period=50):
    return _get({"function": "SMA", "symbol": symbol,
                 "interval": interval, "time_period": period,
                 "series_type": "close"})


# ─── NORMALIZE ────────────────────────────────────────────────
def _norm_single(symbol, data, key_name, val_field, interval, ind_name, max_n):
    tk = _tech_key(data, key_name)
    if not tk:
        return []
    rows = []
    for dt_str, vals in list(data[tk].items())[:max_n]:
        try:
            ts = _parse_dt(dt_str)
        except ValueError:
            continue
        rows.append({
            "symbol": symbol, "indicator": ind_name, "interval": interval,
            "ts": ts,
            "value":      float(vals[val_field]) if vals.get(val_field) else None,
            "macd":       None, "macd_signal": None, "macd_hist": None,
            "raw":        {dt_str: vals},
        })
    return rows

def _norm_rsi(symbol, data, interval, max_n):
    return _norm_single(symbol, data, "RSI", "RSI", interval, "RSI", max_n)

def _norm_ema(symbol, data, interval, max_n):
    return _norm_single(symbol, data, "EMA", "EMA", interval, "EMA", max_n)

def _norm_sma(symbol, data, interval, max_n):
    return _norm_single(symbol, data, "SMA", "SMA", interval, "SMA", max_n)

def _norm_macd(symbol, data, interval, max_n):
    tk = _tech_key(data, "MACD")
    if not tk:
        return []
    rows = []
    for dt_str, vals in list(data[tk].items())[:max_n]:
        try:
            ts = _parse_dt(dt_str)
        except ValueError:
            continue
        rows.append({
            "symbol": symbol, "indicator": "MACD", "interval": interval,
            "ts": ts,
            "value":      float(vals.get("MACD") or 0),
            "macd":       float(vals["MACD"])        if vals.get("MACD")        else None,
            "macd_signal":float(vals["MACD_Signal"]) if vals.get("MACD_Signal") else None,
            "macd_hist":  float(vals["MACD_Hist"])   if vals.get("MACD_Hist")   else None,
            "raw":        {dt_str: vals},
        })
    return rows


# ─── SAVE ─────────────────────────────────────────────────────
def _save(rows: list[dict]):
    if not rows:
        return
    sql = """
    INSERT INTO fact_market_indicator
        (symbol_id, source_id, indicator_id, interval_id, candle_time_utc,
         value, macd, macd_signal, macd_hist, raw_payload)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (symbol_id, indicator_id, interval_id, candle_time_utc) DO UPDATE SET
        value       = EXCLUDED.value,
        macd        = EXCLUDED.macd,
        macd_signal = EXCLUDED.macd_signal,
        macd_hist   = EXCLUDED.macd_hist,
        raw_payload = EXCLUDED.raw_payload
    """
    with get_connection() as conn:
        src_id = get_source_id(conn, "alphavantage")
        with conn.cursor() as cur:
            for r in rows:
                sym_id  = get_symbol_id(conn, r["symbol"])
                ind_id  = get_indicator_id(conn, r["indicator"])
                int_id  = get_interval_id(conn, r["interval"])
                cur.execute(sql, (
                    sym_id, src_id, ind_id, int_id, r["ts"],
                    r["value"], r["macd"], r["macd_signal"], r["macd_hist"],
                    Json(r["raw"]),
                ))
        conn.commit()


# ─── ENTRY POINT ──────────────────────────────────────────────
def run(symbol: str, interval: str = "daily", max_records: int = 10):
    INDICATORS = [
        ("RSI",  _fetch_rsi,  _norm_rsi,  {"interval": interval, "period": 14}),
        ("MACD", _fetch_macd, _norm_macd, {"interval": interval}),
        ("EMA",  _fetch_ema,  _norm_ema,  {"interval": interval, "period": 20}),
        ("SMA",  _fetch_sma,  _norm_sma,  {"interval": interval, "period": 50}),
    ]
    for name, fetcher, normalizer, kwargs in INDICATORS:
        try:
            raw  = fetcher(symbol, **kwargs)
            rows = normalizer(symbol, raw, interval, max_records)
            _save(rows)
            print(f"  [AlphaVantage] {name} → {symbol} ({len(rows)} rows)")
        except Exception as e:
            print(f"  [AlphaVantage] ERROR {name} {symbol}: {e}")
        time.sleep(15)   # 25 req/day free → محافظه‌کارانه صبر می‌کنیم
