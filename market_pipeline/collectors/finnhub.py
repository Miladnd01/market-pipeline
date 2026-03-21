"""
collectors/finnhub.py
Finnhub → fact_market_quote + fact_company_fundamental + fact_earnings_calendar
"""

import os
import time
import requests
from datetime import datetime, timezone, date, timedelta
from psycopg2.extras import Json

from db.connection import (
    get_connection, get_source_id, get_symbol_id, get_interval_id
)

KEY  = os.getenv("FINNHUB_API_KEY", "")
BASE = "https://finnhub.io/api/v1"


def _get(endpoint: str, params: dict = None) -> dict:
    t0 = time.time()
    url = f"{BASE}{endpoint}"
    r   = requests.get(url, headers={"X-Finnhub-Token": KEY},
                       params=params or {}, timeout=20)
    ms  = int((time.time() - t0) * 1000)
    r.raise_for_status()
    return r.json(), ms


def _log(conn, source_id, symbol_id, endpoint, status, ms, err=None):
    sql = """INSERT INTO log_api_call
             (source_id, symbol_id, endpoint, http_status, response_ms, error_msg)
             VALUES (%s,%s,%s,%s,%s,%s)"""
    with conn.cursor() as cur:
        cur.execute(sql, (source_id, symbol_id, endpoint, status, ms, err))


# ─── QUOTE ────────────────────────────────────────────────────
def run_quote(symbol: str):
    try:
        data, ms = _get("/quote", {"symbol": symbol})
    except Exception as e:
        print(f"  [Finnhub] Quote ERROR {symbol}: {e}")
        return

    price = data.get("c")
    pc    = data.get("pc")
    chg   = round(price - pc, 6) if (price and pc) else None
    chgp  = round((price - pc) / pc * 100, 4) if (price and pc and pc != 0) else None
    ts_q  = (datetime.fromtimestamp(data["t"], tz=timezone.utc)
             if data.get("t") else None)

    with get_connection() as conn:
        src_id = get_source_id(conn, "finnhub")
        sym_id = get_symbol_id(conn, symbol)
        _log(conn, src_id, sym_id, "/quote", 200, ms)

        sql = """
        INSERT INTO fact_market_quote
            (symbol_id, source_id, fetched_at_utc, quote_time_utc,
             price, open, high, low, previous_close, change, change_pct, raw_payload)
        VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol_id, source_id, quote_time_utc) DO UPDATE SET
            price          = EXCLUDED.price,
            open           = EXCLUDED.open,
            high           = EXCLUDED.high,
            low            = EXCLUDED.low,
            previous_close = EXCLUDED.previous_close,
            change         = EXCLUDED.change,
            change_pct     = EXCLUDED.change_pct,
            raw_payload    = EXCLUDED.raw_payload,
            fetched_at_utc = NOW()
        """
        with conn.cursor() as cur:
            cur.execute(sql, (
                sym_id, src_id, ts_q,
                price, data.get("o"), data.get("h"), data.get("l"),
                pc, chg, chgp, Json(data)
            ))
        conn.commit()
    print(f"  [Finnhub] Quote → {symbol} @ {price}")


# ─── FUNDAMENTALS ─────────────────────────────────────────────
def run_fundamentals(symbol: str):
    try:
        profile, ms1 = _get("/stock/profile2", {"symbol": symbol})
        time.sleep(1)
        metrics_raw, ms2 = _get("/stock/metric", {"symbol": symbol, "metric": "all"})
    except Exception as e:
        print(f"  [Finnhub] Fundamentals ERROR {symbol}: {e}")
        return

    m = metrics_raw.get("metric", {})
    ipo = None
    if profile.get("ipo"):
        try:
            ipo = datetime.strptime(profile["ipo"], "%Y-%m-%d").date()
        except Exception:
            pass

    with get_connection() as conn:
        src_id = get_source_id(conn, "finnhub")
        sym_id = get_symbol_id(
            conn, symbol,
            company_name=profile.get("name"),
            exchange=profile.get("exchange"),
            country=profile.get("country"),
            currency=profile.get("currency"),
        )
        _log(conn, src_id, sym_id, "/stock/profile2", 200, ms1)
        _log(conn, src_id, sym_id, "/stock/metric",   200, ms2)

        sql = """
        INSERT INTO fact_company_fundamental
            (symbol_id, source_id, fetched_at_utc, ipo_date,
             market_cap, share_outstanding,
             pe_ratio, eps_ttm, gross_margin, net_margin,
             roe, debt_to_equity, current_ratio, beta,
             week_52_high, week_52_low, raw_profile, raw_metrics)
        VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        with conn.cursor() as cur:
            cur.execute(sql, (
                sym_id, src_id, ipo,
                profile.get("marketCapitalization"),
                profile.get("shareOutstanding"),
                m.get("peNormalizedAnnual"),
                m.get("epsNormalizedAnnual"),
                m.get("grossMarginAnnual"),
                m.get("netProfitMarginAnnual"),
                m.get("roeAnnual"),
                m.get("totalDebt/totalEquityAnnual"),
                m.get("currentRatioAnnual"),
                m.get("beta"),
                m.get("52WeekHigh"),
                m.get("52WeekLow"),
                Json(profile), Json(metrics_raw),
            ))
        conn.commit()
    print(f"  [Finnhub] Fundamentals → {symbol}")


# ─── EARNINGS ─────────────────────────────────────────────────
def run_earnings(symbol: str):
    today  = date.today()
    params = {
        "symbol": symbol,
        "from":   today.strftime("%Y-%m-%d"),
        "to":     (today + timedelta(days=90)).strftime("%Y-%m-%d"),
    }
    try:
        data, ms = _get("/calendar/earnings", params)
    except Exception as e:
        print(f"  [Finnhub] Earnings ERROR {symbol}: {e}")
        return

    items = data.get("earningsCalendar", [])
    if not items:
        print(f"  [Finnhub] No earnings data for {symbol}")
        return

    with get_connection() as conn:
        src_id = get_source_id(conn, "finnhub")
        sym_id = get_symbol_id(conn, symbol)
        _log(conn, src_id, sym_id, "/calendar/earnings", 200, ms)

        sql = """
        INSERT INTO fact_earnings_calendar
            (symbol_id, source_id, fetched_at_utc, report_date, hour,
             eps_estimate, eps_actual, revenue_estimate, revenue_actual, raw_payload)
        VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol_id, report_date) DO UPDATE SET
            eps_estimate     = EXCLUDED.eps_estimate,
            eps_actual       = EXCLUDED.eps_actual,
            revenue_estimate = EXCLUDED.revenue_estimate,
            revenue_actual   = EXCLUDED.revenue_actual,
            raw_payload      = EXCLUDED.raw_payload,
            fetched_at_utc   = NOW()
        """
        with conn.cursor() as cur:
            for e in items:
                rd = None
                if e.get("date"):
                    try:
                        rd = datetime.strptime(e["date"], "%Y-%m-%d").date()
                    except Exception:
                        pass
                cur.execute(sql, (
                    sym_id, src_id, rd,
                    e.get("hour"),
                    e.get("epsEstimate"), e.get("epsActual"),
                    e.get("revenueEstimate"), e.get("revenueActual"),
                    Json(e),
                ))
        conn.commit()
    print(f"  [Finnhub] Earnings → {symbol} ({len(items)} records)")


# ─── ENTRY POINT ──────────────────────────────────────────────
def run(symbol: str, fetch_fundamentals: bool = True, fetch_earn: bool = True):
    run_quote(symbol)
    time.sleep(1)
    if fetch_fundamentals:
        run_fundamentals(symbol)
        time.sleep(1)
    if fetch_earn:
        run_earnings(symbol)
        time.sleep(1)
