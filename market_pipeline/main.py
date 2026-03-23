# main.py
import os
import sys
import time
import argparse
import traceback
import logging
from datetime import datetime
from typing import Callable, Optional, Any

import pytz
from dotenv import load_dotenv

# .env laden
load_dotenv()

import collectors.finnhub as fh
import collectors.alphavantage as av
import collectors.twelvedata as td
from db.connection import create_schema, get_connection

# ---------------------------------------------------
# Logging
# ---------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Konfiguration
# ---------------------------------------------------
BERLIN_TZ = pytz.timezone("Europe/Berlin")

DEFAULT_SYMBOLS = [
    s.strip()
    for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",")
    if s.strip()
]

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))
ALPHA_MAX = int(os.getenv("ALPHA_MAX_RECORDS", "10"))
TWELVE_SIZE = int(os.getenv("TWELVE_OUTPUTSIZE", "30"))

# ---------------------------------------------------
# Status-Callback Typ
# ---------------------------------------------------
StatusCallback = Optional[Callable[..., None]]


# ---------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------
def now_berlin() -> datetime:
    return datetime.now(BERLIN_TZ)


def now_berlin_iso() -> str:
    return now_berlin().isoformat()


def report_status(callback: StatusCallback, **kwargs: Any) -> None:
    """
    Ruft optionalen Status-Callback robust auf.
    Falls der Callback selbst crasht, soll die Pipeline weiterlaufen.
    """
    if callback is None:
        return

    try:
        callback(**kwargs)
    except Exception as cb_error:
        logger.warning("Status callback failed: %s", cb_error)


def print_live_dashboard() -> None:
    """
    Live Terminal Dashboard - zeigt aktuelle Daten aus der Datenbank.
    Wird nach jedem Cycle automatisch aufgerufen.
    """
    queries = {
        "📈 LATEST QUOTES": """
            SELECT 
                symbol_code AS Symbol,
                ROUND(price::numeric, 2) AS Price,
                ROUND(change_pct::numeric, 2) AS "Change%",
                TO_CHAR(q.fetched_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS') AS "Time (Berlin)"
            FROM fact_market_quote q
            JOIN dim_symbol s ON s.symbol_id = q.symbol_id
            ORDER BY q.fetched_at_utc DESC
            LIMIT 5;
        """,
        "📊 CANDLE DATA (Last 3)": """
            SELECT 
                symbol_code AS Symbol,
                ROUND(close::numeric, 2) AS Close,
                ROUND(volume::numeric, 0) AS Volume,
                TO_CHAR(t.candle_time_utc AT TIME ZONE 'Europe/Berlin', 'DD.MM HH24:MI') AS "Time"
            FROM fact_market_timeseries t
            JOIN dim_symbol s ON s.symbol_id = t.symbol_id
            ORDER BY t.candle_time_utc DESC
            LIMIT 3;
        """,
        "🔧 API CALLS (Last 5)": """
            SELECT 
                src.source_name AS Source,
                endpoint AS Endpoint,
                http_status AS Status,
                response_ms AS "MS",
                TO_CHAR(called_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS') AS "Time"
            FROM log_api_call l
            LEFT JOIN dim_source src ON src.source_id = l.source_id
            ORDER BY l.called_at_utc DESC
            LIMIT 5;
        """,
        "💰 FUNDAMENTALS": """
            SELECT 
                s.symbol_code AS Symbol,
                ROUND((f.market_cap / 1000)::numeric, 1) || 'B' AS "Market Cap",
                ROUND(f.pe_ratio::numeric, 2) AS "P/E"
            FROM fact_company_fundamental f
            JOIN dim_symbol s ON s.symbol_id = f.symbol_id
            ORDER BY f.fetched_at_utc DESC
            LIMIT 3;
        """
    }

    os.system("clear" if os.name != "nt" else "cls")

    print("\n" + "=" * 80)
    print("   📊 MARKET DATA PIPELINE - LIVE DASHBOARD")
    print(f"   🕐 {now_berlin().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 80)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for title, sql in queries.items():
                    print(f"\n{title}")
                    print("-" * 80)

                    cur.execute(sql)
                    rows = cur.fetchall()

                    if not rows:
                        print("  (No data yet)")
                        continue

                    colnames = [desc[0] for desc in cur.description]

                    col_widths = []
                    for i, name in enumerate(colnames):
                        max_width = len(str(name))
                        for row in rows:
                            max_width = max(max_width, len(str(row[i])) if row[i] is not None else 0)
                        col_widths.append(min(max_width + 2, 20))

                    header = " | ".join(f"{name:<{col_widths[i]}}" for i, name in enumerate(colnames))
                    print(f"  {header}")
                    print("  " + "-" * len(header))

                    for row in rows:
                        line = " | ".join(
                            f"{str(val) if val is not None else 'NULL':<{col_widths[i]}}"
                            for i, val in enumerate(row)
                        )
                        print(f"  {line}")

    except Exception as e:
        print(f"\n  ⚠️ [DASHBOARD ERROR] {e}")
        traceback.print_exc()

    print("\n" + "=" * 80 + "\n")


def fix_null_symbol_info() -> None:
    """Füllt fehlende Stammdaten in dim_symbol aus den Fundamentals auf."""
    sql = """
    UPDATE dim_symbol ds
    SET
        company_name = COALESCE(ds.company_name, sub.company_name),
        exchange     = COALESCE(ds.exchange,     sub.exchange),
        country      = COALESCE(ds.country,      sub.country),
        currency     = COALESCE(ds.currency,     sub.currency)
    FROM (
        SELECT DISTINCT ON (f.symbol_id)
            f.symbol_id,
            (f.raw_profile->>'name')      AS company_name,
            (f.raw_profile->>'exchange')  AS exchange,
            (f.raw_profile->>'country')   AS country,
            (f.raw_profile->>'currency')  AS currency
        FROM fact_company_fundamental f
        ORDER BY f.symbol_id, f.fetched_at_utc DESC
    ) sub
    WHERE ds.symbol_id = sub.symbol_id
      AND (
          ds.company_name IS NULL OR
          ds.exchange     IS NULL OR
          ds.country      IS NULL OR
          ds.currency     IS NULL
      );
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                updated = cur.rowcount
            conn.commit()

        if updated > 0:
            print(f"  [FIX] {updated} symbol(s) updated with company info.")
            logger.info("[FIX] %s symbol(s) updated with company info.", updated)

    except Exception as e:
        print(f"  [FIX ERROR] {e}")
        logger.exception("fix_null_symbol_info failed")


def run_cycle(
    symbols: list[str],
    cycle_num: int,
    status_callback: StatusCallback = None
) -> dict[str, Any]:
    """
    Führt einen Sammlungs-Durchlauf für alle Symbole aus
    und liefert strukturierte Statusinformationen zurück.
    """
    cycle_started_at_dt = now_berlin()
    cycle_started_at = cycle_started_at_dt.isoformat()
    cycle_start_perf = time.perf_counter()

    cycle_result: dict[str, Any] = {
        "cycle_num": cycle_num,
        "cycle_started_at": cycle_started_at,
        "cycle_finished_at": None,
        "cycle_duration_seconds": None,
        "symbols_total": len(symbols),
        "symbols_ok": 0,
        "symbols_failed": 0,
        "symbol_results": [],
        "fetch_indicators": (cycle_num == 1) or (cycle_num % 10 == 0),
        "success": False,
        "error": None
    }

    report_status(
        status_callback,
        event="cycle_started",
        running=True,
        phase="cycle_started",
        cycle_num=cycle_num,
        cycle_started_at=cycle_started_at,
        last_cycle_at=cycle_started_at,
        symbols_total=len(symbols),
        fetch_indicators=cycle_result["fetch_indicators"],
        error=None
    )

    print(f"\n{'═' * 80}")
    print(f"  🔄 Cycle #{cycle_num}  |  {cycle_started_at_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}  |  {len(symbols)} symbol(s)")
    print(f"{'═' * 80}")

    fetch_indicators = cycle_result["fetch_indicators"]

    for i, symbol in enumerate(symbols, 1):
        print(f"\n  [{i}/{len(symbols)}] Processing: {symbol}")

        symbol_started_at = now_berlin_iso()
        symbol_start_perf = time.perf_counter()

        symbol_result = {
            "symbol": symbol,
            "started_at": symbol_started_at,
            "finished_at": None,
            "duration_seconds": None,
            "success": True,
            "errors": [],
            "steps": {
                "finnhub": {"success": False, "error": None},
                "alphavantage": {"success": None, "error": None},
                "twelvedata": {"success": False, "error": None}
            }
        }

        report_status(
            status_callback,
            event="symbol_started",
            running=True,
            phase="symbol_started",
            cycle_num=cycle_num,
            current_symbol=symbol,
            current_symbol_index=i,
            symbols_total=len(symbols),
            last_cycle_at=now_berlin_iso(),
            error=None
        )

        # 1) Finnhub
        try:
            fh.run(symbol, fetch_fundamentals=fetch_indicators, fetch_earn=(cycle_num == 1))
            symbol_result["steps"]["finnhub"]["success"] = True
        except Exception as e:
            msg = f"[Finnhub ERROR] {e}"
            print(f"    ❌ {msg}")
            logger.exception("Finnhub failed for symbol %s", symbol)
            symbol_result["steps"]["finnhub"]["error"] = str(e)
            symbol_result["errors"].append(msg)
            symbol_result["success"] = False

        time.sleep(1)

        # 2) Alpha Vantage
        if fetch_indicators:
            try:
                av.run(symbol, interval="daily", max_records=ALPHA_MAX)
                symbol_result["steps"]["alphavantage"]["success"] = True
            except Exception as e:
                msg = f"[AlphaVantage ERROR] {e}"
                print(f"    ❌ {msg}")
                logger.exception("AlphaVantage failed for symbol %s", symbol)
                symbol_result["steps"]["alphavantage"]["error"] = str(e)
                symbol_result["errors"].append(msg)
                symbol_result["success"] = False

            time.sleep(2)

        # 3) Twelve Data
        try:
            td.run(symbol, outputsize=TWELVE_SIZE)
            symbol_result["steps"]["twelvedata"]["success"] = True
        except Exception as e:
            msg = f"[TwelveData ERROR] {e}"
            print(f"    ❌ {msg}")
            logger.exception("TwelveData failed for symbol %s", symbol)
            symbol_result["steps"]["twelvedata"]["error"] = str(e)
            symbol_result["errors"].append(msg)
            symbol_result["success"] = False

        time.sleep(2)

        symbol_finished_at = now_berlin_iso()
        symbol_result["finished_at"] = symbol_finished_at
        symbol_result["duration_seconds"] = round(time.perf_counter() - symbol_start_perf, 2)

        if symbol_result["success"]:
            cycle_result["symbols_ok"] += 1
        else:
            cycle_result["symbols_failed"] += 1

        cycle_result["symbol_results"].append(symbol_result)

        report_status(
            status_callback,
            event="symbol_finished",
            running=True,
            phase="symbol_finished",
            cycle_num=cycle_num,
            current_symbol=symbol,
            current_symbol_index=i,
            symbols_total=len(symbols),
            symbols_ok=cycle_result["symbols_ok"],
            symbols_failed=cycle_result["symbols_failed"],
            last_cycle_at=symbol_finished_at,
            last_symbol_result=symbol_result,
            error=None if symbol_result["success"] else "; ".join(symbol_result["errors"])
        )

    # Datenkonsistenz prüfen
    try:
        fix_null_symbol_info()
    except Exception as e:
        logger.exception("fix_null_symbol_info wrapper failed")
        cycle_result["error"] = f"fix_null_symbol_info failed: {e}"

    # Live Dashboard anzeigen
    try:
        print_live_dashboard()
    except Exception as e:
        logger.exception("print_live_dashboard wrapper failed")
        if cycle_result["error"] is None:
            cycle_result["error"] = f"print_live_dashboard failed: {e}"

    cycle_finished_at = now_berlin_iso()
    cycle_result["cycle_finished_at"] = cycle_finished_at
    cycle_result["cycle_duration_seconds"] = round(time.perf_counter() - cycle_start_perf, 2)
    cycle_result["success"] = cycle_result["symbols_failed"] == 0 and cycle_result["error"] is None

    print(f"\n  ✅ Cycle #{cycle_num} complete.\n")

    report_status(
        status_callback,
        event="cycle_finished",
        running=True,
        phase="cycle_finished",
        cycle_num=cycle_num,
        cycle_started_at=cycle_result["cycle_started_at"],
        last_cycle_at=cycle_finished_at,
        last_successful_cycle_at=cycle_finished_at if cycle_result["success"] else None,
        cycle_duration_seconds=cycle_result["cycle_duration_seconds"],
        symbols_total=cycle_result["symbols_total"],
        symbols_ok=cycle_result["symbols_ok"],
        symbols_failed=cycle_result["symbols_failed"],
        last_cycle_result=cycle_result,
        error=cycle_result["error"]
    )

    return cycle_result


def main(
    override_symbols: Optional[list[str]] = None,
    once: bool = False,
    status_callback: StatusCallback = None
) -> None:
    """
    Hauptfunktion der Pipeline.
    Unterstützt einen optionalen status_callback, damit die Flask-App
    nach jedem Cycle Statusinformationen aktualisieren kann.
    """
    symbols = override_symbols if override_symbols else DEFAULT_SYMBOLS

    report_status(
        status_callback,
        event="pipeline_initializing",
        running=False,
        phase="initializing",
        started_at=now_berlin_iso(),
        symbols=symbols,
        poll_interval_seconds=POLL_INTERVAL,
        error=None
    )

    # Datenbank-Schema sicherstellen
    try:
        create_schema()
    except Exception as e:
        logger.exception("create_schema failed")
        report_status(
            status_callback,
            event="pipeline_error",
            running=False,
            phase="initialization_failed",
            last_error=str(e),
            error=str(e),
            thread_alive=False
        )
        raise

    if once:
        report_status(
            status_callback,
            event="pipeline_started",
            running=True,
            phase="single_run",
            started_at=now_berlin_iso(),
            symbols=symbols,
            poll_interval_seconds=POLL_INTERVAL,
            error=None
        )

        try:
            result = run_cycle(symbols, cycle_num=1, status_callback=status_callback)

            report_status(
                status_callback,
                event="pipeline_finished",
                running=False,
                phase="finished",
                last_cycle_at=result["cycle_finished_at"],
                last_successful_cycle_at=result["cycle_finished_at"] if result["success"] else None,
                last_cycle_result=result,
                error=result["error"]
            )
            return

        except Exception as e:
            logger.exception("Pipeline single run crashed")
            report_status(
                status_callback,
                event="pipeline_error",
                running=False,
                phase="crashed",
                last_error=str(e),
                error=str(e),
                thread_alive=False
            )
            raise

    print(f"\n🚀 [AUTO-LOOP] Interval: {POLL_INTERVAL}s | Symbols: {', '.join(symbols)}")

    report_status(
        status_callback,
        event="pipeline_started",
        running=True,
        phase="loop_waiting_for_first_cycle",
        started_at=now_berlin_iso(),
        symbols=symbols,
        poll_interval_seconds=POLL_INTERVAL,
        error=None
    )

    cycle_num = 1

    while True:
        try:
            result = run_cycle(symbols, cycle_num, status_callback=status_callback)

        except KeyboardInterrupt:
            print("\n\n✅ [STOPPED] Pipeline stopped by user.")
            report_status(
                status_callback,
                event="pipeline_stopped",
                running=False,
                phase="stopped",
                stopped_at=now_berlin_iso(),
                error=None,
                thread_alive=False
            )
            sys.exit(0)

        except Exception as e:
            logger.exception("Critical error in pipeline loop")
            print(f"\n❌ [CRITICAL ERROR] {e}")
            traceback.print_exc()

            report_status(
                status_callback,
                event="pipeline_error",
                running=True,
                phase="cycle_crashed",
                cycle_num=cycle_num,
                last_cycle_at=now_berlin_iso(),
                last_error=str(e),
                error=str(e)
            )

            result = None

        cycle_num += 1

        try:
            print(f"  ⏱️  Next cycle in {POLL_INTERVAL} seconds...")

            report_status(
                status_callback,
                event="sleeping",
                running=True,
                phase="sleeping",
                next_cycle_num=cycle_num,
                sleep_seconds=POLL_INTERVAL,
                sleep_started_at=now_berlin_iso(),
                last_cycle_at=(result["cycle_finished_at"] if result else now_berlin_iso()),
                error=None
            )

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n✅ [STOPPED] Pipeline stopped.")

            report_status(
                status_callback,
                event="pipeline_stopped",
                running=False,
                phase="stopped",
                stopped_at=now_berlin_iso(),
                error=None,
                thread_alive=False
            )
            sys.exit(0)


# ---------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Data Pipeline CLI")
    parser.add_argument("--symbols", type=str, default=None, help="Comma separated symbols")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")

    args = parser.parse_args()
    input_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None

    main(override_symbols=input_symbols, once=args.once)
