# main.py
import os
import sys
import time
import argparse
import traceback
from datetime import datetime
import pytz
from dotenv import load_dotenv

# .env laden
load_dotenv()

import collectors.finnhub      as fh
import collectors.alphavantage as av
import collectors.twelvedata   as td
from db.connection import create_schema, get_connection

# --- Konfiguration ---
# Definition der Zeitzone (Muss vor der Verwendung definiert sein!)
BERLIN_TZ = pytz.timezone('Europe/Berlin')

DEFAULT_SYMBOLS = [
    s.strip()
    for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",")
    if s.strip()
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
ALPHA_MAX     = int(os.getenv("ALPHA_MAX_RECORDS", "10"))
TWELVE_SIZE   = int(os.getenv("TWELVE_OUTPUTSIZE", "30"))

def fix_null_symbol_info():
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
                # WICHTIG: Setzt die Datenbank-Session auf Berliner Zeit
                cur.execute("SET TIME ZONE 'Europe/Berlin';")
                cur.execute(sql)
                updated = cur.rowcount
            conn.commit()
        if updated > 0:
            print(f"  [FIX] {updated} symbol(s) updated with company info.")
    except Exception as e:
        print(f"  [FIX ERROR] {e}")

def run_cycle(symbols: list[str], cycle_num: int):
    """Führt einen Sammlungs-Durchlauf für alle Symbole aus."""
    # Hier wird die aktuelle Zeit für das Log berechnet
    now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    
    print(f"\n{'═'*52}")
    print(f"  Cycle #{cycle_num}  |  {now_str}  |  {len(symbols)} symbol(s)")
    print(f"{'═'*52}")

    # Indikatoren nur beim ersten Mal oder alle 10 Zyklen
    fetch_indicators = (cycle_num == 1) or (cycle_num % 10 == 0)

    for i, symbol in enumerate(symbols, 1):
        print(f"\n  [{i}/{len(symbols)}] Processing: {symbol}")
        
        # 1. Finnhub
        try:
            fh.run(symbol, fetch_fundamentals=fetch_indicators, fetch_earn=(cycle_num == 1))
        except Exception as e:
            print(f"    [Finnhub ERROR] {e}")
        time.sleep(1)

        # 2. Alpha Vantage
        if fetch_indicators:
            try:
                av.run(symbol, interval="daily", max_records=ALPHA_MAX)
            except Exception as e:
                print(f"    [AlphaVantage ERROR] {e}")
            time.sleep(2)

        # 3. Twelve Data
        try:
            td.run(symbol, outputsize=TWELVE_SIZE)
        except Exception as e:
            print(f"    [TwelveData ERROR] {e}")
        time.sleep(2)

    # Daten-Konsistenz prüfen
    fix_null_symbol_info()
    print(f"\n  ✓ Cycle #{cycle_num} complete.")

def main(override_symbols=None, once=False):
    """Hauptfunktion der Pipeline."""
    symbols = override_symbols if override_symbols else DEFAULT_SYMBOLS
    
    # Datenbank-Schema sicherstellen
    create_schema()

    if once:
        run_cycle(symbols, cycle_num=1)
        return

    print(f"\n[AUTO-LOOP] Interval: {POLL_INTERVAL}s | Symbols: {', '.join(symbols)}")

    cycle_num = 1
    while True:
        try:
            run_cycle(symbols, cycle_num)
        except KeyboardInterrupt:
            print("\n\n[✓] Pipeline stopped by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\n[CRITICAL ERROR] {e}")
            traceback.print_exc()

        cycle_num += 1
        
        # Countdown bis zum nächsten Start
        try:
            print(f"  ⏱ Next cycle in {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[✓] Pipeline stopped.")
            sys.exit(0)

# --- CLI Entrypoint ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Data Pipeline CLI")
    parser.add_argument("--symbols", type=str, default=None, help="Comma separated symbols")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    
    args = parser.parse_args()

    input_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    main(override_symbols=input_symbols, once=args.once)
