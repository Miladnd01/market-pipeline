import os
import sys
import time
import argparse
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import collectors.finnhub      as fh
import collectors.alphavantage as av
import collectors.twelvedata   as td
from db.connection import create_schema, get_connection

# ─── تنظیمات از .env ──────────────────────────────────────────
DEFAULT_SYMBOLS = [
    s.strip()
    for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",")
    if s.strip()
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
ALPHA_MAX     = int(os.getenv("ALPHA_MAX_RECORDS", "10"))
TWELVE_SIZE   = int(os.getenv("TWELVE_OUTPUTSIZE", "30"))


# ─── FIX NULL: آپدیت dim_symbol بعد از گرفتن profile ─────────
def fix_null_symbol_info():
    """
    اگر company_name یا exchange در dim_symbol هنوز NULL هستند،
    از fact_company_fundamental که قبلاً ذخیره شده آپدیت می‌کند.
    """
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
            (f.raw_profile->>'name')     AS company_name,
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
    except Exception as e:
        print(f"  [FIX ERROR] {e}")


# ─── یک دور کامل ──────────────────────────────────────────────
def run_cycle(symbols: list[str], cycle_num: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'═'*52}")
    print(f"  Cycle #{cycle_num}  |  {now}  |  {len(symbols)} symbol(s)")
    print(f"{'═'*52}")

    # اندیکاتورها را فقط در دور اول و بعد هر 10 دور بگیر
    # (Alpha Vantage روزانه 25 call محدودیت دارد)
    fetch_indicators = (cycle_num == 1) or (cycle_num % 10 == 0)

    for i, symbol in enumerate(symbols, 1):
        print(f"\n  [{i}/{len(symbols)}] {symbol}")

        # ── Finnhub: quote (هر دور) + fundamentals (دور اول و هر 10 دور) ──
        try:
            fh.run(
                symbol,
                fetch_fundamentals=fetch_indicators,
                fetch_earn=(cycle_num == 1),  # earnings فقط دور اول
            )
        except Exception as e:
            print(f"    [Finnhub ERROR] {e}")

        time.sleep(1)

        # ── Alpha Vantage: RSI, EMA, SMA (محدود به هر 10 دور) ──
        if fetch_indicators:
            try:
                av.run(symbol, interval="daily", max_records=ALPHA_MAX)
            except Exception as e:
                print(f"    [AlphaVantage ERROR] {e}")
            time.sleep(2)

        # ── Twelve Data: کندل‌ها (هر دور) ──
        try:
            td.run(symbol, outputsize=TWELVE_SIZE)
        except Exception as e:
            print(f"    [TwelveData ERROR] {e}")

        time.sleep(2)

    # ── درست کردن NULL های dim_symbol ──
    fix_null_symbol_info()

    print(f"\n  ✓ Cycle #{cycle_num} complete.")
    print(f"  ⏱  Next run in {POLL_INTERVAL} seconds  (Ctrl+C to stop)")


# ─── MAIN ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Market Data Pipeline")
    parser.add_argument(
        "--symbols", type=str, default=None,
        help="کاما جدا، مثلاً AAPL,MSFT,TSLA"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="فقط یک بار اجرا شود و بسته شود"
    )
    args = parser.parse_args()

    symbols = (
        [s.strip() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else DEFAULT_SYMBOLS
    )

    # ── ساختار دیتابیس ──
    create_schema()

    if args.once:
        # ── حالت یک‌بار ──
        run_cycle(symbols, cycle_num=1)
        return

    # ── حالت پیش‌فرض: loop خودکار ──
    print(f"\n[AUTO-LOOP] هر {POLL_INTERVAL} ثانیه اجرا می‌شود — Ctrl+C برای توقف")
    print(f"[SYMBOLS]   {', '.join(symbols)}")

    cycle_num = 1
    while True:
        try:
            run_cycle(symbols, cycle_num)
        except KeyboardInterrupt:
            print("\n\n[✓] Pipeline stopped by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\n[Cycle ERROR] {e}")
            traceback.print_exc()

        cycle_num += 1

        # countdown تا دور بعدی
        try:
            for remaining in range(POLL_INTERVAL, 0, -1):
                print(f"\r  ⏳ Next cycle in {remaining:3d}s ...", end="", flush=True)
                time.sleep(1)
            print()
        except KeyboardInterrupt:
            print("\n\n[✓] Pipeline stopped by user.")
            sys.exit(0)


if __name__ == "__main__":
    main()
