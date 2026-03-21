# راهنمای کامل راه‌اندازی — Market Data Pipeline (Star Schema)

## ۱. دریافت API Key رایگان

| سرویس | لینک ثبت‌نام | محدودیت رایگان |
|-------|-------------|----------------|
| Finnhub | https://finnhub.io/register | 60 req/min |
| Alpha Vantage | https://www.alphavantage.co/support/#api-key | 25 req/day |
| Twelve Data | https://twelvedata.com/register | 800 req/day |

---

## ۲. ایجاد دیتابیس PostgreSQL

```sql
CREATE DATABASE marketdb;
```

---

## ۳. پیکربندی .env

```bash
cp .env.example .env
```

مقادیر واقعی را در `.env` جایگزین کن:

```
FINNHUB_API_KEY=d1...
ALPHAVANTAGE_API_KEY=ABC...
TWELVEDATA_API_KEY=abc123...

PGHOST=localhost
PGPORT=5432
PGDATABASE=marketdb
PGUSER=postgres
PGPASSWORD=my_password

SYMBOLS=AAPL,MSFT
POLL_INTERVAL_SECONDS=300
ALPHA_MAX_RECORDS=10
TWELVE_OUTPUTSIZE=30
```

---

## ۴. نصب و اجرا

```bash
pip install -r requirements.txt

# یک دور کامل
python main.py

# بدون داده بنیادی (کمتر API call)
python main.py --no-fundamentals

# سمبل‌های دلخواه
python main.py --symbols AAPL,TSLA,GOOGL

# حالت loop هر ۵ دقیقه
python main.py --loop
```

---

## ۵. ایجاد Views برای Power BI (یک بار)

```bash
psql -d marketdb -f queries_for_powerbi.sql
```

---

## ۶. ساختار Star Schema

```
dim_source       ──┐
dim_symbol       ──┼──► fact_market_quote
dim_indicator    ──┤
dim_interval     ──┘──► fact_market_indicator
                   └──► fact_market_timeseries
                   └──► fact_company_fundamental
                   └──► fact_earnings_calendar
                        log_api_call
```

### Relationships در Power BI

| از | به | نوع |
|----|-----|-----|
| dim_symbol.symbol_id | fact_market_quote.symbol_id | 1:Many |
| dim_symbol.symbol_id | fact_market_indicator.symbol_id | 1:Many |
| dim_symbol.symbol_id | fact_market_timeseries.symbol_id | 1:Many |
| dim_symbol.symbol_id | fact_company_fundamental.symbol_id | 1:Many |
| dim_symbol.symbol_id | fact_earnings_calendar.symbol_id | 1:Many |
| dim_source.source_id | همه fact‌ها | 1:Many |
| dim_indicator.indicator_id | fact_market_indicator.indicator_id | 1:Many |
| dim_interval.interval_id | fact_market_indicator.interval_id | 1:Many |
| dim_interval.interval_id | fact_market_timeseries.interval_id | 1:Many |

---

## ۷. Views آماده در Power BI

| View | کاربرد |
|------|--------|
| `vw_latest_quotes` | کارت قیمت لحظه‌ای |
| `vw_quote_history` | نمودار خطی تاریخچه |
| `vw_candles_daily` | نمودار شمعی روزانه |
| `vw_rsi_history` | نمودار RSI + سیگنال |
| `vw_macd_history` | نمودار MACD |
| `vw_ema_sma` | EMA vs SMA overlay |
| `vw_fundamentals` | مقایسه بنیادی شرکت‌ها |
| `vw_earnings_upcoming` | تقویم سود ۹۰ روز آینده |
| `vw_dashboard_main` | صفحه اصلی داشبورد |
| `vw_api_log` | مانیتورینگ API calls |

---

## ۸. محدودیت‌های مهم

با ۲ سمبل و همه اندیکاتورها:
- Finnhub: ~6 call → خوب
- Alpha Vantage: 8 call از 25 روزانه → محتاط باش
- Twelve Data: ~4 call → خوب

**توصیه:** با `--no-fundamentals` هر ۵ دقیقه اجرا کن.
برای fundamentals، یک بار در روز صبح اجرا کن.

---

## ساختار فایل‌ها

```
market_pipeline/
├── main.py                    ← نقطه ورود
├── requirements.txt
├── .env.example
├── queries_for_powerbi.sql    ← 10 View آماده
├── SETUP_GUIDE.md
├── collectors/
│   ├── finnhub.py             ← quote + fundamentals + earnings
│   ├── alphavantage.py        ← RSI, MACD, EMA, SMA
│   └── twelvedata.py          ← OHLCV candles
└── db/
    └── connection.py          ← Star Schema DDL + dimension helpers
```
