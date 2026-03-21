# Market Data Pipeline — Free APIs → PostgreSQL

## منابع داده
| API | نقش | محدودیت رایگان |
|-----|-----|----------------|
| Finnhub | Quote زنده + Company Profile + Earnings | 60 req/min |
| Alpha Vantage | RSI + MACD + EMA + SMA + Fundamentals | 25 req/day |
| Twelve Data | OHLCV time series | 800 req/day |

## جداول PostgreSQL
- `market_quotes` — قیمت لحظه‌ای
- `market_indicators` — شاخص‌های تکنیکال
- `market_timeseries` — کندل‌استیک
- `company_fundamentals` — داده‌های بنیادی
- `earnings_calendar` — تقویم سود

## نصب
```bash
pip install -r requirements.txt
cp .env.example .env
# مقادیر .env را پر کن
python main.py
```
