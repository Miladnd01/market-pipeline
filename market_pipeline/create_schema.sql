CREATE TABLE IF NOT EXISTS dim_source (
    source_id   SERIAL       PRIMARY KEY,
    source_name TEXT         NOT NULL UNIQUE,
    base_url    TEXT,
    notes       TEXT
);

-- نمادهای بورسی
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id    SERIAL  PRIMARY KEY,
    symbol_code  TEXT    NOT NULL UNIQUE,  -- مثال: AAPL
    company_name TEXT,
    exchange     TEXT,                     -- مثال: NASDAQ
    country      TEXT,
    currency     TEXT,
    sector       TEXT,
    industry     TEXT
);

-- بازه‌های زمانی
CREATE TABLE IF NOT EXISTS dim_interval (
    interval_id   SERIAL  PRIMARY KEY,
    interval_code TEXT    NOT NULL UNIQUE, -- مثال: 1min / 1day
    interval_type TEXT                     -- intraday / daily / weekly
);

-- نام اندیکاتورهای تکنیکال
CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_id   SERIAL  PRIMARY KEY,
    indicator_name TEXT    NOT NULL UNIQUE, -- مثال: RSI
    description    TEXT,
    category       TEXT                     -- momentum / trend / volatility
);


-- ============================================================
-- بخش ۲: FACT TABLES (جداول داده)
-- ============================================================

-- ── ۲.۱  قیمت لحظه‌ای  (از Finnhub /quote) ──────────────────
CREATE TABLE IF NOT EXISTS fact_market_quote (
    quote_id        BIGSERIAL    PRIMARY KEY,
    symbol_id       INT          NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT          NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- زمان دریافت داده
    quote_time_utc  TIMESTAMPTZ,                           -- زمان اعلام قیمت (از API)
    price           NUMERIC(18,6),                         -- c  = current price
    open            NUMERIC(18,6),                         -- o  = open
    high            NUMERIC(18,6),                         -- h  = day high
    low             NUMERIC(18,6),                         -- l  = day low
    previous_close  NUMERIC(18,6),                         -- pc = previous close
    change          NUMERIC(18,6),                         -- price - previous_close
    change_pct      NUMERIC(10,4),                         -- درصد تغییر
    raw_payload     JSONB,                                  -- پاسخ خام API
    CONSTRAINT uq_fact_quote UNIQUE (symbol_id, source_id, quote_time_utc)
);

CREATE INDEX IF NOT EXISTS ix_quote_symbol_time
    ON fact_market_quote (symbol_id, fetched_at_utc DESC);


-- ── ۲.۲  شاخص‌های تکنیکال  (از Alpha Vantage) ────────────────
--         RSI, MACD, EMA, SMA همه در این جدول ذخیره می‌شوند
CREATE TABLE IF NOT EXISTS fact_market_indicator (
    indicator_fact_id BIGSERIAL    PRIMARY KEY,
    symbol_id         INT          NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id         INT          NOT NULL REFERENCES dim_source(source_id),
    indicator_id      INT          NOT NULL REFERENCES dim_indicator(indicator_id),
    interval_id       INT          REFERENCES dim_interval(interval_id),
    candle_time_utc   TIMESTAMPTZ  NOT NULL,  -- زمان کندل مربوطه
    value             NUMERIC(18,6),          -- مقدار اصلی (RSI / EMA / SMA / MACD line)
    macd              NUMERIC(18,6),          -- فقط برای MACD
    macd_signal       NUMERIC(18,6),          -- فقط برای MACD
    macd_hist         NUMERIC(18,6),          -- فقط برای MACD
    raw_payload       JSONB,
    CONSTRAINT uq_fact_indicator
        UNIQUE (symbol_id, indicator_id, interval_id, candle_time_utc)
);

CREATE INDEX IF NOT EXISTS ix_indicator_symbol_time
    ON fact_market_indicator (symbol_id, indicator_id, candle_time_utc DESC);


-- ── ۲.۳  کندل‌استیک OHLCV  (از Twelve Data) ──────────────────
CREATE TABLE IF NOT EXISTS fact_market_timeseries (
    timeseries_id   BIGSERIAL    PRIMARY KEY,
    symbol_id       INT          NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT          NOT NULL REFERENCES dim_source(source_id),
    interval_id     INT          NOT NULL REFERENCES dim_interval(interval_id),
    candle_time_utc TIMESTAMPTZ  NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6),
    volume          NUMERIC(20,2),
    raw_payload     JSONB,
    CONSTRAINT uq_fact_timeseries
        UNIQUE (symbol_id, interval_id, candle_time_utc)
);

CREATE INDEX IF NOT EXISTS ix_timeseries_symbol_time
    ON fact_market_timeseries (symbol_id, interval_id, candle_time_utc DESC);


-- ── ۲.۴  داده‌های بنیادی شرکت  (از Finnhub /profile2 + /metric) ──
CREATE TABLE IF NOT EXISTS fact_company_fundamental (
    fundamental_id    BIGSERIAL    PRIMARY KEY,
    symbol_id         INT          NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id         INT          NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ipo_date          DATE,
    market_cap        NUMERIC(22,2),           -- میلیون دلار (Finnhub)
    share_outstanding NUMERIC(18,2),           -- میلیون سهم
    pe_ratio          NUMERIC(12,4),           -- peNormalizedAnnual
    eps_ttm           NUMERIC(12,4),           -- epsNormalizedAnnual
    gross_margin      NUMERIC(10,4),           -- grossMarginAnnual (%)
    net_margin        NUMERIC(10,4),           -- netProfitMarginAnnual (%)
    roe               NUMERIC(10,4),           -- roeAnnual (%)
    debt_to_equity    NUMERIC(10,4),           -- totalDebt/totalEquityAnnual
    current_ratio     NUMERIC(10,4),           -- currentRatioAnnual
    beta              NUMERIC(10,4),
    week_52_high      NUMERIC(18,6),
    week_52_low       NUMERIC(18,6),
    raw_profile       JSONB,                   -- پاسخ خام /stock/profile2
    raw_metrics       JSONB                    -- پاسخ خام /stock/metric
);

CREATE INDEX IF NOT EXISTS ix_fundamental_symbol
    ON fact_company_fundamental (symbol_id, fetched_at_utc DESC);


-- ── ۲.۵  تقویم سود  (از Finnhub /calendar/earnings) ──────────
CREATE TABLE IF NOT EXISTS fact_earnings_calendar (
    earnings_id      BIGSERIAL    PRIMARY KEY,
    symbol_id        INT          NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id        INT          NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    report_date      DATE,
    hour             TEXT,                     -- BMO / AMC / unknown
    eps_estimate     NUMERIC(12,4),
    eps_actual       NUMERIC(12,4),
    revenue_estimate NUMERIC(22,2),
    revenue_actual   NUMERIC(22,2),
    raw_payload      JSONB,
    CONSTRAINT uq_fact_earnings UNIQUE (symbol_id, report_date)
);


-- ── ۲.۶  لاگ API calls  (برای debugging / monitoring) ─────────
CREATE TABLE IF NOT EXISTS log_api_call (
    log_id        BIGSERIAL    PRIMARY KEY,
    source_id     INT          REFERENCES dim_source(source_id),
    symbol_id     INT          REFERENCES dim_symbol(symbol_id),
    called_at_utc TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    endpoint      TEXT,                        -- مثال: /quote
    http_status   INT,                         -- 200 / 429 / 500
    response_ms   INT,                         -- زمان پاسخ به میلی‌ثانیه
    error_msg     TEXT                         -- خالی اگر موفق
);

CREATE INDEX IF NOT EXISTS ix_log_called_at
    ON log_api_call (called_at_utc DESC);


-- ============================================================
-- بخش ۳: SEED DATA (مقادیر ثابت اولیه)
-- ============================================================

INSERT INTO dim_source (source_name, base_url, notes) VALUES
    ('finnhub',
     'https://finnhub.io/api/v1',
     'Quote / Profile / Earnings — 60 req/min free'),
    ('alphavantage',
     'https://www.alphavantage.co/query',
     'Technical indicators — 25 req/day free'),
    ('twelvedata',
     'https://api.twelvedata.com',
     'OHLCV time series — 800 req/day free')
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO dim_indicator (indicator_name, description, category) VALUES
    ('RSI',  'Relative Strength Index — period 14',       'momentum'),
    ('MACD', 'Moving Average Convergence Divergence 12/26/9', 'trend'),
    ('EMA',  'Exponential Moving Average — period 20',    'trend'),
    ('SMA',  'Simple Moving Average — period 50',         'trend')
ON CONFLICT (indicator_name) DO NOTHING;

INSERT INTO dim_interval (interval_code, interval_type) VALUES
    ('1min',  'intraday'),
    ('5min',  'intraday'),
    ('15min', 'intraday'),
    ('30min', 'intraday'),
    ('1h',    'intraday'),
    ('1day',  'daily'),
    ('1week', 'weekly'),
    ('daily', 'daily')
ON CONFLICT (interval_code) DO NOTHING;


-- ============================================================
-- بخش ۴: VIEWS برای Power BI
-- ============================================================

-- آخرین قیمت هر سمبل
CREATE OR REPLACE VIEW vw_latest_quotes AS
SELECT DISTINCT ON (s.symbol_code)
    s.symbol_code,
    s.company_name,
    s.exchange,
    s.country,
    q.quote_time_utc,
    q.price,
    q.open,
    q.high,
    q.low,
    q.previous_close,
    q.change,
    q.change_pct,
    q.fetched_at_utc
FROM fact_market_quote q
JOIN dim_symbol s ON s.symbol_id = q.symbol_id
ORDER BY s.symbol_code, q.fetched_at_utc DESC;

-- تاریخچه قیمت
CREATE OR REPLACE VIEW vw_quote_history AS
SELECT
    s.symbol_code,
    s.company_name,
    q.quote_time_utc::DATE AS trade_date,
    q.price,
    q.open,
    q.high,
    q.low,
    q.change_pct
FROM fact_market_quote q
JOIN dim_symbol s ON s.symbol_id = q.symbol_id
WHERE q.quote_time_utc IS NOT NULL
ORDER BY s.symbol_code, q.quote_time_utc;

-- کندل روزانه
CREATE OR REPLACE VIEW vw_candles_daily AS
SELECT
    s.symbol_code,
    s.company_name,
    t.candle_time_utc::DATE AS trade_date,
    t.open, t.high, t.low, t.close, t.volume
FROM fact_market_timeseries t
JOIN dim_symbol   s ON s.symbol_id   = t.symbol_id
JOIN dim_interval i ON i.interval_id = t.interval_id
WHERE i.interval_code = '1day'
ORDER BY s.symbol_code, t.candle_time_utc DESC;

-- RSI با سیگنال
CREATE OR REPLACE VIEW vw_rsi_history AS
SELECT
    s.symbol_code,
    iv.interval_code,
    f.candle_time_utc::DATE AS trade_date,
    f.value AS rsi,
    CASE
        WHEN f.value >= 70 THEN 'Overbought'
        WHEN f.value <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal
FROM fact_market_indicator f
JOIN dim_symbol    s  ON s.symbol_id    = f.symbol_id
JOIN dim_indicator i  ON i.indicator_id = f.indicator_id
JOIN dim_interval  iv ON iv.interval_id = f.interval_id
WHERE i.indicator_name = 'RSI'
ORDER BY s.symbol_code, f.candle_time_utc DESC;

-- MACD با سیگنال
CREATE OR REPLACE VIEW vw_macd_history AS
SELECT
    s.symbol_code,
    iv.interval_code,
    f.candle_time_utc::DATE AS trade_date,
    f.macd, f.macd_signal, f.macd_hist,
    CASE
        WHEN f.macd > f.macd_signal THEN 'Bullish'
        WHEN f.macd < f.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_cross_signal
FROM fact_market_indicator f
JOIN dim_symbol    s  ON s.symbol_id    = f.symbol_id
JOIN dim_indicator i  ON i.indicator_id = f.indicator_id
JOIN dim_interval  iv ON iv.interval_id = f.interval_id
WHERE i.indicator_name = 'MACD'
ORDER BY s.symbol_code, f.candle_time_utc DESC;

-- EMA vs SMA
CREATE OR REPLACE VIEW vw_ema_sma AS
SELECT
    s.symbol_code,
    iv.interval_code,
    e.candle_time_utc::DATE AS trade_date,
    e.value  AS ema_20,
    sm.value AS sma_50
FROM fact_market_indicator e
JOIN fact_market_indicator sm
    ON  sm.symbol_id       = e.symbol_id
    AND sm.candle_time_utc = e.candle_time_utc
    AND sm.interval_id     = e.interval_id
JOIN dim_indicator ei ON ei.indicator_id = e.indicator_id  AND ei.indicator_name = 'EMA'
JOIN dim_indicator si ON si.indicator_id = sm.indicator_id AND si.indicator_name = 'SMA'
JOIN dim_symbol    s  ON s.symbol_id     = e.symbol_id
JOIN dim_interval  iv ON iv.interval_id  = e.interval_id
ORDER BY s.symbol_code, e.candle_time_utc DESC;

-- داده‌های بنیادی
CREATE OR REPLACE VIEW vw_fundamentals AS
SELECT DISTINCT ON (s.symbol_code)
    s.symbol_code, s.company_name, s.exchange, s.sector, s.industry,
    f.ipo_date, f.market_cap, f.share_outstanding,
    f.pe_ratio, f.eps_ttm, f.gross_margin, f.net_margin,
    f.roe, f.debt_to_equity, f.current_ratio, f.beta,
    f.week_52_high, f.week_52_low, f.fetched_at_utc
FROM fact_company_fundamental f
JOIN dim_symbol s ON s.symbol_id = f.symbol_id
ORDER BY s.symbol_code, f.fetched_at_utc DESC;

-- تقویم سود
CREATE OR REPLACE VIEW vw_earnings_upcoming AS
SELECT
    s.symbol_code, s.company_name,
    e.report_date, e.hour,
    e.eps_estimate, e.eps_actual,
    e.revenue_estimate, e.revenue_actual,
    CASE
        WHEN e.eps_actual IS NOT NULL AND e.eps_estimate IS NOT NULL
        THEN ROUND((e.eps_actual - e.eps_estimate)
                   / NULLIF(ABS(e.eps_estimate), 0) * 100, 2)
    END AS eps_surprise_pct
FROM fact_earnings_calendar e
JOIN dim_symbol s ON s.symbol_id = e.symbol_id
WHERE e.report_date >= CURRENT_DATE
ORDER BY e.report_date, s.symbol_code;

-- داشبورد اصلی — همه چیز یکجا
CREATE OR REPLACE VIEW vw_dashboard_main AS
SELECT
    s.symbol_code, s.company_name, s.exchange,
    q.price AS current_price, q.change_pct,
    q.high  AS day_high,      q.low AS day_low,
    f.market_cap, f.pe_ratio, f.eps_ttm, f.beta,
    f.week_52_high, f.week_52_low,
    rsi.value AS rsi_14,
    CASE
        WHEN rsi.value >= 70 THEN 'Overbought'
        WHEN rsi.value <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal,
    macd.macd, macd.macd_signal AS macd_sig, macd.macd_hist,
    CASE
        WHEN macd.macd > macd.macd_signal THEN 'Bullish'
        WHEN macd.macd < macd.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_signal_text,
    q.fetched_at_utc AS last_updated
FROM dim_symbol s
LEFT JOIN LATERAL (
    SELECT * FROM fact_market_quote
    WHERE symbol_id = s.symbol_id
    ORDER BY fetched_at_utc DESC LIMIT 1
) q ON TRUE
LEFT JOIN LATERAL (
    SELECT * FROM fact_company_fundamental
    WHERE symbol_id = s.symbol_id
    ORDER BY fetched_at_utc DESC LIMIT 1
) f ON TRUE
LEFT JOIN LATERAL (
    SELECT fi.* FROM fact_market_indicator fi
    JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
    WHERE fi.symbol_id = s.symbol_id AND di.indicator_name = 'RSI'
    ORDER BY fi.candle_time_utc DESC LIMIT 1
) rsi ON TRUE
LEFT JOIN LATERAL (
    SELECT fi.* FROM fact_market_indicator fi
    JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
    WHERE fi.symbol_id = s.symbol_id AND di.indicator_name = 'MACD'
    ORDER BY fi.candle_time_utc DESC LIMIT 1
) macd ON TRUE;

-- لاگ API
CREATE OR REPLACE VIEW vw_api_log AS
SELECT
    l.called_at_utc,
    src.source_name,
    s.symbol_code,
    l.endpoint,
    l.http_status,
    l.response_ms,
    l.error_msg
FROM log_api_call l
LEFT JOIN dim_source src ON src.source_id = l.source_id
LEFT JOIN dim_symbol s   ON s.symbol_id   = l.symbol_id
ORDER BY l.called_at_utc DESC;


-- ============================================================
-- تأیید نهایی
-- ============================================================
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'dim_source','dim_symbol','dim_interval','dim_indicator',
      'fact_market_quote','fact_market_indicator',
      'fact_market_timeseries','fact_company_fundamental',
      'fact_earnings_calendar','log_api_call'
  )
ORDER BY table_name;
