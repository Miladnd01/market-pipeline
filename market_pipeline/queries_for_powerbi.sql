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
JOIN dim_symbol         s ON s.symbol_id = q.symbol_id
ORDER BY s.symbol_code, q.fetched_at_utc DESC;



CREATE OR REPLACE VIEW vw_quote_history AS
SELECT
    s.symbol_code,
    s.company_name,
    q.quote_time_utc::DATE  AS trade_date,
    q.price,
    q.open,
    q.high,
    q.low,
    q.change_pct
FROM fact_market_quote q
JOIN dim_symbol         s ON s.symbol_id = q.symbol_id
WHERE q.quote_time_utc IS NOT NULL
ORDER BY s.symbol_code, q.quote_time_utc;


CREATE OR REPLACE VIEW vw_candles_daily AS
SELECT
    s.symbol_code,
    s.company_name,
    t.candle_time_utc::DATE AS trade_date,
    t.open,
    t.high,
    t.low,
    t.close,
    t.volume
FROM fact_market_timeseries t
JOIN dim_symbol              s ON s.symbol_id   = t.symbol_id
JOIN dim_interval            i ON i.interval_id = t.interval_id
WHERE i.interval_code = '1day'
ORDER BY s.symbol_code, t.candle_time_utc DESC;



CREATE OR REPLACE VIEW vw_rsi_history AS
SELECT
    s.symbol_code,
    i.indicator_name,
    iv.interval_code,
    f.candle_time_utc::DATE AS trade_date,
    f.value                 AS rsi,
    CASE
        WHEN f.value >= 70 THEN 'Overbought'
        WHEN f.value <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal
FROM fact_market_indicator f
JOIN dim_symbol             s  ON s.symbol_id    = f.symbol_id
JOIN dim_indicator          i  ON i.indicator_id = f.indicator_id
JOIN dim_interval           iv ON iv.interval_id = f.interval_id
WHERE i.indicator_name = 'RSI'
ORDER BY s.symbol_code, f.candle_time_utc DESC;



CREATE OR REPLACE VIEW vw_macd_history AS
SELECT
    s.symbol_code,
    iv.interval_code,
    f.candle_time_utc::DATE AS trade_date,
    f.macd,
    f.macd_signal,
    f.macd_hist,
    CASE
        WHEN f.macd > f.macd_signal THEN 'Bullish'
        WHEN f.macd < f.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END AS macd_cross_signal
FROM fact_market_indicator f
JOIN dim_symbol             s  ON s.symbol_id    = f.symbol_id
JOIN dim_indicator          i  ON i.indicator_id = f.indicator_id
JOIN dim_interval           iv ON iv.interval_id = f.interval_id
WHERE i.indicator_name = 'MACD'
ORDER BY s.symbol_code, f.candle_time_utc DESC;



CREATE OR REPLACE VIEW vw_ema_sma AS
SELECT
    s.symbol_code,
    iv.interval_code,
    e.candle_time_utc::DATE AS trade_date,
    e.value                 AS ema_20,
    sm.value                AS sma_50
FROM fact_market_indicator e
JOIN fact_market_indicator  sm ON  sm.symbol_id       = e.symbol_id
                                AND sm.candle_time_utc = e.candle_time_utc
                                AND sm.interval_id     = e.interval_id
JOIN dim_indicator          ei ON  ei.indicator_id    = e.indicator_id  AND ei.indicator_name = 'EMA'
JOIN dim_indicator          si ON  si.indicator_id    = sm.indicator_id AND si.indicator_name = 'SMA'
JOIN dim_symbol             s  ON  s.symbol_id        = e.symbol_id
JOIN dim_interval           iv ON  iv.interval_id     = e.interval_id
ORDER BY s.symbol_code, e.candle_time_utc DESC;



CREATE OR REPLACE VIEW vw_fundamentals AS
SELECT DISTINCT ON (s.symbol_code)
    s.symbol_code,
    s.company_name,
    s.exchange,
    s.sector,
    s.industry,
    f.ipo_date,
    f.market_cap,
    f.share_outstanding,
    f.pe_ratio,
    f.eps_ttm,
    f.gross_margin,
    f.net_margin,
    f.roe,
    f.debt_to_equity,
    f.current_ratio,
    f.beta,
    f.week_52_high,
    f.week_52_low,
    f.fetched_at_utc
FROM fact_company_fundamental f
JOIN dim_symbol               s ON s.symbol_id = f.symbol_id
ORDER BY s.symbol_code, f.fetched_at_utc DESC;



CREATE OR REPLACE VIEW vw_earnings_upcoming AS
SELECT
    s.symbol_code,
    s.company_name,
    e.report_date,
    e.hour,
    e.eps_estimate,
    e.eps_actual,
    e.revenue_estimate,
    e.revenue_actual,
    CASE
        WHEN e.eps_actual IS NOT NULL AND e.eps_estimate IS NOT NULL
        THEN ROUND((e.eps_actual - e.eps_estimate) / NULLIF(ABS(e.eps_estimate), 0) * 100, 2)
    END AS eps_surprise_pct
FROM fact_earnings_calendar e
JOIN dim_symbol              s ON s.symbol_id = e.symbol_id
WHERE e.report_date >= CURRENT_DATE
ORDER BY e.report_date, s.symbol_code;



CREATE OR REPLACE VIEW vw_dashboard_main AS
SELECT
    s.symbol_code,
    s.company_name,
    s.exchange,
    -- قیمت
    q.price             AS current_price,
    q.change_pct,
    q.high              AS day_high,
    q.low               AS day_low,
    -- بنیادی
    f.market_cap,
    f.pe_ratio,
    f.eps_ttm,
    f.beta,
    f.week_52_high,
    f.week_52_low,
    -- RSI
    rsi.value           AS rsi_14,
    CASE
        WHEN rsi.value >= 70 THEN 'Overbought'
        WHEN rsi.value <= 30 THEN 'Oversold'
        ELSE 'Neutral'
    END                 AS rsi_signal,
    -- MACD
    macd.macd,
    macd.macd_signal    AS macd_sig,
    macd.macd_hist,
    CASE
        WHEN macd.macd > macd.macd_signal THEN 'Bullish'
        WHEN macd.macd < macd.macd_signal THEN 'Bearish'
        ELSE 'Neutral'
    END                 AS macd_signal_text,
    -- زمان آخرین بروزرسانی
    q.fetched_at_utc    AS last_updated
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
    SELECT fi.*
    FROM fact_market_indicator fi
    JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
    WHERE fi.symbol_id = s.symbol_id AND di.indicator_name = 'RSI'
    ORDER BY fi.candle_time_utc DESC LIMIT 1
) rsi ON TRUE
LEFT JOIN LATERAL (
    SELECT fi.*
    FROM fact_market_indicator fi
    JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
    WHERE fi.symbol_id = s.symbol_id AND di.indicator_name = 'MACD'
    ORDER BY fi.candle_time_utc DESC LIMIT 1
) macd ON TRUE;



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
