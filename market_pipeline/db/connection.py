import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Erstellt PostgreSQL-Verbindung."""
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    return conn

DDL = """
-- 1. DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dim_source (
    source_id   SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE,
    base_url    TEXT,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id    SERIAL PRIMARY KEY,
    symbol_code  TEXT NOT NULL UNIQUE,
    company_name TEXT,
    exchange     TEXT,
    country      TEXT,
    currency     TEXT,
    sector       TEXT,
    industry     TEXT
);

CREATE TABLE IF NOT EXISTS dim_interval (
    interval_id   SERIAL PRIMARY KEY,
    interval_code TEXT NOT NULL UNIQUE,
    interval_type TEXT
);

CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_id   SERIAL PRIMARY KEY,
    indicator_name TEXT NOT NULL UNIQUE,
    description    TEXT,
    category       TEXT
);

-- 2. FACT TABLES (TIMESTAMPTZ = UTC in DB)
CREATE TABLE IF NOT EXISTS fact_market_quote (
    quote_id        BIGSERIAL PRIMARY KEY,
    symbol_id       INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    quote_time_utc  TIMESTAMPTZ,
    price           NUMERIC(18,6),
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    previous_close  NUMERIC(18,6),
    change          NUMERIC(18,6),
    change_pct      NUMERIC(10,4),
    raw_payload     JSONB,
    CONSTRAINT uq_fact_quote UNIQUE (symbol_id, source_id, quote_time_utc)
);

CREATE TABLE IF NOT EXISTS fact_market_timeseries (
    timeseries_id   BIGSERIAL PRIMARY KEY,
    symbol_id       INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT NOT NULL REFERENCES dim_source(source_id),
    interval_id     INT NOT NULL REFERENCES dim_interval(interval_id),
    candle_time_utc TIMESTAMPTZ NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6),
    volume          NUMERIC(20,2),
    raw_payload     JSONB,
    CONSTRAINT uq_fact_timeseries UNIQUE (symbol_id, interval_id, candle_time_utc)
);

CREATE TABLE IF NOT EXISTS fact_company_fundamental (
    fundamental_id    BIGSERIAL PRIMARY KEY,
    symbol_id         INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id         INT NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_cap        NUMERIC(22,2),
    raw_profile       JSONB,
    raw_metrics       JSONB
);

CREATE TABLE IF NOT EXISTS log_api_call (
    log_id        BIGSERIAL PRIMARY KEY,
    source_id     INT REFERENCES dim_source(source_id),
    symbol_id     INT REFERENCES dim_symbol(symbol_id),
    called_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    endpoint      TEXT,
    http_status   INT,
    response_ms   INT,
    error_msg     TEXT
);

-- 3. SEED DATA
INSERT INTO dim_source (source_name, base_url, notes) VALUES
    ('finnhub',      'https://finnhub.io/api/v1',         '60 req/min'),
    ('alphavantage', 'https://www.alphavantage.co/query', '25 req/day'),
    ('twelvedata',   'https://api.twelvedata.com',        '800 req/day')
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO dim_indicator (indicator_name, description, category) VALUES
    ('RSI',  'RSI (14)',  'momentum'),
    ('MACD', 'MACD',      'trend'),
    ('EMA',  'EMA (20)',  'trend'),
    ('SMA',  'SMA (50)',  'trend')
ON CONFLICT (indicator_name) DO NOTHING;

INSERT INTO dim_interval (interval_code, interval_type) VALUES
    ('1min','intraday'), ('5min','intraday'), ('1day','daily'), ('daily','daily')
ON CONFLICT (interval_code) DO NOTHING;
"""

def create_schema():
    """Schema erstellen OHNE Daten zu löschen."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    print("[DB] Star Schema created / verified.")

# Dimension Helpers
_cache = {}

def _upsert_dim(conn, table, uk_col, uk_val, pk_col, extra=None):
    cache_key = f"{table}:{uk_val}"
    if cache_key in _cache: return _cache[cache_key]
    cols = [uk_col] + list((extra or {}).keys())
    vals = [uk_val] + list((extra or {}).values())
    ph = ", ".join(["%s"] * len(vals))
    cn = ", ".join(cols)
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != uk_col) or f"{uk_col}=EXCLUDED.{uk_col}"
    sql = f"INSERT INTO {table} ({cn}) VALUES ({ph}) ON CONFLICT ({uk_col}) DO UPDATE SET {upd} RETURNING {pk_col};"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        row_id = cur.fetchone()[0]
    _cache[cache_key] = row_id
    return row_id

def get_source_id(conn, name): return _upsert_dim(conn, "dim_source", "source_name", name, "source_id")
def get_symbol_id(conn, code, **kwargs): return _upsert_dim(conn, "dim_symbol", "symbol_code", code, "symbol_id", kwargs)
def get_interval_id(conn, code): return _upsert_dim(conn, "dim_interval", "interval_code", code, "interval_id")
def get_indicator_id(conn, name): return _upsert_dim(conn, "dim_indicator", "indicator_name", name, "indicator_id")
