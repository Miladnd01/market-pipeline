import os
import psycopg2
from dotenv import load_dotenv

# .env laden
load_dotenv()

def get_connection():
    """
    Erstellt eine Verbindung zur PostgreSQL-Datenbank.
    Setzt die Session auf Europe/Berlin.
    """
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "dpg-d6v9lh94tr6s73dgj93g-a.frankfurt-postgres.render.com"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "marketdb_6mxq"),
        user=os.getenv("PGUSER", "marketdb_6mxq_user"),
        password=os.getenv("PGPASSWORD", "gSbpVTiDKKo7YCrgLg3dHSipcpJpR9JF"),
    )
    # Stellt sicher, dass die Datenbank-Verbindung im Berlin-Modus arbeitet
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Europe/Berlin';")
    return conn


# ============================================================
# DDL - DATA DEFINITION LANGUAGE (BERLIN OPTIMIERT)
# ============================================================
# Wir nutzen TIMESTAMP statt TIMESTAMPTZ, um das "+00" zu entfernen.
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

-- 2. FACT TABLES (Zeitstempel ohne Zeitzonen-Anhang)
CREATE TABLE IF NOT EXISTS fact_market_quote (
    quote_id        BIGSERIAL PRIMARY KEY,
    symbol_id       INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT NOT NULL REFERENCES dim_source(source_id),
    -- Nutzt lokale Berliner Zeit ohne Zone
    fetched_at_utc  TIMESTAMP NOT NULL DEFAULT (timezone('Europe/Berlin', now())),
    quote_time_utc  TIMESTAMP,
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

CREATE TABLE IF NOT EXISTS fact_market_indicator (
    indicator_fact_id BIGSERIAL PRIMARY KEY,
    symbol_id          INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id          INT NOT NULL REFERENCES dim_source(source_id),
    indicator_id       INT NOT NULL REFERENCES dim_indicator(indicator_id),
    interval_id        INT REFERENCES dim_interval(interval_id),
    candle_time_utc    TIMESTAMP NOT NULL,
    value              NUMERIC(18,6),
    macd               NUMERIC(18,6),
    macd_signal        NUMERIC(18,6),
    macd_hist          NUMERIC(18,6),
    raw_payload        JSONB,
    CONSTRAINT uq_fact_indicator UNIQUE (symbol_id, indicator_id, interval_id, candle_time_utc)
);

CREATE TABLE IF NOT EXISTS fact_company_fundamental (
    fundamental_id    BIGSERIAL PRIMARY KEY,
    symbol_id         INT NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id         INT NOT NULL REFERENCES dim_source(source_id),
    fetched_at_utc    TIMESTAMP NOT NULL DEFAULT (timezone('Europe/Berlin', now())),
    ipo_date          DATE,
    market_cap        NUMERIC(22,2),
    share_outstanding NUMERIC(18,2),
    pe_ratio          NUMERIC(12,4),
    eps_ttm           NUMERIC(12,4),
    gross_margin      NUMERIC(10,4),
    net_margin        NUMERIC(10,4),
    roe               NUMERIC(10,4),
    debt_to_equity    NUMERIC(10,4),
    current_ratio     NUMERIC(10,4),
    beta              NUMERIC(10,4),
    week_52_high      NUMERIC(18,6),
    week_52_low       NUMERIC(18,6),
    raw_profile       JSONB,
    raw_metrics       JSONB
);

CREATE TABLE IF NOT EXISTS log_api_call (
    log_id        BIGSERIAL PRIMARY KEY,
    source_id     INT REFERENCES dim_source(source_id),
    symbol_id     INT REFERENCES dim_symbol(symbol_id),
    called_at_utc TIMESTAMP NOT NULL DEFAULT (timezone('Europe/Berlin', now())),
    endpoint      TEXT,
    http_status   INT,
    response_ms   INT,
    error_msg     TEXT
);

-- 3. SEED DATA (Standardwerte)
INSERT INTO dim_source (source_name, base_url, notes) VALUES
    ('finnhub',      'https://finnhub.io/api/v1',         '60 req/min free'),
    ('alphavantage', 'https://www.alphavantage.co/query', '25 req/day free'),
    ('twelvedata',   'https://api.twelvedata.com',        '800 req/day free')
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO dim_indicator (indicator_name, description, category) VALUES
    ('RSI',  'Relative Strength Index (14)',          'momentum'),
    ('MACD', 'Moving Average Convergence Divergence', 'trend'),
    ('EMA',  'Exponential Moving Average (20)',        'trend'),
    ('SMA',  'Simple Moving Average (50)',             'trend')
ON CONFLICT (indicator_name) DO NOTHING;

INSERT INTO dim_interval (interval_code, interval_type) VALUES
    ('1min', 'intraday'), ('5min', 'intraday'), ('15min', 'intraday'), 
    ('30min', 'intraday'), ('1h', 'intraday'), ('1day', 'daily'), 
    ('1week', 'weekly'), ('daily', 'daily')
ON CONFLICT (interval_code) DO NOTHING;
"""

def create_schema():
    """Erstellt das Star-Schema und bereinigt alte Views."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Lösche alle alten Views, falls diese noch existieren
            cur.execute("DROP VIEW IF EXISTS vw_api_log CASCADE;")
            cur.execute("DROP VIEW IF EXISTS log_api_call_berlin CASCADE;")
            cur.execute("DROP VIEW IF EXISTS vw_latest_quotes CASCADE;")
            
            # Tabellen erstellen
            cur.execute(DDL)
            
            # Neue View erstellen, die direkt die Berliner Zeit anzeigt
            cur.execute(\"\"\"
                CREATE OR REPLACE VIEW vw_api_log AS
                SELECT log_id, called_at_utc AS zeit_berlin, endpoint, http_status, response_ms
                FROM log_api_call
                ORDER BY log_id DESC;
            \"\"\")
            
        conn.commit()
    print("[DB] Berlin-Zeit-Schema (TIMESTAMP) erfolgreich erstellt.")


# ============================================================
# DIMENSION HELPERS (In-Memory Cache)
# ============================================================
_cache: dict = {}

def _upsert_dim(conn, table: str, uk_col: str, uk_val: str,
                pk_col: str, extra: dict | None = None) -> int:
    cache_key = f"{table}:{uk_val}"
    if cache_key in _cache:
        return _cache[cache_key]

    cols = [uk_col] + list((extra or {}).keys())
    vals = [uk_val] + list((extra or {}).values())
    ph   = ", ".join(["%s"] * len(vals))
    cn   = ", ".join(cols)

    non_uk = [c for c in cols if c != uk_col]
    if non_uk:
        upd = ", ".join(f"{c} = COALESCE(EXCLUDED.{c}, {table}.{c})" for c in non_uk)
    else:
        upd = f"{uk_col} = EXCLUDED.{uk_col}"

    sql = f"INSERT INTO {table} ({cn}) VALUES ({ph}) ON CONFLICT ({uk_col}) DO UPDATE SET {upd} RETURNING {pk_col};"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        row_id = cur.fetchone()[0]

    _cache[cache_key] = row_id
    return row_id

def get_source_id(conn, name: str) -> int:
    return _upsert_dim(conn, "dim_source", "source_name", name, "source_id")

def get_symbol_id(conn, code: str, **kwargs) -> int:
    allowed = {"company_name", "exchange", "country", "currency", "sector", "industry"}
    extra   = {k: v for k, v in kwargs.items() if k in allowed and v}
    return _upsert_dim(conn, "dim_symbol", "symbol_code", code, "symbol_id", extra or None)

def get_interval_id(conn, code: str) -> int:
    return _upsert_dim(conn, "dim_interval", "interval_code", code, "interval_id")

def get_indicator_id(conn, name: str) -> int:
    return _upsert_dim(conn, "dim_indicator", "indicator_name", name, "indicator_id")
