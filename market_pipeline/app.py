from flask import Flask, render_template, jsonify
import threading
import os
import logging
from datetime import datetime
import pytz

from main import main as pipeline_main
from db.connection import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
BERLIN_TZ = pytz.timezone("Europe/Berlin")

# ---------------------------------------------------
# Pipeline Status
# ---------------------------------------------------
pipeline_status = {
    "started": False,
    "running": False,
    "thread_alive": False,
    "phase": "idle",
    "started_at": None,
    "stopped_at": None,
    "last_cycle_at": None,
    "last_successful_cycle_at": None,
    "last_error": None,
    "error": None,
    "event": None,
    "cycle_num": 0,
    "next_cycle_num": None,
    "sleep_seconds": None,
    "sleep_started_at": None,
    "symbols": [],
    "symbols_total": 0,
    "symbols_ok": 0,
    "symbols_failed": 0,
    "current_symbol": None,
    "current_symbol_index": None,
    "fetch_indicators": False,
    "cycle_started_at": None,
    "cycle_duration_seconds": None,
    "last_symbol_result": None,
    "last_cycle_result": None,
    "poll_interval_seconds": None
}

pipeline_lock = threading.Lock()
pipeline_thread = None


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------
def now_berlin_iso() -> str:
    return datetime.now(BERLIN_TZ).isoformat()


def to_float(value):
    return float(value) if value is not None else None


def to_iso(value):
    return value.isoformat() if value is not None else None


def update_pipeline_status(**kwargs):
    with pipeline_lock:
        for key, value in kwargs.items():
            pipeline_status[key] = value

        if "error" in kwargs and kwargs["error"]:
            pipeline_status["last_error"] = kwargs["error"]

        if kwargs.get("event") == "pipeline_started":
            pipeline_status["started"] = True
            pipeline_status["running"] = True
            pipeline_status["thread_alive"] = True
            pipeline_status["stopped_at"] = None

        if kwargs.get("event") == "pipeline_stopped":
            pipeline_status["running"] = False
            pipeline_status["thread_alive"] = False
            pipeline_status["stopped_at"] = kwargs.get("stopped_at", now_berlin_iso())

        if kwargs.get("event") == "cycle_finished":
            pipeline_status["current_symbol"] = None
            pipeline_status["current_symbol_index"] = None

        if "thread_alive" not in kwargs and pipeline_thread is not None:
            pipeline_status["thread_alive"] = pipeline_thread.is_alive()


def get_pipeline_status_copy():
    with pipeline_lock:
        status_copy = dict(pipeline_status)

    status_copy["thread_alive"] = pipeline_thread.is_alive() if pipeline_thread else False
    return status_copy


# ---------------------------------------------------
# Pipeline Thread
# ---------------------------------------------------
def run_pipeline_loop():
    logger.info("Pipeline thread booting...")

    update_pipeline_status(
        event="thread_boot",
        started=True,
        running=True,
        thread_alive=True,
        phase="thread_booting",
        started_at=now_berlin_iso(),
        error=None
    )

    try:
        pipeline_main(status_callback=update_pipeline_status)

    except Exception as e:
        logger.exception("Pipeline crashed with exception")
        update_pipeline_status(
            event="pipeline_error",
            running=False,
            thread_alive=False,
            phase="crashed",
            error=str(e),
            stopped_at=now_berlin_iso()
        )

    finally:
        logger.warning("Pipeline thread stopped")
        update_pipeline_status(
            event="thread_stopped",
            running=False,
            thread_alive=False,
            phase="stopped",
            stopped_at=now_berlin_iso()
        )


def ensure_pipeline_started():
    global pipeline_thread

    with pipeline_lock:
        if pipeline_thread is not None and pipeline_thread.is_alive():
            return False

        pipeline_thread = threading.Thread(
            target=run_pipeline_loop,
            daemon=True,
            name="market-pipeline-thread"
        )
        pipeline_thread.start()

        pipeline_status["started"] = True
        pipeline_status["running"] = True
        pipeline_status["thread_alive"] = True
        pipeline_status["phase"] = "starting"
        pipeline_status["started_at"] = now_berlin_iso()
        pipeline_status["stopped_at"] = None
        pipeline_status["error"] = None
        pipeline_status["last_error"] = None

        logger.info("Pipeline started from web process")
        return True


@app.before_request
def start_pipeline_once():
    ensure_pipeline_started()


# ---------------------------------------------------
# Routes
# ---------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html", status=get_pipeline_status_copy())


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "market-dashboard",
        "time": now_berlin_iso(),
        "pipeline": get_pipeline_status_copy()
    }), 200


@app.route("/api")
def api_root():
    return jsonify({
        "service": "market-dashboard",
        "available_endpoints": [
            "/",
            "/health",
            "/api",
            "/api/dashboard",
            "/api/pipeline-status",
            "/api/start-pipeline"
        ],
        "timestamp": now_berlin_iso()
    }), 200


@app.route("/api/pipeline-status")
def pipeline_status_api():
    return jsonify(get_pipeline_status_copy()), 200


@app.route("/api/start-pipeline", methods=["GET", "POST"])
def start_pipeline_api():
    started_now = ensure_pipeline_started()
    return jsonify({
        "started_now": started_now,
        "pipeline": get_pipeline_status_copy(),
        "timestamp": now_berlin_iso()
    }), 200


@app.route("/api/dashboard")
def dashboard_data():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # ---------------------------------------------------
                # 1) Stats
                # ---------------------------------------------------
                cur.execute("SELECT COUNT(*) FROM dim_symbol;")
                total_symbols = cur.fetchone()[0] or 0

                cur.execute("SELECT COUNT(*) FROM log_api_call;")
                total_api_calls = cur.fetchone()[0] or 0

                cur.execute("""
                    SELECT COALESCE(
                        ROUND(
                            CAST(COUNT(*) FILTER (WHERE http_status = 200) AS NUMERIC)
                            / NULLIF(COUNT(*), 0) * 100,
                            2
                        ),
                        0
                    )
                    FROM log_api_call;
                """)
                success_rate = float(cur.fetchone()[0] or 0)

                # ---------------------------------------------------
                # 2) dashboard_rows
                #    Letzter Quote + letztes Fundamental + letzter RSI + letzter MACD
                # ---------------------------------------------------
                cur.execute("""
                    SELECT
                        s.symbol_code,
                        s.company_name,
                        COALESCE(s.exchange, 'Market') AS exchange,

                        q.price AS current_price,
                        q.change_pct,
                        q.high AS day_high,
                        q.low AS day_low,
                        q.fetched_at_utc AS last_updated,

                        f.market_cap,
                        f.pe_ratio,
                        f.eps_ttm,
                        f.beta,
                        f.week_52_high,
                        f.week_52_low,

                        rsi.value AS rsi_14,
                        CASE
                            WHEN rsi.value >= 70 THEN 'Overbought'
                            WHEN rsi.value <= 30 THEN 'Oversold'
                            WHEN rsi.value IS NULL THEN NULL
                            ELSE 'Neutral'
                        END AS rsi_signal,

                        macd.macd,
                        macd.macd_signal AS macd_sig,
                        macd.macd_hist,
                        CASE
                            WHEN macd.macd > macd.macd_signal THEN 'Bullish'
                            WHEN macd.macd < macd.macd_signal THEN 'Bearish'
                            WHEN macd.macd IS NULL OR macd.macd_signal IS NULL THEN NULL
                            ELSE 'Neutral'
                        END AS macd_signal_text

                    FROM dim_symbol s

                    LEFT JOIN LATERAL (
                        SELECT fq.*
                        FROM fact_market_quote fq
                        WHERE fq.symbol_id = s.symbol_id
                        ORDER BY fq.fetched_at_utc DESC
                        LIMIT 1
                    ) q ON TRUE

                    LEFT JOIN LATERAL (
                        SELECT ff.*
                        FROM fact_company_fundamental ff
                        WHERE ff.symbol_id = s.symbol_id
                        ORDER BY ff.fetched_at_utc DESC
                        LIMIT 1
                    ) f ON TRUE

                    LEFT JOIN LATERAL (
                        SELECT fi.*
                        FROM fact_market_indicator fi
                        JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
                        WHERE fi.symbol_id = s.symbol_id
                          AND di.indicator_name = 'RSI'
                        ORDER BY fi.candle_time_utc DESC
                        LIMIT 1
                    ) rsi ON TRUE

                    LEFT JOIN LATERAL (
                        SELECT fi.*
                        FROM fact_market_indicator fi
                        JOIN dim_indicator di ON di.indicator_id = fi.indicator_id
                        WHERE fi.symbol_id = s.symbol_id
                          AND di.indicator_name = 'MACD'
                        ORDER BY fi.candle_time_utc DESC
                        LIMIT 1
                    ) macd ON TRUE

                    WHERE q.price IS NOT NULL
                    ORDER BY s.symbol_code;
                """)

                dashboard_rows = []
                for row in cur.fetchall():
                    dashboard_rows.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "exchange": row[2],
                        "current_price": to_float(row[3]),
                        "change_pct": to_float(row[4]),
                        "day_high": to_float(row[5]),
                        "day_low": to_float(row[6]),
                        "last_updated": to_iso(row[7]),
                        "market_cap": to_float(row[8]),
                        "pe_ratio": to_float(row[9]),
                        "eps_ttm": to_float(row[10]),
                        "beta": to_float(row[11]),
                        "week_52_high": to_float(row[12]),
                        "week_52_low": to_float(row[13]),
                        "rsi_14": to_float(row[14]),
                        "rsi_signal": row[15],
                        "macd": to_float(row[16]),
                        "macd_sig": to_float(row[17]),
                        "macd_hist": to_float(row[18]),
                        "macd_signal_text": row[19]
                    })

                # ---------------------------------------------------
                # 3) latest quotes
                # ---------------------------------------------------
                cur.execute("""
                    SELECT DISTINCT ON (s.symbol_code)
                        s.symbol_code,
                        s.company_name,
                        COALESCE(s.exchange, 'Market') AS exchange,
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
                """)

                quotes = []
                for row in cur.fetchall():
                    quotes.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "exchange": row[2],
                        "country": row[3],
                        "quote_time_utc": to_iso(row[4]),
                        "price": to_float(row[5]),
                        "open": to_float(row[6]),
                        "high": to_float(row[7]),
                        "low": to_float(row[8]),
                        "previous_close": to_float(row[9]),
                        "change": to_float(row[10]),
                        "change_pct": to_float(row[11]),
                        "fetched_at_utc": to_iso(row[12])
                    })

                # ---------------------------------------------------
                # 4) history
                #    Intraday 24h, fallback daily 30d
                # ---------------------------------------------------
                cur.execute("""
                    SELECT
                        s.symbol_code,
                        t.close,
                        t.candle_time_utc AT TIME ZONE 'Europe/Berlin' AS local_time
                    FROM fact_market_timeseries t
                    JOIN dim_symbol s   ON s.symbol_id = t.symbol_id
                    JOIN dim_interval i ON i.interval_id = t.interval_id
                    WHERE i.interval_code IN ('1min', '5min', '15min', '30min', '1h')
                      AND t.candle_time_utc > NOW() - INTERVAL '24 hours'
                    ORDER BY s.symbol_code, t.candle_time_utc ASC;
                """)

                raw_series = cur.fetchall()
                history = {}

                for sym, close_price, local_time in raw_series:
                    history.setdefault(sym, []).append({
                        "x": to_iso(local_time),
                        "y": to_float(close_price)
                    })

                if not history:
                    cur.execute("""
                        SELECT
                            s.symbol_code,
                            t.close,
                            t.candle_time_utc AT TIME ZONE 'Europe/Berlin' AS local_time
                        FROM fact_market_timeseries t
                        JOIN dim_symbol s   ON s.symbol_id = t.symbol_id
                        JOIN dim_interval i ON i.interval_id = t.interval_id
                        WHERE i.interval_code IN ('1day', 'daily')
                          AND t.candle_time_utc > NOW() - INTERVAL '30 days'
                        ORDER BY s.symbol_code, t.candle_time_utc ASC;
                    """)
                    raw_series = cur.fetchall()

                    for sym, close_price, local_time in raw_series:
                        history.setdefault(sym, []).append({
                            "x": to_iso(local_time),
                            "y": to_float(close_price)
                        })

                # ---------------------------------------------------
                # 5) latency history
                # ---------------------------------------------------
                cur.execute("""
                    SELECT
                        TO_CHAR(called_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI') AS t,
                        AVG(response_ms) AS avg_ms
                    FROM log_api_call
                    WHERE called_at_utc > NOW() - INTERVAL '6 hours'
                    GROUP BY 1
                    ORDER BY 1 ASC;
                """)

                latency_history = []
                for row in cur.fetchall():
                    latency_history.append({
                        "t": row[0],
                        "ms": to_float(row[1]) or 0.0
                    })

                # ---------------------------------------------------
                # 6) earnings
                # ---------------------------------------------------
                cur.execute("""
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
                            WHEN e.eps_actual IS NOT NULL
                             AND e.eps_estimate IS NOT NULL
                            THEN ROUND(
                                (e.eps_actual - e.eps_estimate)
                                / NULLIF(ABS(e.eps_estimate), 0) * 100,
                                2
                            )
                            ELSE NULL
                        END AS eps_surprise_pct
                    FROM fact_earnings_calendar e
                    JOIN dim_symbol s ON s.symbol_id = e.symbol_id
                    WHERE e.report_date >= CURRENT_DATE
                    ORDER BY e.report_date ASC, s.symbol_code ASC
                    LIMIT 8;
                """)

                earnings = []
                for row in cur.fetchall():
                    earnings.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "report_date": to_iso(row[2]),
                        "hour": row[3],
                        "eps_estimate": to_float(row[4]),
                        "eps_actual": to_float(row[5]),
                        "revenue_estimate": to_float(row[6]),
                        "revenue_actual": to_float(row[7]),
                        "eps_surprise_pct": to_float(row[8])
                    })

                # ---------------------------------------------------
                # 7) api log
                # ---------------------------------------------------
                cur.execute("""
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
                    LEFT JOIN dim_symbol s   ON s.symbol_id = l.symbol_id
                    ORDER BY l.called_at_utc DESC
                    LIMIT 12;
                """)

                api_log = []
                for row in cur.fetchall():
                    api_log.append({
                        "called_at_utc": to_iso(row[0]),
                        "source_name": row[1],
                        "symbol_code": row[2],
                        "endpoint": row[3],
                        "http_status": row[4],
                        "response_ms": row[5],
                        "error_msg": row[6]
                    })

                return jsonify({
                    "stats": {
                        "total_symbols": total_symbols,
                        "total_api_calls": total_api_calls,
                        "success_rate": success_rate
                    },
                    "dashboard_rows": dashboard_rows,
                    "quotes": quotes,
                    "history": history,
                    "latency_history": latency_history,
                    "earnings": earnings,
                    "api_log": api_log,
                    "pipeline": get_pipeline_status_copy(),
                    "timestamp": now_berlin_iso()
                }), 200

    except Exception as e:
        logger.exception("API Error in /api/dashboard")
        return jsonify({
            "error": str(e),
            "pipeline": get_pipeline_status_copy(),
            "timestamp": now_berlin_iso()
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
