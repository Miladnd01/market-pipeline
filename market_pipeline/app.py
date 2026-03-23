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
                # ---------------------------
                # Stats
                # ---------------------------
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

                # ---------------------------
                # Dashboard rows (best source)
                # ---------------------------
                cur.execute("""
                    SELECT
                        symbol_code,
                        company_name,
                        exchange,
                        current_price,
                        change_pct,
                        day_high,
                        day_low,
                        market_cap,
                        pe_ratio,
                        eps_ttm,
                        beta,
                        week_52_high,
                        week_52_low,
                        rsi_14,
                        rsi_signal,
                        macd,
                        macd_sig,
                        macd_hist,
                        macd_signal_text,
                        last_updated
                    FROM vw_dashboard_main
                    WHERE current_price IS NOT NULL
                    ORDER BY symbol_code;
                """)
                dashboard_rows = []
                for row in cur.fetchall():
                    dashboard_rows.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "exchange": row[2],
                        "current_price": float(row[3]) if row[3] is not None else None,
                        "change_pct": float(row[4]) if row[4] is not None else None,
                        "day_high": float(row[5]) if row[5] is not None else None,
                        "day_low": float(row[6]) if row[6] is not None else None,
                        "market_cap": float(row[7]) if row[7] is not None else None,
                        "pe_ratio": float(row[8]) if row[8] is not None else None,
                        "eps_ttm": float(row[9]) if row[9] is not None else None,
                        "beta": float(row[10]) if row[10] is not None else None,
                        "week_52_high": float(row[11]) if row[11] is not None else None,
                        "week_52_low": float(row[12]) if row[12] is not None else None,
                        "rsi_14": float(row[13]) if row[13] is not None else None,
                        "rsi_signal": row[14],
                        "macd": float(row[15]) if row[15] is not None else None,
                        "macd_sig": float(row[16]) if row[16] is not None else None,
                        "macd_hist": float(row[17]) if row[17] is not None else None,
                        "macd_signal_text": row[18],
                        "last_updated": row[19].isoformat() if row[19] else None
                    })

                # ---------------------------
                # Latest quotes
                # ---------------------------
                cur.execute("""
                    SELECT
                        symbol_code,
                        company_name,
                        exchange,
                        country,
                        quote_time_utc,
                        price,
                        open,
                        high,
                        low,
                        previous_close,
                        change,
                        change_pct,
                        fetched_at_utc
                    FROM vw_latest_quotes
                    ORDER BY fetched_at_utc DESC
                    LIMIT 20;
                """)
                quotes = []
                for row in cur.fetchall():
                    quotes.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "exchange": row[2],
                        "country": row[3],
                        "quote_time_utc": row[4].isoformat() if row[4] else None,
                        "price": float(row[5]) if row[5] is not None else None,
                        "open": float(row[6]) if row[6] is not None else None,
                        "high": float(row[7]) if row[7] is not None else None,
                        "low": float(row[8]) if row[8] is not None else None,
                        "previous_close": float(row[9]) if row[9] is not None else None,
                        "change": float(row[10]) if row[10] is not None else None,
                        "change_pct": float(row[11]) if row[11] is not None else None,
                        "fetched_at_utc": row[12].isoformat() if row[12] else None
                    })

                # ---------------------------
                # Chart history (24h intraday; fallback 30d daily)
                # ---------------------------
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
                        "x": local_time.isoformat() if local_time else None,
                        "y": float(close_price) if close_price is not None else None
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
                            "x": local_time.isoformat() if local_time else None,
                            "y": float(close_price) if close_price is not None else None
                        })

                # ---------------------------
                # Latency history
                # ---------------------------
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
                        "ms": float(row[1]) if row[1] is not None else 0.0
                    })

                # ---------------------------
                # Upcoming earnings
                # ---------------------------
                cur.execute("""
                    SELECT
                        symbol_code,
                        company_name,
                        report_date,
                        hour,
                        eps_estimate,
                        eps_actual,
                        revenue_estimate,
                        revenue_actual,
                        eps_surprise_pct
                    FROM vw_earnings_upcoming
                    ORDER BY report_date ASC, symbol_code ASC
                    LIMIT 8;
                """)
                earnings = []
                for row in cur.fetchall():
                    earnings.append({
                        "symbol": row[0],
                        "company_name": row[1],
                        "report_date": row[2].isoformat() if row[2] else None,
                        "hour": row[3],
                        "eps_estimate": float(row[4]) if row[4] is not None else None,
                        "eps_actual": float(row[5]) if row[5] is not None else None,
                        "revenue_estimate": float(row[6]) if row[6] is not None else None,
                        "revenue_actual": float(row[7]) if row[7] is not None else None,
                        "eps_surprise_pct": float(row[8]) if row[8] is not None else None
                    })

                # ---------------------------
                # Recent API log
                # ---------------------------
                cur.execute("""
                    SELECT
                        called_at_utc,
                        source_name,
                        symbol_code,
                        endpoint,
                        http_status,
                        response_ms,
                        error_msg
                    FROM vw_api_log
                    LIMIT 12;
                """)
                api_log = []
                for row in cur.fetchall():
                    api_log.append({
                        "called_at_utc": row[0].isoformat() if row[0] else None,
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
