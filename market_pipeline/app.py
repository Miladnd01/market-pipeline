from flask import Flask, render_template, jsonify
import threading
import os
import logging
from datetime import datetime
import pytz

from main import main as pipeline_main
from db.connection import get_connection

# ---------------------------------------------------
# Logging
# ---------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Flask App
# ---------------------------------------------------
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

    # Meta / Progress
    "event": None,
    "cycle_num": 0,
    "next_cycle_num": None,
    "sleep_seconds": None,
    "sleep_started_at": None,

    # Symbols / Processing
    "symbols": [],
    "symbols_total": 0,
    "symbols_ok": 0,
    "symbols_failed": 0,
    "current_symbol": None,
    "current_symbol_index": None,
    "fetch_indicators": False,

    # Timing / Results
    "cycle_started_at": None,
    "cycle_duration_seconds": None,
    "last_symbol_result": None,
    "last_cycle_result": None,
    "poll_interval_seconds": None
}

pipeline_lock = threading.Lock()
pipeline_thread = None

# ---------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------
def now_berlin_iso() -> str:
    return datetime.now(BERLIN_TZ).isoformat()


def update_pipeline_status(**kwargs):
    """
    Thread-sicheres Update des globalen Pipeline-Status.
    Diese Funktion wird auch als status_callback an main.py übergeben.
    """
    with pipeline_lock:
        for key, value in kwargs.items():
            pipeline_status[key] = value

        # Konsistenzregeln
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

        if kwargs.get("event") == "pipeline_error" and "running" not in kwargs:
            pipeline_status["running"] = True

        if kwargs.get("event") == "cycle_finished":
            pipeline_status["current_symbol"] = None
            pipeline_status["current_symbol_index"] = None

        if "symbols" in kwargs and kwargs["symbols"] is None:
            pipeline_status["symbols"] = []

        if "thread_alive" not in kwargs and pipeline_thread is not None:
            pipeline_status["thread_alive"] = pipeline_thread.is_alive()


def get_pipeline_status_copy():
    with pipeline_lock:
        status_copy = dict(pipeline_status)

    # Thread-Status außerhalb des Locks ergänzen
    status_copy["thread_alive"] = pipeline_thread.is_alive() if pipeline_thread else False
    return status_copy


# ---------------------------------------------------
# Pipeline Thread
# ---------------------------------------------------
def run_pipeline_loop():
    """
    Startet die Pipeline im Background-Thread.
    main.py übernimmt die eigentliche Schleife und sendet Status-Updates
    über den status_callback.
    """
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
    """
    Startet die Pipeline genau einmal pro Prozess.
    """
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
    """
    Startet die Pipeline beim ersten Request.
    """
    ensure_pipeline_started()


# ---------------------------------------------------
# Routes
# ---------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html", status=get_pipeline_status_copy())


@app.route("/health")
def health():
    """
    Leichter Health-Check.
    Für UptimeRobot / Cron / Render Keep-Alive geeignet.
    """
    status = get_pipeline_status_copy()

    return jsonify({
        "status": "ok",
        "service": "market-dashboard",
        "time": now_berlin_iso(),
        "pipeline": status
    }), 200


@app.route("/api/pipeline-status")
def pipeline_status_api():
    """
    Detaillierter Pipeline-Status für Debugging / Frontend.
    """
    return jsonify(get_pipeline_status_copy()), 200


@app.route("/api/start-pipeline", methods=["GET", "POST"])
def start_pipeline_api():
    """
    Startet Pipeline manuell, falls sie noch nicht läuft.
    """
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
                cur.execute("SELECT COUNT(DISTINCT symbol_code) FROM dim_symbol;")
                total_symbols = cur.fetchone()[0] or 0

                cur.execute("SELECT COUNT(*) FROM log_api_call;")
                total_api_calls = cur.fetchone()[0] or 0

                cur.execute("""
                    SELECT ROUND(
                        CAST(COUNT(*) FILTER (WHERE http_status = 200) AS NUMERIC)
                        / NULLIF(COUNT(*), 0) * 100,
                        2
                    )
                    FROM log_api_call;
                """)
                success_rate = cur.fetchone()[0] or 0

                # ---------------------------------------------------
                # 2) Latest Quotes
                # ---------------------------------------------------
                cur.execute("""
                    SELECT
                        s.symbol_code,
                        COALESCE(s.exchange, 'Market') AS exchange,
                        q.price,
                        q.change_pct,
                        TO_CHAR(q.fetched_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS') AS local_time
                    FROM fact_market_quote q
                    JOIN dim_symbol s ON s.symbol_id = q.symbol_id
                    ORDER BY q.fetched_at_utc DESC
                    LIMIT 10;
                """)

                quotes = []
                for row in cur.fetchall():
                    quotes.append({
                        "symbol": row[0],
                        "exchange": row[1],
                        "price": float(row[2]) if row[2] is not None else 0.0,
                        "change": float(row[3]) if row[3] is not None else 0.0,
                        "time": row[4]
                    })

                # ---------------------------------------------------
                # 3) Time Series (letzte 24h)
                # ---------------------------------------------------
                cur.execute("""
                    SELECT
                        s.symbol_code,
                        t.close,
                        t.candle_time_utc AT TIME ZONE 'Europe/Berlin' AS local_time
                    FROM fact_market_timeseries t
                    JOIN dim_symbol s ON s.symbol_id = t.symbol_id
                    WHERE t.candle_time_utc > NOW() - INTERVAL '24 hours'
                    ORDER BY s.symbol_code, t.candle_time_utc ASC;
                """)

                raw_series = cur.fetchall()
                history = {}

                for sym, price, local_time in raw_series:
                    if sym not in history:
                        history[sym] = []

                    history[sym].append({
                        "x": local_time.isoformat() if local_time else None,
                        "y": float(price) if price is not None else 0.0
                    })

                # ---------------------------------------------------
                # 4) Latency Verlauf
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
                        "ms": float(row[1]) if row[1] is not None else 0.0
                    })

                # ---------------------------------------------------
                # Pipeline Status ergänzen
                # ---------------------------------------------------
                status = get_pipeline_status_copy()

                return jsonify({
                    "stats": {
                        "total_symbols": total_symbols,
                        "total_api_calls": total_api_calls,
                        "success_rate": float(success_rate)
                    },
                    "quotes": quotes,
                    "history": history,
                    "latency_history": latency_history,
                    "pipeline": status,
                    "timestamp": now_berlin_iso()
                }), 200

    except Exception as e:
        logger.exception("API Error in /api/dashboard")
        return jsonify({
            "error": str(e),
            "timestamp": now_berlin_iso(),
            "pipeline": get_pipeline_status_copy()
        }), 500


# ---------------------------------------------------
# Optional: einfache Landing-Info
# ---------------------------------------------------
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


# ---------------------------------------------------
# App Start
# ---------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
