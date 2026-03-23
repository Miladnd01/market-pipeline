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
    "started_at": None,
    "last_cycle_at": None,
    "last_error": None,
    "thread_alive": False
}

pipeline_lock = threading.Lock()
pipeline_thread = None


# ---------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------
def now_berlin_iso():
    return datetime.now(BERLIN_TZ).isoformat()


def update_pipeline_status(**kwargs):
    with pipeline_lock:
        for key, value in kwargs.items():
            pipeline_status[key] = value


def get_pipeline_status_copy():
    with pipeline_lock:
        return dict(pipeline_status)


# ---------------------------------------------------
# Background Pipeline
# ---------------------------------------------------
def run_pipeline_loop():
    """
    Startet die eigentliche Endlosschleife der Pipeline.
    Läuft in einem Background-Thread.
    """
    logger.info("Pipeline thread started.")

    update_pipeline_status(
        started=True,
        running=True,
        started_at=now_berlin_iso(),
        last_error=None,
        thread_alive=True
    )

    try:
        # pipeline_main (status_callback=update_pipeline_status) enthält bereits die while-True-Schleife
  pipeline_main(status_callback=update_pipeline_status)

    except Exception as e:
        logger.exception("Pipeline crashed with exception.")
        update_pipeline_status(
            running=False,
            last_error=str(e),
            thread_alive=False
        )

    finally:
        logger.warning("Pipeline thread stopped.")
        update_pipeline_status(
            running=False,
            thread_alive=False
        )


def ensure_pipeline_started():
    """
    Startet die Pipeline genau einmal pro Prozess.
    """
    global pipeline_thread

    with pipeline_lock:
        already_started = pipeline_status["started"]
        thread_is_alive = pipeline_thread is not None and pipeline_thread.is_alive()

        if already_started and thread_is_alive:
            return False

        if thread_is_alive:
            return False

        pipeline_thread = threading.Thread(
            target=run_pipeline_loop,
            daemon=True,
            name="market-pipeline-thread"
        )
        pipeline_thread.start()

        pipeline_status["started"] = True
        pipeline_status["running"] = True
        pipeline_status["started_at"] = now_berlin_iso()
        pipeline_status["last_error"] = None
        pipeline_status["thread_alive"] = True

        logger.info("Pipeline started once from web process.")
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
    Leichter Health-Check Endpoint.
    Ideal für UptimeRobot / Cron / Keep-Alive.
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
    Detaillierter Status für Debugging / Frontend.
    """
    status = get_pipeline_status_copy()
    return jsonify(status), 200


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
            "timestamp": now_berlin_iso()
        }), 500


# ---------------------------------------------------
# Optional: Pipeline manuell triggern / prüfen
# ---------------------------------------------------
@app.route("/api/start-pipeline", methods=["POST", "GET"])
def start_pipeline_api():
    started = ensure_pipeline_started()
    status = get_pipeline_status_copy()

    return jsonify({
        "started_now": started,
        "pipeline": status,
        "timestamp": now_berlin_iso()
    }), 200


# ---------------------------------------------------
# App Start
# ---------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
