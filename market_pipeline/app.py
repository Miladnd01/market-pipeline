from flask import Flask, render_template, jsonify
import threading
import os
import logging
from datetime import datetime
import pytz

# Import der main-Funktion aus main.py
from main import main as pipeline_main
from db.connection import get_connection

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
BERLIN_TZ = pytz.timezone('Europe/Berlin')

# Globaler Status
pipeline_status = {
    "running": False,
    "last_run": None,
    "cycle_count": 0,
    "error": None,
    "start_time": datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
}

# Lock, um doppelten Thread-Start zu verhindern
pipeline_lock = threading.Lock()
pipeline_started = False

def run_pipeline_loop():
    """Hintergrund-Thread für die Pipeline"""
    global pipeline_status
    with pipeline_lock:
        try:
            pipeline_status["running"] = True
            logger.info("🚀 Starting market data pipeline in background...")
            # Startet die Pipeline-Schleife aus main.py
            # Falls main() Parameter akzeptiert, hier anpassen
            pipeline_main() 
        except Exception as e:
            pipeline_status["error"] = str(e)
            logger.error(f"❌ Pipeline error: {e}")
            pipeline_status["running"] = False

@app.before_request
def start_pipeline_once():
    """Startet den Thread beim ersten Web-Request, falls noch nicht aktiv."""
    global pipeline_started
    if not pipeline_started:
        with pipeline_lock:
            if not pipeline_started:
                thread = threading.Thread(target=run_pipeline_loop, daemon=True)
                thread.start()
                pipeline_started = True
                logger.info("✅ Background Thread initialisiert.")

@app.route('/')
def index():
    return render_template('index.html', status=pipeline_status)

@app.route('/health')
def health():
    return {
        "status": "healthy",
        "pipeline_running": pipeline_status["running"],
        "server_time": datetime.now(BERLIN_TZ).isoformat()
    }

@app.route('/status')
def status():
    return jsonify(pipeline_status)

@app.route('/api/dashboard')
def dashboard_data():
    """API Endpoint für Live Dashboard Daten aus der Datenbank."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Statistics
                cur.execute("SELECT COUNT(DISTINCT symbol_code) FROM dim_symbol;")
                total_symbols = cur.fetchone()[0] or 0
                
                cur.execute("SELECT COUNT(*) FROM log_api_call;")
                total_api_calls = cur.fetchone()[0] or 0
                
                cur.execute("""
                    SELECT 
                        ROUND(
                            CAST(COUNT(*) FILTER (WHERE http_status = 200) AS NUMERIC) / 
                            NULLIF(COUNT(*), 0) * 100, 2
                        )
                    FROM log_api_call;
                """)
                success_rate = cur.fetchone()[0] or 0
                
                # 2. Latest Quotes
                cur.execute("""
                    SELECT s.symbol_code, s.company_name, q.price, q.change_pct,
                           TO_CHAR(q.fetched_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS')
                    FROM fact_market_quote q
                    JOIN dim_symbol s ON s.symbol_id = q.symbol_id
                    ORDER BY q.fetched_at_utc DESC LIMIT 10;
                """)
                quotes = [{
                    'symbol_code': r[0], 'company_name': r[1], 
                    'price': float(r[2]) if r[2] else None, 
                    'change_pct': float(r[3]) if r[3] else 0, 'time_berlin': r[4]
                } for r in cur.fetchall()]
                
                # 3. Latest Candles
                cur.execute("""
                    SELECT s.symbol_code, t.open, t.high, t.low, t.close, t.volume,
                           TO_CHAR(t.candle_time_utc AT TIME ZONE 'Europe/Berlin', 'DD.MM HH24:MI')
                    FROM fact_market_timeseries t
                    JOIN dim_symbol s ON s.symbol_id = t.symbol_id
                    ORDER BY t.candle_time_utc DESC LIMIT 10;
                """)
                candles = [{
                    'symbol_code': r[0], 'open': float(r[1]), 'high': float(r[2]),
                    'low': float(r[3]), 'close': float(r[4]), 'volume': float(r[5]),
                    'time_berlin': r[6]
                } for r in cur.fetchall()]
                
                # 4. API Logs
                cur.execute("""
                    SELECT src.source_name, l.endpoint, l.http_status, l.response_ms,
                           TO_CHAR(l.called_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS')
                    FROM log_api_call l
                    LEFT JOIN dim_source src ON src.source_id = l.source_id
                    ORDER BY l.called_at_utc DESC LIMIT 10;
                """)
                api_logs = [{
                    'source_name': r[0], 'endpoint': r[1], 'http_status': r[2],
                    'response_ms': r[3], 'time_berlin': r[4]
                } for r in cur.fetchall()]

                return jsonify({
                    'stats': {'total_symbols': total_symbols, 'total_api_calls': total_api_calls, 'success_rate': float(success_rate)},
                    'quotes': quotes,
                    'candles': candles,
                    'api_logs': api_logs,
                    'timestamp': datetime.now(BERLIN_TZ).isoformat()
                })
    except Exception as e:
        logger.error(f"❌ [API ERROR] {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Lokal ohne Gunicorn:
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
