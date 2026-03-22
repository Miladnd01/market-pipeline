from flask import Flask, render_template, jsonify
import threading
import os
import logging
from datetime import datetime, timedelta
import pytz

from main import main as pipeline_main
from db.connection import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
BERLIN_TZ = pytz.timezone('Europe/Berlin')

pipeline_status = {
    "running": False,
    "last_run": None,
    "error": None
}

pipeline_lock = threading.Lock()
pipeline_started = False

def run_pipeline_loop():
    global pipeline_status
    with pipeline_lock:
        try:
            pipeline_status["running"] = True
            pipeline_main() 
        except Exception as e:
            pipeline_status["error"] = str(e)
            pipeline_status["running"] = False

@app.before_request
def start_pipeline_once():
    global pipeline_started
    if not pipeline_started:
        with pipeline_lock:
            if not pipeline_started:
                thread = threading.Thread(target=run_pipeline_loop, daemon=True)
                thread.start()
                pipeline_started = True

@app.route('/')
def index():
    return render_template('index.html', status=pipeline_status)

@app.route('/api/dashboard')
def dashboard_data():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Stats
                cur.execute("SELECT COUNT(DISTINCT symbol_code) FROM dim_symbol;")
                total_symbols = cur.fetchone()[0] or 0
                cur.execute("SELECT COUNT(*) FROM log_api_call;")
                total_api_calls = cur.fetchone()[0] or 0
                cur.execute("SELECT ROUND(CAST(COUNT(*) FILTER (WHERE http_status = 200) AS NUMERIC) / NULLIF(COUNT(*), 0) * 100, 2) FROM log_api_call;")
                success_rate = cur.fetchone()[0] or 0

                # 2. Latest Quotes (Table)
                cur.execute("""
                    SELECT s.symbol_code, s.company_name, q.price, q.change_pct,
                           TO_CHAR(q.fetched_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS')
                    FROM fact_market_quote q
                    JOIN dim_symbol s ON s.symbol_id = q.symbol_id
                    ORDER BY q.fetched_at_utc DESC LIMIT 10;
                """)
                quotes = [{'symbol': r[0], 'name': r[1], 'price': float(r[2]), 'change': float(r[3]), 'time': r[4]} for r in cur.fetchall()]

                # 3. TimeSeries für KURVEN (Letzte 24h für die Top Symbole)
                cur.execute("""
                    SELECT s.symbol_code, t.close, t.candle_time_utc AT TIME ZONE 'Europe/Berlin' as local_time
                    FROM fact_market_timeseries t
                    JOIN dim_symbol s ON s.symbol_id = t.symbol_id
                    WHERE t.candle_time_utc > NOW() - INTERVAL '24 hours'
                    ORDER BY s.symbol_code, t.candle_time_utc ASC;
                """)
                raw_series = cur.fetchall()
                history = {}
                for sym, price, time in raw_series:
                    if sym not in history: history[sym] = []
                    history[sym].append({'x': time.isoformat(), 'y': float(price)})

                # 4. API Latenz Verlauf
                cur.execute("""
                    SELECT TO_CHAR(called_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI'), AVG(response_ms)
                    FROM log_api_call
                    WHERE called_at_utc > NOW() - INTERVAL '6 hours'
                    GROUP BY 1 ORDER BY 1 ASC;
                """)
                latency_history = [{'t': r[0], 'ms': float(r[1])} for r in cur.fetchall()]

                return jsonify({
                    'stats': {'total_symbols': total_symbols, 'total_api_calls': total_api_calls, 'success_rate': float(success_rate)},
                    'quotes': quotes,
                    'history': history,
                    'latency_history': latency_history,
                    'timestamp': datetime.now(BERLIN_TZ).isoformat()
                })
    except Exception as e:
        logger.error(f"API Error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
