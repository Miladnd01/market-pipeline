# app.py
from flask import Flask, render_template, jsonify
import threading
import os
from datetime import datetime
import pytz

# Import der main-Funktion aus main.py
from main import main as pipeline_main
from db.connection import get_connection

app = Flask(__name__)

BERLIN_TZ = pytz.timezone('Europe/Berlin')

pipeline_status = {
    "running": False,
    "last_run": None,
    "cycle_count": 0,
    "error": None
}

def run_pipeline_loop():
    """Hintergrund-Thread für die Pipeline"""
    global pipeline_status
    try:
        pipeline_status["running"] = True
        print("🚀 Starting market data pipeline in background...")
        # Startet die Pipeline-Schleife aus main.py
        pipeline_main()
    except Exception as e:
        pipeline_status["error"] = str(e)
        print(f"❌ Pipeline error: {e}")
        pipeline_status["running"] = False

# Start des Hintergrund-Threads beim Laden der App
pipeline_thread = threading.Thread(target=run_pipeline_loop, daemon=True)
pipeline_thread.start()


@app.route('/')
def index():
    return render_template('index.html', status=pipeline_status)


@app.route('/health')
def health():
    return {
        "status": "healthy",
        "pipeline_running": pipeline_status["running"]
    }


@app.route('/status')
def status():
    return jsonify(pipeline_status)


@app.route('/api/dashboard')
def dashboard_data():
    """
    API Endpoint für Live Dashboard Daten
    Liefert alle Daten für das Frontend
    """
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
                            NULLIF(COUNT(*), 0) * 100, 
                            2
                        ) AS success_rate
                    FROM log_api_call;
                """)
                success_rate = cur.fetchone()[0] or 0
                
                # 2. Latest Quotes
                cur.execute("""
                    SELECT 
                        s.symbol_code,
                        s.company_name,
                        q.price,
                        q.change_pct,
                        TO_CHAR(q.fetched_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS') AS time_berlin
                    FROM fact_market_quote q
                    JOIN dim_symbol s ON s.symbol_id = q.symbol_id
                    ORDER BY q.fetched_at_utc DESC
                    LIMIT 10;
                """)
                quotes = []
                for row in cur.fetchall():
                    quotes.append({
                        'symbol_code': row[0],
                        'company_name': row[1],
                        'price': float(row[2]) if row[2] else None,
                        'change_pct': float(row[3]) if row[3] else 0,
                        'time_berlin': row[4]
                    })
                
                # 3. Latest Candles
                cur.execute("""
                    SELECT 
                        s.symbol_code,
                        t.open,
                        t.high,
                        t.low,
                        t.close,
                        t.volume,
                        TO_CHAR(t.candle_time_utc AT TIME ZONE 'Europe/Berlin', 'DD.MM HH24:MI') AS time_berlin
                    FROM fact_market_timeseries t
                    JOIN dim_symbol s ON s.symbol_id = t.symbol_id
                    ORDER BY t.candle_time_utc DESC
                    LIMIT 10;
                """)
                candles = []
                for row in cur.fetchall():
                    candles.append({
                        'symbol_code': row[0],
                        'open': float(row[1]) if row[1] else None,
                        'high': float(row[2]) if row[2] else None,
                        'low': float(row[3]) if row[3] else None,
                        'close': float(row[4]) if row[4] else None,
                        'volume': float(row[5]) if row[5] else None,
                        'time_berlin': row[6]
                    })
                
                # 4. API Logs
                cur.execute("""
                    SELECT 
                        src.source_name,
                        l.endpoint,
                        l.http_status,
                        l.response_ms,
                        TO_CHAR(l.called_at_utc AT TIME ZONE 'Europe/Berlin', 'HH24:MI:SS') AS time_berlin
                    FROM log_api_call l
                    LEFT JOIN dim_source src ON src.source_id = l.source_id
                    ORDER BY l.called_at_utc DESC
                    LIMIT 10;
                """)
                api_logs = []
                for row in cur.fetchall():
                    api_logs.append({
                        'source_name': row[0],
                        'endpoint': row[1],
                        'http_status': row[2],
                        'response_ms': row[3],
                        'time_berlin': row[4]
                    })
                
                # 5. Fundamentals
                cur.execute("""
                    SELECT DISTINCT ON (s.symbol_code)
                        s.symbol_code,
                        s.company_name,
                        f.market_cap,
                        f.pe_ratio,
                        f.eps_ttm
                    FROM fact_company_fundamental f
                    JOIN dim_symbol s ON s.symbol_id = f.symbol_id
                    ORDER BY s.symbol_code, f.fetched_at_utc DESC;
                """)
                fundamentals = []
                for row in cur.fetchall():
                    fundamentals.append({
                        'symbol_code': row[0],
                        'company_name': row[1],
                        'market_cap': float(row[2]) if row[2] else None,
                        'pe_ratio': float(row[3]) if row[3] else None,
                        'eps_ttm': float(row[4]) if row[4] else None
                    })
                
                return jsonify({
                    'stats': {
                        'total_symbols': total_symbols,
                        'total_api_calls': total_api_calls,
                        'success_rate': float(success_rate)
                    },
                    'quotes': quotes,
                    'candles': candles,
                    'api_logs': api_logs,
                    'fundamentals': fundamentals,
                    'timestamp': datetime.now(BERLIN_TZ).isoformat()
                })
                
    except Exception as e:
        print(f"❌ [API ERROR] {e}")
        return jsonify({
            'error': str(e),
            'stats': {'total_symbols': 0, 'total_api_calls': 0, 'success_rate': 0},
            'quotes': [],
            'candles': [],
            'api_logs': [],
            'fundamentals': []
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
