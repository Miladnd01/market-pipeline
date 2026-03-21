# app.py
from flask import Flask, render_template
import threading
import os

# Import der main-Funktion aus main.py
from main import main as pipeline_main

app = Flask(__name__)

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
    return pipeline_status

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
