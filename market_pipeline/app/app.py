from flask import Flask, render_template
import threading
import os

# Import مستقیم main (چون توی همون پوشه است)
from main import main as pipeline_main

app = Flask(__name__)

# متغیر global برای وضعیت pipeline
pipeline_status = {
    "running": False,
    "last_run": None,
    "cycle_count": 0,
    "error": None
}

def run_pipeline_loop():
    """اجرای pipeline در background thread"""
    global pipeline_status
    
    try:
        pipeline_status["running"] = True
        print("🚀 Starting market data pipeline in background...")
        
        # اجرای pipeline (خودش loop داره)
        pipeline_main()
        
    except Exception as e:
        pipeline_status["error"] = str(e)
        print(f"❌ Pipeline error: {e}")

# شروع pipeline thread
pipeline_thread = threading.Thread(target=run_pipeline_loop, daemon=True)
pipeline_thread.start()

@app.route('/')
def index():
    """صفحه اصلی با Power BI Dashboard"""
    return render_template('index.html', status=pipeline_status)

@app.route('/health')
def health():
    """Health check برای Render"""
    return {
        "status": "healthy",
        "pipeline_running": pipeline_status["running"],
        "cycle_count": pipeline_status["cycle_count"]
    }

@app.route('/status')
def status():
    """نمایش وضعیت pipeline"""
    return pipeline_status

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
