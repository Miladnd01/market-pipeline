from pathlib import Path
from flask import Flask, Response, jsonify

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent

INDEX_CANDIDATES = [
    BASE_DIR / "index.html",
    BASE_DIR / "static" / "index.html",
]

def find_index_file():
    for path in INDEX_CANDIDATES:
        if path.exists():
            return path
    return None

@app.route("/", methods=["GET"])
def home():
    index_path = find_index_file()

    if index_path is None:
        return jsonify({
            "error": "index.html nicht gefunden",
            "searched_in": [str(p) for p in INDEX_CANDIDATES]
        }), 500

    html = index_path.read_text(encoding="utf-8")
    html = html.replace(
        "const API_BASE = 'http://localhost:5000/api';",
        "const API_BASE = '/api';"
    )
    return Response(html, mimetype="text/html")

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})
