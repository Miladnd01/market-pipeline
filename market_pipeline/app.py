from pathlib import Path
from flask import Flask, Response, jsonify

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent

INDEX_CANDIDATES = [
    BASE_DIR / "index.html",
    BASE_DIR / "static" / "index.html",
    BASE_DIR / "templates" / "index.html",
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

    try:
        html = index_path.read_text(encoding="utf-8")
        return Response(html, mimetype="text/html")
    except Exception as e:
        return jsonify({
            "error": "Fehler beim Lesen von index.html",
            "details": str(e),
            "path": str(index_path)
        }), 500

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/health", methods=["GET"])
def health_simple():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
