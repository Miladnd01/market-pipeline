from pathlib import Path
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from decimal import Decimal
import os, json

app = Flask(__name__)
CORS(app)  # Browser kann von Render.com auf API zugreifen

# ═══════════ FILE SERVING ═══════════
BASE_DIR = Path(__file__).resolve().parent
INDEX_CANDIDATES = [
    BASE_DIR / "index.html",
    BASE_DIR / "static" / "index.html",
    BASE_DIR / "templates" / "index.html",
]

def find_index():
    for p in INDEX_CANDIDATES:
        if p.exists():
            return p
    return None

@app.route("/", methods=["GET"])
def home():
    p = find_index()
    if not p:
        return jsonify({"error": "index.html nicht gefunden"}), 500
    return Response(p.read_text(encoding="utf-8"), mimetype="text/html")

# ═══════════ HEALTH ═══════════
@app.route("/api/health")
@app.route("/health")
def health():
    conn = get_db()
    if conn:
        conn.close()
        return jsonify({"status": "ok", "db": "connected"})
    return jsonify({"status": "error", "db": "disconnected"}), 503

# ═══════════ DB CONFIG ═══════════
# Minimal fix:
# 1) Prefer Render/Postgres PG* env vars
# 2) Keep DB_* as fallback
DB_CONFIG = {
    "host": os.getenv("PGHOST") or os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("PGDATABASE") or os.getenv("DB_NAME", "market_db"),
    "user": os.getenv("PGUSER") or os.getenv("DB_USER", "postgres"),
    "password": os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD", ""),
    "port": int(os.getenv("PGPORT") or os.getenv("DB_PORT", 5432)),
}

def get_db():
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"❌ DB: {e}")
        return None

# JSON-sichere Serialisierung
class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        return super().default(obj)

def jsonify_safe(data):
    return app.response_class(
        response=json.dumps(data, cls=SafeEncoder, ensure_ascii=False),
        status=200,
        mimetype="application/json"
    )

# ═══════════ API: TABELLEN-LISTE ═══════════
@app.route("/api/tables")
def get_tables():
    conn = get_db()
    if not conn:
        return jsonify({"error": "Datenbankverbindung fehlgeschlagen"}), 500
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                t.table_name                                             AS name,
                CASE
                    WHEN t.table_name LIKE 'dim_%%'  THEN 'dim'
                    WHEN t.table_name LIKE 'fact_%%' THEN 'fact'
                    WHEN t.table_name LIKE 'log_%%'  THEN 'log'
                    ELSE 'other'
                END                                                      AS type,
                obj_description(
                    (quote_ident(t.table_schema)||'.'||
                     quote_ident(t.table_name))::regclass
                )                                                        AS description,
                (SELECT COUNT(*)
                 FROM information_schema.columns c
                 WHERE c.table_name  = t.table_name
                   AND c.table_schema = t.table_schema)                  AS column_count,
                COALESCE(
                    (SELECT reltuples::bigint
                     FROM pg_class
                     WHERE oid = (quote_ident(t.table_schema)||'.'||
                                  quote_ident(t.table_name))::regclass),
                    0
                )                                                        AS row_count
            FROM information_schema.tables t
            WHERE t.table_schema = 'public'
              AND t.table_type   = 'BASE TABLE'
            ORDER BY
                CASE
                    WHEN t.table_name LIKE 'dim_%%'  THEN 1
                    WHEN t.table_name LIKE 'fact_%%' THEN 2
                    WHEN t.table_name LIKE 'log_%%'  THEN 3
                    ELSE 4
                END,
                t.table_name
        """)
        tables = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify_safe({"tables": tables})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

# ═══════════ API: TABELLEN-DATEN ═══════════
@app.route("/api/table/<table_name>")
def get_table_data(table_name):
    # Sicherheit
    if not table_name.replace("_", "").isalnum():
        return jsonify({"error": "Ungültiger Tabellenname"}), 400

    conn = get_db()
    if not conn:
        return jsonify({"error": "Datenbankverbindung fehlgeschlagen"}), 500

    try:
        cur = conn.cursor()

        # ── Parameter ──
        page      = max(1, int(request.args.get("page", 1)))
        page_size = min(500, max(1, int(request.args.get("page_size", 50))))
        search    = request.args.get("search", "").strip()
        date_from = request.args.get("date_from", "").strip()
        date_to   = request.args.get("date_to", "").strip()
        sort_col  = request.args.get("sort_col", "").strip()
        sort_dir  = request.args.get("sort_dir", "desc").upper()
        symbol_id = request.args.get("symbol_id", "").strip()
        endpoint  = request.args.get("endpoint", "").strip()
        http_st   = request.args.get("http_status", "").strip()

        if sort_dir not in ("ASC", "DESC"):
            sort_dir = "DESC"

        # ── Spalten-Info ──
        cur.execute("""
            SELECT
                c.column_name                                           AS name,
                c.data_type                                             AS dtype,
                c.is_nullable = 'YES'                                   AS nullable,
                EXISTS(
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage k
                      ON tc.constraint_name = k.constraint_name
                     AND tc.table_schema    = k.table_schema
                    WHERE tc.table_name       = c.table_name
                      AND tc.table_schema     = c.table_schema
                      AND k.column_name       = c.column_name
                      AND tc.constraint_type  = 'PRIMARY KEY'
                )                                                       AS is_pk,
                EXISTS(
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage k
                      ON tc.constraint_name = k.constraint_name
                     AND tc.table_schema    = k.table_schema
                    WHERE tc.table_name       = c.table_name
                      AND tc.table_schema     = c.table_schema
                      AND k.column_name       = c.column_name
                      AND tc.constraint_type  = 'FOREIGN KEY'
                )                                                       AS is_fk
            FROM information_schema.columns c
            WHERE c.table_name   = %s
              AND c.table_schema = 'public'
            ORDER BY c.ordinal_position
        """, (table_name,))
        columns = [dict(r) for r in cur.fetchall()]

        if not columns:
            cur.close()
            conn.close()
            return jsonify({"error": f"Tabelle '{table_name}' nicht gefunden"}), 404

        # ── Timestamp-Spalte erkennen ──
        ts_col = next((
            c["name"] for c in columns
            if "timestamp" in c["dtype"] or "time zone" in c["dtype"] or c["dtype"] == "date"
        ), None)

        # ── WHERE-Clause ──
        where, params = [], []

        if search:
            parts = [f"{c['name']}::text ILIKE %s" for c in columns]
            where.append(f"({' OR '.join(parts)})")
            params += [f"%{search}%"] * len(columns)

        if ts_col and date_from:
            where.append(f"{ts_col} >= %s::timestamp")
            params.append(date_from)

        if ts_col and date_to:
            where.append(f"{ts_col} < (%s::date + interval '1 day')::timestamp")
            params.append(date_to)

        if symbol_id and any(c["name"] == "symbol_id" for c in columns):
            where.append("symbol_id = %s")
            params.append(int(symbol_id))

        if endpoint and any(c["name"] == "endpoint" for c in columns):
            where.append("endpoint ILIKE %s")
            params.append(f"%{endpoint}%")

        if http_st and any(c["name"] == "http_status" for c in columns):
            where.append("http_status = %s")
            params.append(int(http_st))

        wc = f"WHERE {' AND '.join(where)}" if where else ""

        # ── ORDER BY ──
        col_names = [c["name"] for c in columns]
        if sort_col and sort_col in col_names:
            oc = f"ORDER BY {sort_col} {sort_dir}"
        elif ts_col:
            oc = f"ORDER BY {ts_col} DESC"
        else:
            pk = next((c["name"] for c in columns if c["is_pk"]), col_names[0])
            oc = f"ORDER BY {pk} DESC"

        # ── COUNT ──
        cur.execute(f"SELECT COUNT(*) AS n FROM {table_name} {wc}", params)
        total = cur.fetchone()["n"]

        # ── DATA ──
        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT * FROM {table_name} {wc} {oc} LIMIT %s OFFSET %s",
            params + [page_size, offset]
        )
        rows = [dict(r) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify_safe({
            "columns": columns,
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, (total + page_size - 1) // page_size),
        })

    except Exception as e:
        if conn:
            conn.close()
        print(f"❌ get_table_data({table_name}): {e}")
        return jsonify({"error": str(e)}), 500

# ═══════════ ERROR HANDLERS ═══════════
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint nicht gefunden"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Interner Serverfehler"}), 500

# ═══════════ MAIN ═══════════
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  Market Terminal Pro")
    print("=" * 60)

    idx = find_index()
    if idx:
        print(f"✅  index.html: {idx}")
    else:
        print("❌  index.html NICHT gefunden!")
        for p in INDEX_CANDIDATES:
            print(f"    – {p}")

    print(f"\n🗄️   DB: {DB_CONFIG['database']} @ {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    conn = get_db()
    if conn:
        print("✅  Datenbankverbindung OK")
        conn.close()
    else:
        print("❌  Datenbankverbindung FEHLGESCHLAGEN")

    print(f"\n🌐  http://0.0.0.0:8080")
    print("=" * 60)

    app.run(host="0.0.0.0", port=8080, debug=False)
