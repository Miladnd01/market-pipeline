from pathlib import Path
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# ═══════════ FILE SERVING ═══════════
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

# ═══════════ HEALTH CHECKS ═══════════
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/health", methods=["GET"])
def health_simple():
    return "ok", 200

# ═══════════ DATABASE CONFIG ═══════════
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'market_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'port': os.getenv('DB_PORT', 5432)
}

def get_db():
    """Erstellt Datenbankverbindung"""
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

# ═══════════ API: TABELLEN LISTE ═══════════
@app.route('/api/tables', methods=['GET'])
def get_tables():
    """Gibt alle Tabellen mit Metadaten zurück"""
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Datenbankverbindung fehlgeschlagen'}), 500
    
    try:
        cur = conn.cursor()
        
        # Alle Tabellen mit Metadata
        cur.execute("""
            SELECT 
                t.table_name as name,
                CASE 
                    WHEN t.table_name LIKE 'dim_%' THEN 'dim'
                    WHEN t.table_name LIKE 'fact_%' THEN 'fact'
                    WHEN t.table_name LIKE 'log_%' THEN 'log'
                    ELSE 'other'
                END as type,
                obj_description((quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass) as description,
                (SELECT COUNT(*) FROM information_schema.columns c 
                 WHERE c.table_name = t.table_name 
                 AND c.table_schema = t.table_schema) as column_count,
                COALESCE(
                    (SELECT reltuples::bigint FROM pg_class 
                     WHERE oid = (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass),
                    0
                ) as row_count
            FROM information_schema.tables t
            WHERE t.table_schema = 'public' 
            AND t.table_type = 'BASE TABLE'
            ORDER BY 
                CASE 
                    WHEN t.table_name LIKE 'dim_%' THEN 1
                    WHEN t.table_name LIKE 'fact_%' THEN 2
                    WHEN t.table_name LIKE 'log_%' THEN 3
                    ELSE 4
                END,
                t.table_name
        """)
        
        tables = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({'tables': tables})
    
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

# ═══════════ API: TABELLEN-DATEN ═══════════
@app.route('/api/table/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """Gibt Daten einer Tabelle zurück mit Pagination, Filter, Sort"""
    
    # Sicherheit: Nur alphanumerische Zeichen + Unterstriche
    if not table_name.replace('_', '').isalnum():
        return jsonify({'error': 'Ungültiger Tabellenname'}), 400
    
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Datenbankverbindung fehlgeschlagen'}), 500
    
    try:
        cur = conn.cursor()
        
        # ─── Parameter ───
        page = int(request.args.get('page', 1))
        page_size = min(int(request.args.get('page_size', 50)), 500)  # Max 500
        search = request.args.get('search', '').strip()
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        sort_col = request.args.get('sort_col', '').strip()
        sort_dir = request.args.get('sort_dir', 'desc').upper()
        
        # Validierung sort_dir
        if sort_dir not in ['ASC', 'DESC']:
            sort_dir = 'DESC'
        
        # ─── Spalten-Info ───
        cur.execute("""
            SELECT 
                c.column_name as name,
                c.data_type as dtype,
                c.is_nullable = 'YES' as nullable,
                EXISTS(
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage k 
                        ON tc.constraint_name = k.constraint_name
                    WHERE tc.table_name = c.table_name 
                    AND k.column_name = c.column_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                ) as is_pk,
                EXISTS(
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage k 
                        ON tc.constraint_name = k.constraint_name
                    WHERE tc.table_name = c.table_name 
                    AND k.column_name = c.column_name
                    AND tc.constraint_type = 'FOREIGN KEY'
                ) as is_fk
            FROM information_schema.columns c
            WHERE c.table_name = %s
            AND c.table_schema = 'public'
            ORDER BY c.ordinal_position
        """, (table_name,))
        
        columns = cur.fetchall()
        
        if not columns:
            cur.close()
            conn.close()
            return jsonify({'error': f'Tabelle {table_name} nicht gefunden'}), 404
        
        # ─── Timestamp-Spalte finden ───
        timestamp_cols = [
            c['name'] for c in columns 
            if 'timestamp' in c['dtype'].lower() 
            or 'date' in c['dtype'].lower()
        ]
        timestamp_col = timestamp_cols[0] if timestamp_cols else None
        
        # ─── WHERE Clause bauen ───
        where_parts = []
        params = []
        
        # Suche über alle Spalten
        if search:
            search_parts = []
            for col in columns:
                search_parts.append(f"{col['name']}::text ILIKE %s")
                params.append(f'%{search}%')
            where_parts.append(f"({' OR '.join(search_parts)})")
        
        # Datum-Filter
        if timestamp_col and date_from:
            where_parts.append(f"{timestamp_col} >= %s::timestamp")
            params.append(date_from)
        
        if timestamp_col and date_to:
            where_parts.append(f"{timestamp_col} <= %s::timestamp + interval '1 day'")
            params.append(date_to)
        
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        
        # ─── ORDER BY ───
        # Standard: Timestamp DESC oder PK DESC
        if sort_col and sort_col in [c['name'] for c in columns]:
            order_clause = f"ORDER BY {sort_col} {sort_dir}"
        elif timestamp_col:
            order_clause = f"ORDER BY {timestamp_col} DESC"
        else:
            pk_col = next((c['name'] for c in columns if c['is_pk']), columns[0]['name'])
            order_clause = f"ORDER BY {pk_col} DESC"
        
        # ─── COUNT ───
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        # ─── DATA ───
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT * FROM {table_name} 
            {where_clause} 
            {order_clause} 
            LIMIT %s OFFSET %s
        """
        cur.execute(data_query, params + [page_size, offset])
        rows = cur.fetchall()
        
        # ─── JSON Serialization Fix ───
        for row in rows:
            for key, val in list(row.items()):
                if isinstance(val, datetime):
                    row[key] = val.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(val, (bytes, bytearray)):
                    row[key] = val.hex()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'columns': columns,
            'rows': rows,
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': (total + page_size - 1) // page_size
        })
    
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500

# ═══════════ ERROR HANDLERS ═══════════
@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Endpoint nicht gefunden'}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Interner Serverfehler'}), 500

# ═══════════ MAIN ═══════════
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Market Terminal Pro Server")
    print("=" * 60)
    print(f"📂 BASE_DIR: {BASE_DIR}")
    
    index_path = find_index_file()
    if index_path:
        print(f"✅ index.html gefunden: {index_path}")
    else:
        print(f"❌ index.html NICHT gefunden in:")
        for p in INDEX_CANDIDATES:
            print(f"   - {p}")
    
    print(f"\n🗄️  Datenbank: {DB_CONFIG['database']} @ {DB_CONFIG['host']}")
    
    # Test DB Connection
    test_conn = get_db()
    if test_conn:
        print("✅ Datenbankverbindung erfolgreich")
        test_conn.close()
    else:
        print("❌ Datenbankverbindung fehlgeschlagen")
    
    print(f"\n🌐 Server läuft auf: http://0.0.0.0:8080")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=8080, debug=True)
