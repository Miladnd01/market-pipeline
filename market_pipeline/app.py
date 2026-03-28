import os
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "database": os.environ.get("DB_NAME", "market_db"),
    "user": os.environ.get("DB_USER", "your_user"),
    "password": os.environ.get("DB_PASSWORD", "your_password"),
    "port": int(os.environ.get("DB_PORT", 5432)),
}

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "message": "Market API läuft",
        "status": "ok"
    })

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/api/tables", methods=["GET"])
def get_tables():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            t.table_name as name,
            CASE 
                WHEN t.table_name LIKE 'dim_%' THEN 'dim'
                WHEN t.table_name LIKE 'fact_%' THEN 'fact'
                WHEN t.table_name LIKE 'log_%' THEN 'log'
                ELSE 'other'
            END as type,
            obj_description((t.table_schema||'.'||t.table_name)::regclass) as description,
            (
                SELECT COUNT(*) 
                FROM information_schema.columns c 
                WHERE c.table_name = t.table_name
                  AND c.table_schema = t.table_schema
            ) as column_count,
            (
                SELECT reltuples::bigint 
                FROM pg_class 
                WHERE oid = (t.table_schema||'.'||t.table_name)::regclass
            ) as row_count
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """)

    tables = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify({"tables": tables})

@app.route("/api/table/<table_name>", methods=["GET"])
def get_table_data(table_name):
    conn = get_db()
    cur = conn.cursor()

    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 50))
    search = request.args.get("search", "")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    sort_col = request.args.get("sort_col")
    sort_dir = request.args.get("sort_dir", "desc").upper()

    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        ) as exists
    """, (table_name,))
    exists = cur.fetchone()["exists"]

    if not exists:
        cur.close()
        conn.close()
        return jsonify({"error": "Table not found"}), 404

    cur.execute("""
        SELECT 
            c.column_name as name,
            c.data_type as dtype,
            c.is_nullable = 'YES' as nullable
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = %s
        ORDER BY c.ordinal_position
    """, (table_name,))
    columns = cur.fetchall()

    column_names = [c["name"] for c in columns]

    timestamp_col = next(
        (c["name"] for c in columns if "timestamp" in c["dtype"] or "date" in c["dtype"]),
        None
    )

    where_parts = []
    params = []

    if search:
        search_parts = []
        for col in column_names:
            search_parts.append(f'"{col}"::text ILIKE %s')
            params.append(f"%{search}%")
        where_parts.append("(" + " OR ".join(search_parts) + ")")

    if timestamp_col and date_from:
        where_parts.append(f'"{timestamp_col}" >= %s')
        params.append(date_from)

    if timestamp_col and date_to:
        where_parts.append(f'"{timestamp_col}" <= %s')
        params.append(date_to + " 23:59:59")

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    if not sort_col or sort_col not in column_names:
        sort_col = timestamp_col if timestamp_col else column_names[0]

    if sort_dir not in ("ASC", "DESC"):
        sort_dir = "DESC"

    count_query = f'SELECT COUNT(*) as total FROM "{table_name}" {where_clause}'
    cur.execute(count_query, params)
    total = cur.fetchone()["total"]

    offset = (page - 1) * page_size

    data_query = f'''
        SELECT * FROM "{table_name}"
        {where_clause}
        ORDER BY "{sort_col}" {sort_dir}
        LIMIT %s OFFSET %s
    '''
    cur.execute(data_query, params + [page_size, offset])
    rows = cur.fetchall()

    for row in rows:
        for key, val in row.items():
            if isinstance(val, datetime):
                row[key] = val.isoformat()

    cur.close()
    conn.close()

    return jsonify({
        "columns": columns,
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
