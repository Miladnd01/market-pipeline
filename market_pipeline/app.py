import os
from urllib.parse import urlparse

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from datetime import datetime

app = Flask(__name__)
CORS(app)

def get_db():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

def get_table_columns(cur, table_name):
    cur.execute("""
        SELECT 
            c.column_name as name,
            c.data_type as dtype,
            c.is_nullable = 'YES' as nullable,
            EXISTS(
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = c.table_name
                  AND tc.table_schema = c.table_schema
                  AND kcu.column_name = c.column_name
                  AND tc.constraint_type = 'PRIMARY KEY'
            ) as is_pk,
            EXISTS(
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = c.table_name
                  AND tc.table_schema = c.table_schema
                  AND kcu.column_name = c.column_name
                  AND tc.constraint_type = 'FOREIGN KEY'
            ) as is_fk
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = %s
        ORDER BY c.ordinal_position
    """, (table_name,))
    return cur.fetchall()

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"ok": True})

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
                WHERE c.table_schema = t.table_schema
                  AND c.table_name = t.table_name
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

    # Prüfen, ob Tabelle wirklich existiert
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        ) AS exists
    """, (table_name,))
    exists = cur.fetchone()["exists"]

    if not exists:
        cur.close()
        conn.close()
        return jsonify({"error": "Table not found"}), 404

    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 50)), 1), 200)
    search = request.args.get("search", "").strip()
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    sort_col = request.args.get("sort_col")
    sort_dir = request.args.get("sort_dir", "desc").lower()

    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    columns = get_table_columns(cur, table_name)
    column_names = [c["name"] for c in columns]

    # sichere timestamp/date-Spalte finden
    timestamp_col = next(
        (c["name"] for c in columns if "timestamp" in c["dtype"] or "date" in c["dtype"]),
        None
    )

    where_parts = []
    params = []

    if search:
        search_parts = []
        for col in column_names:
            search_parts.append(sql.SQL("{}::text ILIKE %s").format(sql.Identifier(col)))
            params.append(f"%{search}%")
        where_parts.append(sql.SQL("(") + sql.SQL(" OR ").join(search_parts) + sql.SQL(")"))

    if timestamp_col and date_from:
        where_parts.append(sql.SQL("{} >= %s").format(sql.Identifier(timestamp_col)))
        params.append(date_from)

    if timestamp_col and date_to:
        where_parts.append(sql.SQL("{} <= %s").format(sql.Identifier(timestamp_col)))
        params.append(date_to + " 23:59:59")

    where_clause = sql.SQL("")
    if where_parts:
        where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_parts)

    if not sort_col or sort_col not in column_names:
        if timestamp_col:
            sort_col = timestamp_col
            sort_dir = "desc"
        else:
            pk_col = next((c["name"] for c in columns if c["is_pk"]), column_names[0])
            sort_col = pk_col
            sort_dir = "desc"

    count_query = sql.SQL("SELECT COUNT(*) AS total FROM {}{}").format(
        sql.Identifier(table_name),
        where_clause
    )
    cur.execute(count_query, params)
    total = cur.fetchone()["total"]

    offset = (page - 1) * page_size

    data_query = sql.SQL("""
        SELECT *
        FROM {}{}
        ORDER BY {} {}
        LIMIT %s OFFSET %s
    """).format(
        sql.Identifier(table_name),
        where_clause,
        sql.Identifier(sort_col),
        sql.SQL(sort_dir.upper())
    )

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
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
