import os
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, Response
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "database": os.environ.get("DB_NAME", "market_db"),
    "user": os.environ.get("DB_USER", "your_user"),
    "password": os.environ.get("DB_PASSWORD", "your_password"),
    "port": int(os.environ.get("DB_PORT", 5432)),
}

INDEX_CANDIDATES = [
    BASE_DIR / "index.html",
    BASE_DIR / "static" / "index.html",
]

def find_index_file():
    for path in INDEX_CANDIDATES:
        if path.exists():
            return path
    return None

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def serialize_rows(rows):
    for row in rows:
        for key, val in row.items():
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    return rows

@app.route("/", methods=["GET"])
def home():
    index_path = find_index_file()
    if index_path is None:
        return jsonify({"error": "index.html nicht gefunden"}), 500

    html = index_path.read_text(encoding="utf-8")
    html = html.replace(
        "const API_BASE = 'http://localhost:5000/api';",
        "const API_BASE = '/api';"
    )
    return Response(html, mimetype="text/html")

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "db_host": DB_CONFIG["host"],
        "db_name": DB_CONFIG["database"],
        "db_user": DB_CONFIG["user"],
        "db_port": DB_CONFIG["port"]
    })

@app.route("/api/tables", methods=["GET"])
def get_tables():
    conn = None
    cur = None

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                t.table_name AS name,
                CASE 
                    WHEN t.table_name LIKE 'dim_%' THEN 'dim'
                    WHEN t.table_name LIKE 'fact_%' THEN 'fact'
                    WHEN t.table_name LIKE 'log_%' THEN 'log'
                    ELSE 'other'
                END AS type,
                obj_description((t.table_schema||'.'||t.table_name)::regclass) AS description,
                (
                    SELECT COUNT(*)
                    FROM information_schema.columns c
                    WHERE c.table_schema = t.table_schema
                      AND c.table_name = t.table_name
                ) AS column_count,
                COALESCE((
                    SELECT reltuples::bigint
                    FROM pg_class
                    WHERE oid = (t.table_schema||'.'||t.table_name)::regclass
                ), 0) AS row_count
            FROM information_schema.tables t
            WHERE t.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """)

        tables = cur.fetchall()
        return jsonify({"tables": tables})

    except Exception as e:
        return jsonify({
            "error": "Fehler beim Laden der Tabellen",
            "details": str(e),
            "db_host": DB_CONFIG["host"],
            "db_name": DB_CONFIG["database"],
            "db_user": DB_CONFIG["user"],
            "db_port": DB_CONFIG["port"]
        }), 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.route("/api/table/<table_name>", methods=["GET"])
def get_table_data(table_name):
    conn = None
    cur = None

    try:
        conn = get_db()
        cur = conn.cursor()

        page = max(int(request.args.get("page", 1)), 1)
        page_size = min(max(int(request.args.get("page_size", 50)), 1), 500)
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        sort_col = request.args.get("sort_col")
        sort_dir = request.args.get("sort_dir", "desc").lower()

        if sort_dir not in ("asc", "desc"):
            sort_dir = "desc"

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
            return jsonify({"error": "Table not found"}), 404

        cur.execute("""
            SELECT
                c.column_name AS name,
                c.data_type AS dtype,
                (c.is_nullable = 'YES') AS nullable,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = c.table_schema
                      AND tc.table_name = c.table_name
                      AND tc.constraint_type = 'PRIMARY KEY'
                      AND kcu.column_name = c.column_name
                ) AS is_pk,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = c.table_schema
                      AND tc.table_name = c.table_name
                      AND tc.constraint_type = 'FOREIGN KEY'
                      AND kcu.column_name = c.column_name
                ) AS is_fk
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (table_name,))

        columns = cur.fetchall()
        if not columns:
            return jsonify({"error": "No columns found"}), 404

        column_names = [c["name"] for c in columns]

        timestamp_col = next(
            (
                c["name"] for c in columns
                if "timestamp" in c["dtype"].lower() or c["dtype"].lower() == "date"
            ),
            None
        )

        pk_col = next((c["name"] for c in columns if c["is_pk"]), None)

        where_clauses = []
        params = []

        if timestamp_col and date_from:
            where_clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(timestamp_col)))
            params.append(date_from)

        if timestamp_col and date_to:
            where_clauses.append(sql.SQL("{} <= %s").format(sql.Identifier(timestamp_col)))
            params.append(date_to + " 23:59:59")

        where_sql = sql.SQL("")
        if where_clauses:
            where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)

        if not sort_col or sort_col not in column_names:
            if timestamp_col:
                sort_col = timestamp_col
                sort_dir = "desc"
            elif pk_col:
                sort_col = pk_col
                sort_dir = "desc"
            else:
                sort_col = column_names[0]
                sort_dir = "desc"

        count_query = sql.SQL("SELECT COUNT(*) AS total FROM {}{}").format(
            sql.Identifier(table_name),
            where_sql
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
            where_sql,
            sql.Identifier(sort_col),
            sql.SQL(sort_dir.upper())
        )

        cur.execute(data_query, params + [page_size, offset])
        rows = serialize_rows(cur.fetchall())

        return jsonify({
            "columns": columns,
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size
        })

    except Exception as e:
        return jsonify({
            "error": "Fehler beim Laden der Tabellendaten",
            "details": str(e)
        }), 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
