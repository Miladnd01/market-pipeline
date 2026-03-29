import os
import threading
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

from db.connection import get_connection, create_schema
import main as pipeline_main

app = Flask(__name__, template_folder="templates")
CORS(app)

pipeline_thread = None
pipeline_started = False
pipeline_status = {
    "running": False,
    "phase": "init",
    "last_error": None,
    "last_cycle_at": None,
    "last_successful_cycle_at": None,
    "last_cycle_result": None,
    "current_symbol": None,
    "current_symbol_index": None,
    "symbols_total": None,
    "cycle_num": 0,
    "next_cycle_num": None,
    "sleep_seconds": None,
    "started_at": None,
    "thread_alive": False,
}

PIPELINE_LOCK = threading.Lock()


def update_status(**kwargs):
    global pipeline_status
    pipeline_status.update(kwargs)
    if pipeline_thread is not None:
        pipeline_status["thread_alive"] = pipeline_thread.is_alive()


def pipeline_worker():
    global pipeline_status
    try:
        update_status(
            running=True,
            phase="thread_boot",
            started_at=pipeline_main.now_berlin_iso(),
            thread_alive=True,
            last_error=None
        )
        pipeline_main.main(status_callback=update_status)
    except Exception as e:
        update_status(
            running=False,
            phase="crashed",
            last_error=str(e),
            thread_alive=False
        )


def ensure_pipeline_started():
    global pipeline_thread, pipeline_started

    with PIPELINE_LOCK:
        if pipeline_started and pipeline_thread and pipeline_thread.is_alive():
            return

        create_schema()

        pipeline_thread = threading.Thread(target=pipeline_worker, daemon=True)
        pipeline_thread.start()
        pipeline_started = True

        update_status(
            running=True,
            phase="starting",
            thread_alive=True
        )


@app.route("/")
def index():
    ensure_pipeline_started()
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "pipeline": pipeline_status
    })


@app.route("/api/status")
def api_status():
    if pipeline_thread is not None:
        pipeline_status["thread_alive"] = pipeline_thread.is_alive()
    return jsonify(pipeline_status)


@app.route("/api/tables")
def api_tables():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        t.table_name,
                        COALESCE(c.column_count, 0) AS column_count,
                        COALESCE(s.row_count, 0) AS row_count
                    FROM information_schema.tables t
                    LEFT JOIN (
                        SELECT table_name, COUNT(*) AS column_count
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        GROUP BY table_name
                    ) c ON c.table_name = t.table_name
                    LEFT JOIN (
                        SELECT 'dim_source' AS table_name, COUNT(*)::bigint AS row_count FROM dim_source
                        UNION ALL
                        SELECT 'dim_symbol', COUNT(*)::bigint FROM dim_symbol
                        UNION ALL
                        SELECT 'dim_interval', COUNT(*)::bigint FROM dim_interval
                        UNION ALL
                        SELECT 'dim_indicator', COUNT(*)::bigint FROM dim_indicator
                        UNION ALL
                        SELECT 'fact_company_fundamental', COUNT(*)::bigint FROM fact_company_fundamental
                        UNION ALL
                        SELECT 'fact_earnings_calendar', COUNT(*)::bigint FROM fact_earnings_calendar
                        UNION ALL
                        SELECT 'fact_market_indicator', COUNT(*)::bigint FROM fact_market_indicator
                        UNION ALL
                        SELECT 'fact_market_quote', COUNT(*)::bigint FROM fact_market_quote
                        UNION ALL
                        SELECT 'fact_market_timeseries', COUNT(*)::bigint FROM fact_market_timeseries
                        UNION ALL
                        SELECT 'log_api_call', COUNT(*)::bigint FROM log_api_call
                    ) s ON s.table_name = t.table_name
                    WHERE t.table_schema = 'public'
                    AND t.table_type = 'BASE TABLE'
                    ORDER BY t.table_name
                """)
                rows = cur.fetchall()

        tables = []
        for name, column_count, row_count in rows:
            if name.startswith("dim_"):
                ttype = "dim"
            elif name.startswith("log_"):
                ttype = "log"
            else:
                ttype = "fact"

            tables.append({
                "name": name,
                "type": ttype,
                "column_count": int(column_count or 0),
                "row_count": int(row_count or 0),
                "description": ""
            })

        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e), "tables": []}), 500


def get_table_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                c.column_name,
                c.data_type,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = 'public'
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
                    WHERE tc.table_schema = 'public'
                      AND tc.table_name = c.table_name
                      AND tc.constraint_type = 'FOREIGN KEY'
                      AND kcu.column_name = c.column_name
                ) AS is_fk
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (table_name,))
        return [
            {
                "name": r[0],
                "dtype": r[1],
                "is_pk": r[2],
                "is_fk": r[3],
            }
            for r in cur.fetchall()
        ]


@app.route("/api/table/<table_name>")
def api_table(table_name):
    allowed = {
        "dim_source", "dim_symbol", "dim_interval", "dim_indicator",
        "fact_company_fundamental", "fact_earnings_calendar",
        "fact_market_indicator", "fact_market_quote",
        "fact_market_timeseries", "log_api_call"
    }

    if table_name not in allowed:
        return jsonify({"error": "table not allowed"}), 400

    page = max(int(request.args.get("page", 1)), 1)
    page_size = min(max(int(request.args.get("page_size", 50)), 1), 200)
    offset = (page - 1) * page_size

    search = request.args.get("search", "").strip()
    sort_col = request.args.get("sort_col", "").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip().lower()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    symbol_id = request.args.get("symbol_id", "").strip()
    endpoint = request.args.get("endpoint", "").strip()
    http_status = request.args.get("http_status", "").strip()

    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    with get_connection() as conn:
        columns = get_table_columns(conn, table_name)
        col_names = [c["name"] for c in columns]

        where = []
        params = []

        if search:
            parts = []
            for c in columns:
                parts.append(f"CAST({c['name']} AS TEXT) ILIKE %s")
                params.append(f"%{search}%")
            where.append("(" + " OR ".join(parts) + ")")

        time_col = None
        for candidate in ["called_at_utc", "fetched_at_utc", "candle_time_utc", "quote_time_utc", "report_date"]:
            if candidate in col_names:
                time_col = candidate
                break

        if date_from and time_col:
            where.append(f"{time_col} >= %s")
            params.append(date_from)

        if date_to and time_col:
            where.append(f"{time_col} <= %s")
            params.append(date_to + " 23:59:59")

        if symbol_id and "symbol_id" in col_names:
            where.append("symbol_id = %s")
            params.append(symbol_id)

        if endpoint and "endpoint" in col_names:
            where.append("endpoint ILIKE %s")
            params.append(f"%{endpoint}%")

        if http_status and "http_status" in col_names:
            where.append("http_status = %s")
            params.append(http_status)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        if not sort_col or sort_col not in col_names:
            if "log_id" in col_names:
                sort_col = "log_id"
            elif "quote_id" in col_names:
                sort_col = "quote_id"
            elif "indicator_fact_id" in col_names:
                sort_col = "indicator_fact_id"
            elif "timeseries_id" in col_names:
                sort_col = "timeseries_id"
            elif "fundamental_id" in col_names:
                sort_col = "fundamental_id"
            elif "earnings_id" in col_names:
                sort_col = "earnings_id"
            else:
                sort_col = col_names[0]

        count_sql = f"SELECT COUNT(*) FROM {table_name}{where_sql}"
        data_sql = f"""
            SELECT * FROM {table_name}
            {where_sql}
            ORDER BY {sort_col} {sort_dir}
            LIMIT %s OFFSET %s
        """

        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()[0]

            cur.execute(data_sql, params + [page_size, offset])
            rows = cur.fetchall()

        result_rows = []
        for row in rows:
            item = {}
            for i, col in enumerate(columns):
                val = row[i]
                if hasattr(val, "isoformat"):
                    item[col["name"]] = val.isoformat()
                else:
                    item[col["name"]] = val
            result_rows.append(item)

    total_pages = max(1, (total + page_size - 1) // page_size)

    return jsonify({
        "columns": columns,
        "rows": result_rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages
    })


if __name__ == "__main__":
    ensure_pipeline_started()
    app.run(host="0.0.0.0", port=8080, debug=False)
