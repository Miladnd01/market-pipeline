from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Deine PostgreSQL Verbindung
DB_CONFIG = {
    'host': 'dpg-d6v9lh94tr6s73dgj93g-a.frankfurt-postgres.render.com',
    'database': 'marketdb_6mxq',
    'user': 'marketdb_6mxq_user',
    'password': 'gSbpVTiDKKo7YCrgLg3dHSipcpJpR9JF'
}

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

@app.route('/api/tables', methods=['GET'])
def get_tables():
    conn = get_db()
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
            obj_description((t.table_schema||'.'||t.table_name)::regclass) as description,
            (SELECT COUNT(*) FROM information_schema.columns c 
             WHERE c.table_name = t.table_name) as column_count,
            (SELECT reltuples::bigint FROM pg_class 
             WHERE oid = (t.table_schema||'.'||t.table_name)::regclass) as row_count
        FROM information_schema.tables t
        WHERE t.table_schema = 'public' 
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """)
    
    tables = cur.fetchall()
    conn.close()
    
    return jsonify({'tables': tables})

@app.route('/api/table/<table_name>', methods=['GET'])
def get_table_data(table_name):
    conn = get_db()
    cur = conn.cursor()
    
    # Parameters
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 50))
    search = request.args.get('search', '')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    sort_col = request.args.get('sort_col')
    sort_dir = request.args.get('sort_dir', 'desc').upper()
    
    # Spalten-Info
    cur.execute("""
        SELECT 
            c.column_name as name,
            c.data_type as dtype,
            c.is_nullable = 'YES' as nullable,
            EXISTS(
                SELECT 1 FROM information_schema.key_column_usage k
                WHERE k.table_name = c.table_name 
                AND k.column_name = c.column_name
                AND k.constraint_name LIKE 'pk_%'
            ) as is_pk,
            EXISTS(
                SELECT 1 FROM information_schema.key_column_usage k
                WHERE k.table_name = c.table_name 
                AND k.column_name = c.column_name
                AND k.constraint_name LIKE 'fk_%'
            ) as is_fk
        FROM information_schema.columns c
        WHERE c.table_name = %s
        ORDER BY c.ordinal_position
    """, (table_name,))
    
    columns = cur.fetchall()
    
    # Timestamp-Spalte finden
    timestamp_col = next((c['name'] for c in columns if 'timestamp' in c['dtype'] or 'date' in c['dtype']), None)
    
    # WHERE clause
    where_parts = []
    params = []
    
    if search:
        search_parts = []
        for col in columns:
            search_parts.append(f"{col['name']}::text ILIKE %s")
            params.append(f'%{search}%')
        where_parts.append(f"({' OR '.join(search_parts)})")
    
    if timestamp_col and date_from:
        where_parts.append(f"{timestamp_col} >= %s")
        params.append(date_from)
    
    if timestamp_col and date_to:
        where_parts.append(f"{timestamp_col} <= %s")
        params.append(date_to + ' 23:59:59')
    
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    
    # ORDER BY - Standard: ID DESC für timestamps
    if not sort_col and timestamp_col:
        sort_col = timestamp_col
        sort_dir = 'DESC'
    elif not sort_col:
        pk_col = next((c['name'] for c in columns if c['is_pk']), columns[0]['name'])
        sort_col = pk_col
        sort_dir = 'DESC'
    
    order_clause = f"ORDER BY {sort_col} {sort_dir}"
    
    # Count
    count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
    cur.execute(count_query, params)
    total = cur.fetchone()['total']
    
    # Data
    offset = (page - 1) * page_size
    data_query = f"""
        SELECT * FROM {table_name} 
        {where_clause} 
        {order_clause} 
        LIMIT %s OFFSET %s
    """
    cur.execute(data_query, params + [page_size, offset])
    rows = cur.fetchall()
    
    # JSON serialization fix für datetime
    for row in rows:
        for key, val in row.items():
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    
    conn.close()
    
    return jsonify({
        'columns': columns,
        'rows': rows,
        'total': total,
        'page': page,
        'page_size': page_size
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
