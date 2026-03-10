"""
Redshift adapter — uses information_schema + STL_QUERY for history.
Redshift is Postgres-compatible so much of the introspection is similar,
but STL_QUERY gives us rich execution history with actual table usage.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class RedshiftAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str):
        super().__init__(connection_string, name)
        self._conn = None

    def connect(self) -> None:
        # redshift_connector is preferred; fall back to psycopg2
        try:
            import redshift_connector
            from urllib.parse import urlparse
            parsed = urlparse(self.connection_string.replace("redshift://", "postgresql://"))
            self._conn = redshift_connector.connect(
                host=parsed.hostname,
                database=parsed.path.strip("/"),
                user=parsed.username,
                password=parsed.password,
                port=parsed.port or 5439,
            )
            self._driver = "redshift_connector"
        except ImportError:
            import psycopg2
            conn_str = self.connection_string.replace("redshift://", "postgresql://")
            self._conn = psycopg2.connect(conn_str)
            self._driver = "psycopg2"

        logger.info(f"[{self.name}] Connected to Redshift via {self._driver}")

    def _cursor(self):
        if self._driver == "redshift_connector":
            return self._conn.cursor()
        else:
            import psycopg2.extras
            return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _fetchall_as_dicts(self, cursor) -> list[dict]:
        if self._driver == "redshift_connector":
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        return [dict(r) for r in cursor.fetchall()]

    def introspect_schema(self) -> dict:
        cursor = self._cursor()

        # Tables + columns — Redshift adds encoding and distkey info
        cursor.execute("""
            SELECT
                t.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default
            FROM information_schema.tables t
            JOIN information_schema.columns c
                ON t.table_name = c.table_name
                AND t.table_schema = c.table_schema
            WHERE t.table_schema = 'public'
            ORDER BY t.table_name, c.ordinal_position
        """)
        rows = self._fetchall_as_dicts(cursor)

        # Distribution and sort keys — important for performance context
        cursor.execute("""
            SELECT
                tablename,
                "column" AS column_name,
                distkey,
                sortkey
            FROM pg_table_def
            WHERE schemaname = 'public'
            AND (distkey = true OR sortkey != 0)
        """)
        key_rows = self._fetchall_as_dicts(cursor)
        dist_keys: dict[str, str] = {}
        sort_keys: dict[str, list] = {}
        for r in key_rows:
            if r.get('distkey'):
                dist_keys[r['tablename']] = r['column_name']
            if r.get('sortkey', 0):
                sort_keys.setdefault(r['tablename'], []).append(r['column_name'])

        # Row counts from SVV_TABLE_INFO
        cursor.execute("""
            SELECT "table", tbl_rows
            FROM svv_table_info
            WHERE schema = 'public'
        """)
        counts = {r['table']: r['tbl_rows'] for r in self._fetchall_as_dicts(cursor)}

        tables: dict = {}
        views: dict = {}
        for row in rows:
            tname = row['table_name']
            is_view = row.get('table_type') == 'VIEW'
            bucket = views if is_view else tables

            if tname not in bucket:
                bucket[tname] = {
                    "columns": [],
                    "row_count_estimate": int(counts.get(tname) or 0),
                    "primary_key": None,
                    "foreign_keys": [],
                    "dist_key": dist_keys.get(tname),
                    "sort_keys": sort_keys.get(tname, [])
                }
            bucket[tname]["columns"].append({
                "name": row['column_name'],
                "type": row['data_type'],
                "nullable": row.get('is_nullable') == 'YES'
            })

        return {
            "adapter": self.name,
            "adapter_type": "redshift",
            "tables": tables,
            "views": views,
        }

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """
        STL_QUERY contains actual executed queries with timing.
        STL_SCAN can show which tables were accessed per query.
        """
        cursor = self._cursor()
        try:
            cursor.execute(f"""
                SELECT
                    q.querytxt AS query,
                    q.elapsed / 1000.0 AS avg_duration_ms,
                    q.starttime AS last_run,
                    u.usename AS user_name
                FROM stl_query q
                LEFT JOIN pg_user u ON q.userid = u.usesysid
                WHERE q.aborted = 0
                AND q.querytxt NOT ILIKE '%stl_query%'
                AND q.querytxt ILIKE 'SELECT%'
                ORDER BY q.starttime DESC
                LIMIT {limit}
            """)
            rows = self._fetchall_as_dicts(cursor)
            return [
                {
                    "query": r.get('query', ''),
                    "execution_count": 1,
                    "avg_duration_ms": float(r.get('avg_duration_ms') or 0),
                    "last_run": str(r.get('last_run', '')),
                    "user": r.get('user_name', '')
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"[{self.name}] Could not fetch Redshift query history: {e}")
            return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        cursor = self._cursor()
        cursor.execute(query, params or {})
        return self._fetchall_as_dicts(cursor)
