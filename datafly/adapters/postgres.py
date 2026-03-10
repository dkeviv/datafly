"""
PostgreSQL adapter — uses information_schema + pg_stat_statements
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str):
        super().__init__(connection_string, name)
        self._conn = None

    def connect(self) -> None:
        import psycopg2
        import psycopg2.extras
        self._conn = psycopg2.connect(self.connection_string)
        self._conn.autocommit = True
        logger.info(f"[{self.name}] Connected to Postgres")

    def introspect_schema(self) -> dict:
        cursor = self._conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor)

        # Tables + columns
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
        rows = cursor.fetchall()

        # Foreign keys
        cursor.execute("""
            SELECT
                kcu.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
        """)
        fk_rows = cursor.fetchall()

        # Row count estimates
        cursor.execute("""
            SELECT relname, reltuples::bigint AS row_estimate
            FROM pg_class
            WHERE relkind = 'r'
        """)
        counts = {r['relname']: r['row_estimate'] for r in cursor.fetchall()}

        # Build normalized schema
        tables = {}
        for row in rows:
            tname = row['table_name']
            ttype = 'views' if row['table_type'] == 'VIEW' else 'tables'
            if tname not in tables:
                tables[tname] = {
                    "_type": ttype,
                    "columns": [],
                    "row_count_estimate": counts.get(tname, 0),
                    "primary_key": None,
                    "foreign_keys": []
                }
            tables[tname]["columns"].append({
                "name": row['column_name'],
                "type": row['data_type'],
                "nullable": row['is_nullable'] == 'YES'
            })

        for fk in fk_rows:
            tname = fk['table_name']
            if tname in tables:
                tables[tname]["foreign_keys"].append({
                    "column": fk['column_name'],
                    "references": f"{fk['foreign_table']}.{fk['foreign_column']}"
                })

        return {
            "adapter": self.name,
            "adapter_type": "postgres",
            "tables": {k: v for k, v in tables.items() if v['_type'] == 'tables'},
            "views": {k: v for k, v in tables.items() if v['_type'] == 'views'},
        }

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """Mine pg_stat_statements if available."""
        cursor = self._conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor)
        try:
            cursor.execute(f"""
                SELECT 
                    query,
                    calls AS execution_count,
                    mean_exec_time AS avg_duration_ms,
                    last_value AS last_run
                FROM pg_stat_statements
                WHERE query NOT LIKE '%pg_stat%'
                ORDER BY calls DESC
                LIMIT {limit}
            """)
            return [dict(r) for r in cursor.fetchall()]
        except Exception:
            logger.warning(f"[{self.name}] pg_stat_statements not available — skipping query history")
            return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        cursor = self._conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor)
        cursor.execute(query, params or {})
        return [dict(r) for r in cursor.fetchall()]
