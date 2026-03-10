"""
Snowflake adapter — uses INFORMATION_SCHEMA + QUERY_HISTORY
Snowflake's QUERY_HISTORY is gold for context mining: full SQL, execution counts, users.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class SnowflakeAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str):
        super().__init__(connection_string, name)
        self._conn = None
        self._database = None
        self._schema = "PUBLIC"

    def connect(self) -> None:
        import snowflake.connector
        # Parse: snowflake://user:pass@account/database/schema?warehouse=WH
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.connection_string)
        qs = parse_qs(parsed.query)

        path_parts = parsed.path.strip("/").split("/")
        self._database = path_parts[0] if len(path_parts) > 0 else None
        self._schema = path_parts[1] if len(path_parts) > 1 else "PUBLIC"

        self._conn = snowflake.connector.connect(
            user=parsed.username,
            password=parsed.password,
            account=parsed.hostname,
            database=self._database,
            schema=self._schema,
            warehouse=qs.get("warehouse", [None])[0],
        )
        logger.info(f"[{self.name}] Connected to Snowflake: {self._database}.{self._schema}")

    def introspect_schema(self) -> dict:
        import snowflake.connector
        cursor = self._conn.cursor(snowflake.connector.DictCursor)

        # Tables
        cursor.execute(f"""
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                BYTES
            FROM {self._database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self._schema}'
            ORDER BY TABLE_NAME
        """)
        tables_meta = {r['TABLE_NAME']: r for r in cursor.fetchall()}

        # Columns
        cursor.execute(f"""
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COMMENT
            FROM {self._database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self._schema}'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """)
        col_rows = cursor.fetchall()

        # Primary keys via SHOW PRIMARY KEYS
        pk_map: dict[str, str] = {}
        try:
            cursor.execute(f"SHOW PRIMARY KEYS IN SCHEMA {self._database}.{self._schema}")
            for r in cursor.fetchall():
                pk_map[r['table_name']] = r['column_name']
        except Exception:
            pass

        # Foreign keys
        fk_map: dict[str, list] = {}
        try:
            cursor.execute(f"SHOW IMPORTED KEYS IN SCHEMA {self._database}.{self._schema}")
            for r in cursor.fetchall():
                tname = r['fk_table_name']
                fk_map.setdefault(tname, []).append({
                    "column": r['fk_column_name'],
                    "references": f"{r['pk_table_name']}.{r['pk_column_name']}"
                })
        except Exception:
            pass

        # Assemble
        tables: dict = {}
        views: dict = {}
        for row in col_rows:
            tname = row['TABLE_NAME']
            meta = tables_meta.get(tname, {})
            is_view = meta.get('TABLE_TYPE') == 'VIEW'
            bucket = views if is_view else tables

            if tname not in bucket:
                bucket[tname] = {
                    "columns": [],
                    "row_count_estimate": int(meta.get('ROW_COUNT') or 0),
                    "primary_key": pk_map.get(tname),
                    "foreign_keys": fk_map.get(tname, []),
                    "size_bytes": int(meta.get('BYTES') or 0)
                }
            col_entry = {
                "name": row['COLUMN_NAME'],
                "type": row['DATA_TYPE'],
                "nullable": row['IS_NULLABLE'] == 'Y'
            }
            # Snowflake column comments are very useful for context
            if row.get('COMMENT'):
                col_entry["comment"] = row['COMMENT']
            bucket[tname]["columns"].append(col_entry)

        return {
            "adapter": self.name,
            "adapter_type": "snowflake",
            "database": self._database,
            "schema": self._schema,
            "tables": tables,
            "views": views,
        }

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """
        Snowflake QUERY_HISTORY is excellent — includes full SQL, user, duration, status.
        Filter to successful SELECT queries only for context mining.
        """
        import snowflake.connector
        cursor = self._conn.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(f"""
                SELECT
                    QUERY_TEXT,
                    EXECUTION_STATUS,
                    TOTAL_ELAPSED_TIME,
                    START_TIME,
                    USER_NAME,
                    WAREHOUSE_NAME
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                    DATE_RANGE_START => DATEADD('day', -90, CURRENT_TIMESTAMP()),
                    RESULT_LIMIT => {limit}
                ))
                WHERE QUERY_TYPE = 'SELECT'
                AND EXECUTION_STATUS = 'SUCCESS'
                AND QUERY_TEXT NOT ILIKE '%query_history%'
                ORDER BY START_TIME DESC
            """)
            rows = cursor.fetchall()
            return [
                {
                    "query": r['QUERY_TEXT'],
                    "execution_count": 1,
                    "avg_duration_ms": float(r.get('TOTAL_ELAPSED_TIME') or 0),
                    "last_run": str(r.get('START_TIME', '')),
                    "user": r.get('USER_NAME', '')
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"[{self.name}] Could not fetch query history: {e}")
            return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        import snowflake.connector
        import snowflake.connector
        cursor = self._conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(query, params or {})
        return [dict(r) for r in cursor.fetchall()]
