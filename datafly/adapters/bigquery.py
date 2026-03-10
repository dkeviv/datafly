"""
BigQuery adapter — uses INFORMATION_SCHEMA + JOBS table for query history.
BigQuery's JOBS table is a goldmine: full SQL, slot usage, referenced tables.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class BigQueryAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str,
                 project_id: str = "", dataset_id: str = "",
                 credentials_path: str = ""):
        super().__init__(connection_string, name)
        # Accept either connection string or explicit params
        if connection_string.startswith("bigquery://"):
            parts = connection_string.replace("bigquery://", "").split("/")
            self._project_id = parts[0] if len(parts) > 0 else project_id
            self._dataset_id = parts[1] if len(parts) > 1 else dataset_id
        else:
            self._project_id = project_id
            self._dataset_id = dataset_id
        self._credentials_path = credentials_path
        self._client = None

    def connect(self) -> None:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        if self._credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self._credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self._client = bigquery.Client(
                project=self._project_id,
                credentials=credentials
            )
        else:
            # Uses ADC (Application Default Credentials)
            self._client = bigquery.Client(project=self._project_id)

        logger.info(f"[{self.name}] Connected to BigQuery: {self._project_id}.{self._dataset_id}")

    def introspect_schema(self) -> dict:
        tables: dict = {}
        views: dict = {}

        # List all tables and views in the dataset
        dataset_ref = self._client.dataset(self._dataset_id)
        bq_tables = list(self._client.list_tables(dataset_ref))

        for bq_table in bq_tables:
            table_ref = self._client.get_table(bq_table)
            tname = table_ref.table_id
            is_view = table_ref.table_type == "VIEW"
            bucket = views if is_view else tables

            columns = []
            for field in table_ref.schema:
                col = {
                    "name": field.name,
                    "type": field.field_type,
                    "nullable": field.mode != "REQUIRED",
                }
                # BigQuery field descriptions are invaluable for context
                if field.description:
                    col["description"] = field.description
                columns.append(col)

            # Partitioning info — important for query efficiency context
            partition_info = None
            if table_ref.time_partitioning:
                partition_info = {
                    "type": table_ref.time_partitioning.type_,
                    "field": table_ref.time_partitioning.field
                }

            bucket[tname] = {
                "columns": columns,
                "row_count_estimate": table_ref.num_rows or 0,
                "primary_key": None,  # BigQuery has no enforced PKs
                "foreign_keys": [],   # No enforced FKs either
                "size_bytes": table_ref.num_bytes or 0,
                "partition": partition_info,
                "clustering_fields": table_ref.clustering_fields,
                "labels": dict(table_ref.labels) if table_ref.labels else {},
                "description": table_ref.description or ""
            }

        return {
            "adapter": self.name,
            "adapter_type": "bigquery",
            "project": self._project_id,
            "dataset": self._dataset_id,
            "tables": tables,
            "views": views,
        }

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """
        Mine INFORMATION_SCHEMA.JOBS for recent queries.
        BigQuery JOBS includes referenced_tables — critical for routing context.
        """
        try:
            query = f"""
                SELECT
                    query,
                    total_slot_ms,
                    creation_time,
                    user_email,
                    referenced_tables,
                    total_bytes_processed
                FROM `{self._project_id}`.`region-us`.INFORMATION_SCHEMA.JOBS
                WHERE job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL
                AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
                AND query NOT LIKE '%INFORMATION_SCHEMA%'
                ORDER BY creation_time DESC
                LIMIT {limit}
            """
            rows = list(self._client.query(query).result())
            result = []
            for r in rows:
                entry = {
                    "query": r.query or "",
                    "execution_count": 1,
                    "avg_duration_ms": float(r.total_slot_ms or 0) / 1000,
                    "last_run": str(r.creation_time or ""),
                    "user": r.user_email or "",
                    "bytes_processed": r.total_bytes_processed or 0
                }
                # referenced_tables tells us exactly what tables are used
                if r.referenced_tables:
                    entry["referenced_tables"] = [
                        f"{t.project_id}.{t.dataset_id}.{t.table_id}"
                        for t in r.referenced_tables
                    ]
                result.append(entry)
            return result
        except Exception as e:
            logger.warning(f"[{self.name}] Could not fetch query history: {e}")
            return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        job = self._client.query(query)
        rows = job.result()
        return [dict(r) for r in rows]
